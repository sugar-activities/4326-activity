#!/usr/bin/env python
#
# Author: Sascha Silbe <sascha-pgp@silbe.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Backup. Activity to back up the Sugar Journal to external media.
"""

import gettext
import hashlib
import logging
import os
import select
import statvfs
import sys
import tempfile
import time
import traceback
import zipfile

import dbus
import gobject
import gtk

#from sugar.activity.widgets import ActivityToolbarButton
try:
    from sugar.activity.widgets import StopButton
    from sugar.graphics.icon import CellRendererIcon
    from sugar.graphics.toolbarbox import ToolbarBox
    pre_086_toolbars = False

except ImportError:
    from sugar.graphics.toolbox import Toolbox
    pre_086_toolbars = True

from sugar.activity import activity
import sugar.env
from sugar.graphics import style
from sugar.graphics.toolbutton import ToolButton
import sugar.logger
from sugar import profile

try:
    import json
    json.dumps
except (ImportError, AttributeError):
    import simplejson as json


DS_DBUS_SERVICE = "org.laptop.sugar.DataStore"
DS_DBUS_INTERFACE1 = "org.laptop.sugar.DataStore"
DS_DBUS_PATH1 = "/org/laptop/sugar/DataStore"
DS_DBUS_INTERFACE2 = "org.laptop.sugar.DataStore2"
DS_DBUS_PATH2 = "/org/laptop/sugar/DataStore2"

HAL_SERVICE_NAME = 'org.freedesktop.Hal'
HAL_MANAGER_PATH = '/org/freedesktop/Hal/Manager'
HAL_MANAGER_IFACE = 'org.freedesktop.Hal.Manager'
HAL_DEVICE_IFACE = 'org.freedesktop.Hal.Device'
HAL_VOLUME_IFACE = 'org.freedesktop.Hal.Device.Volume'


def format_size(size):
    if not size:
        return _('Empty')
    elif size < 10*1024:
        return _('%4d B') % size
    elif size < 10*1024**2:
        return _('%4d KiB') % (size // 1024)
    elif size < 10*1024**3:
        return _('%4d MiB') % (size // 1024**2)
    else:
        return _('%4d GiB') % (size // 1024**3)


class BackupButton(ToolButton):

    def __init__(self, **kwargs):
        ToolButton.__init__(self, 'journal-export', **kwargs)
        self.props.tooltip = _('Backup Journal').encode('utf-8')
        self.props.accelerator = '<Alt>b'


if pre_086_toolbars:
    class StopButton(ToolButton):

        def __init__(self, activity, **kwargs):
            ToolButton.__init__(self, 'activity-stop', **kwargs)
            self.props.tooltip = _('Stop')
            self.props.accelerator = '<Ctrl>Q'
            self.connect('clicked', self.__stop_button_clicked_cb, activity)

        def __stop_button_clicked_cb(self, button, activity):
            activity.close()


class AsyncBackup(gobject.GObject):
    """
    Run a data store backup asynchronously.
    """

    _METADATA_JSON_NAME = '_metadata.json'

    __gsignals__ = {
        'progress': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE,
            ([int, int])),
        'done': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, ([])),
        'error': (gobject.SIGNAL_RUN_FIRST, gobject.TYPE_NONE, ([str])),
    }

    def __init__(self, mount_point):
        gobject.GObject.__init__(self)
        self._mount_point = mount_point
        self._path = None
        self._bundle = None
        self._child_pid = None
        self._pipe_from_child = None
        self._pipe_to_child = None
        self._pipe_from_child_watch_id = None
        self._num_entries = None
        self._entries = None
        self._data_store = None
        self._data_store_version = None
        self._data_store_mount_id = None
        self._user_name = profile.get_nick_name().replace('/', ' ')
        self._id = self._get_id()

        if '\0' in self._user_name:
            raise ValueError('Invalid user name')

    def _get_id(self):
        """Determine a unique identifier for the user or machine.

        On XOs, the serial number will be used. On other systems the SHA-1
        hash of the public key will be used.
        """
        try:
            return file('/ofw/mfg-data/SN').read().rstrip('\0\n')
        except IOError:
            logging.debug('Not running on XO')

        return hashlib.sha1(profile.get_pubkey()).hexdigest()

    def start(self):
        """Start the backup process."""
        to_child_read_fd, to_child_write_fd = os.pipe()
        from_child_read_fd, from_child_write_fd = os.pipe()

        self._child_pid = os.fork()
        if not self._child_pid:
            os.close(from_child_read_fd)
            os.close(to_child_write_fd)
            self._pipe_from_child = os.fdopen(from_child_write_fd, 'w')
            self._pipe_to_child = os.fdopen(to_child_read_fd, 'r')
            self._child_run()
            sys.exit(0)
        else:
            os.close(from_child_write_fd)
            os.close(to_child_read_fd)
            self._pipe_from_child = os.fdopen(from_child_read_fd, 'r')
            self._pipe_to_child = os.fdopen(to_child_write_fd, 'w')
            self._pipe_from_child_watch_id = gobject.io_add_watch(
                self._pipe_from_child,
                gobject.IO_IN | gobject.IO_ERR | gobject.IO_HUP,
                self._child_io_cb)

    def abort(self):
        """Abort the backup and clean up."""
        self._pipe_to_child.write('abort\n')
        self._parent_close()

    def _child_io_cb(self, source_, condition):
        """Receive and handle message from child."""
        if condition in [gobject.IO_ERR, gobject.IO_HUP]:
            logging.debug('error condition: %r', condition)
            self.emit('error',
                _('Lost connection to child process').encode('utf-8'))
            self._parent_close()
            return False

        status = self._read_line_from_child()
        if status == 'progress':
            position = int(self._read_line_from_child())
            num_entries = int(self._read_line_from_child())
            self.emit('progress', position, num_entries)
            return True

        elif status == 'done':
            self.emit('done')
            self._parent_close()
            return False

        elif status == 'error':
            message = unicode(self._read_line_from_child(), 'utf-8')
            trace = unicode(self._pipe_from_child.read().strip(), 'utf-8')
            logging.error('Child reported error: %s\n%s', message, trace)
            self.emit('error', message.encode('utf-8'))
            self._parent_close()
            return False

        else:
            logging.error('Unknown status %r from child process', status)
            self.emit('error', 'Unknown status %r from child process', status)
            self.abort()
            return False

    def _read_line_from_child(self):
        """Read a line from the child process using low-level IO.

        This is a hack to work around the fact that file.readline() buffers
        data without us knowing about it. If we call readline() a second
        time when no data is buffered, it may block (=> the UI would hang).
        If OTOH there is another line already in the buffer, we won't get
        notified about it by select() as it already is in userspace.
        There are cleaner ways to handle this (e.g. using the asyncore module),
        but they are much more complex.
        """
        line = []
        while True:
            character = os.read(self._pipe_from_child.fileno(), 1)
            if character == '\n':
                return ''.join(line)

            line.append(character)

    def _parent_close(self):
        """Close connections to child and wait for it."""
        gobject.source_remove(self._pipe_from_child_watch_id)
        self._pipe_from_child.close()
        self._pipe_to_child.close()
        pid_, status = os.waitpid(self._child_pid, 0)
        if os.WIFEXITED(status):
            logging.debug('Child exited with rc=%d', os.WEXITSTATUS(status))
        elif os.WIFSIGNALED(status):
            logging.debug('Child killed by signal %d', os.WTERMSIG(status))
        else:
            logging.error('Sudden infant death syndrome')

    def _child_run(self):
        """Main program of child."""
        try:
            self._connect_to_data_store()
            self._entries = self._find_entries()
            self._num_entries = len(self._entries)
            self._path, self._bundle = self._create_bundle()

            for position, entry in enumerate(self._entries):
                self._client_check_command()

                self._send_to_parent('progress\n%d\n%d\n' % (position,
                    self._num_entries))
                logging.debug('processing entry %r', entry)
                self._add_entry(self._bundle, entry)

            self._send_to_parent('progress\n%d\n%d\n' % (
                self._num_entries, self._num_entries))
            self._bundle.fp.flush()
            self._bundle.close()
            self._send_to_parent('done\n')

        # pylint: disable=W0703
        except Exception, exception:
            self._pipe_from_child.write('error\n')
            message = unicode(exception).encode('utf-8')
            self._pipe_from_child.write(message+'\n')
            trace = unicode(traceback.format_exc()).encode('utf-8')
            self._pipe_from_child.write(trace)
            self._remove_bundle()
            sys.exit(2)

    def _send_to_parent(self, message):
        self._pipe_from_child.write(message)
        self._pipe_from_child.flush()

    def _client_check_command(self):
        """Check for and execute command from the parent."""
        in_ready, out_ready_, errors_on_ = select.select([self._pipe_to_child],
            [], [], 0)
        if not in_ready:
            return

        command = self._pipe_to_child.readline().strip()
        logging.debug('command %r received', command)
        if command == 'abort':
            self._remove_bundle()
            sys.exit(1)
        else:
            raise ValueError('Unknown command %r' % (command, ))

    def _create_bundle(self):
        """Create / open bundle (zip file) with unique file name."""
        date = time.strftime('%x')
        if '/' in date:
            date = time.strftime('%Y-%m-%d')

        prefix = _('Journal backup of %s (%s) on %s') % (self._user_name,
            self._id, date)
        bundle_fd, path = self._create_file(self._mount_point, prefix, '.xmj')
        try:
            return path, zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED)
        finally:
            os.close(bundle_fd)

    def _remove_bundle(self):
        """Close bundle and remove it from permanent storage."""
        if self._path:
            os.remove(self._path)

        if self._bundle and self._bundle.fp and not self._bundle.fp.closed:
            # Close the underlying file directly instead of the zip as ZipFile
            # will try to write the footer and potentially fail (e.g. ENOSPC).
            self._bundle.fp.close()
            self._bundle.fp = None

    def _create_file(self, directory, prefix, suffix):
        """Create a unique file with given prefix and suffix in directory.

        Append random ASCII characters only if necessary.
        """
        path = '%s/%s%s' % (directory, prefix, suffix)
        flags = os.O_CREAT | os.O_EXCL
        mode = 0600
        try:
            return os.open(path, flags, mode), path

        except OSError:
            return tempfile.mkstemp(dir=directory, prefix=prefix + ' ',
                suffix=suffix)

    def _add_entry(self, bundle, entry):
        """Add data store entry identified by entry to bundle."""
        if 'version_id' in entry:
            object_id = (entry['tree_id'], entry['version_id'])
            object_id_s = '%s,%s' % object_id
        else:
            object_id = entry['uid']
            object_id_s = object_id

        object_id_s = str(object_id_s)
        metadata = self._get_metadata(object_id)
        data_path = self._get_data(object_id)
        if data_path:
            bundle.write(data_path, os.path.join(object_id_s, object_id_s))

        for name in metadata:
            metadata[name] = str(metadata[name])

        for name, value in metadata.items():
            is_binary = False
            try:
                value.encode('utf-8')
            except UnicodeDecodeError:
                is_binary = True

            if is_binary or len(value) > 8192:
                logging.debug('adding binary/large property %r', name)
                bundle.writestr(os.path.join(object_id_s, str(name),
                    object_id_s), value)
                del metadata[name]

        bundle.writestr(os.path.join(object_id_s, self._METADATA_JSON_NAME),
            json.dumps(metadata))

    def _connect_to_data_store(self):
        """Open a connection to a Sugar data store."""
        # We forked => need to use a private connection and make sure we
        # never allow the main loop to run
        # http://lists.freedesktop.org/archives/dbus/2007-April/007498.html
        # http://lists.freedesktop.org/archives/dbus/2007-August/008359.html
        bus = dbus.SessionBus(private=True)
        try:
            self._data_store = dbus.Interface(bus.get_object(DS_DBUS_SERVICE,
                DS_DBUS_PATH2), DS_DBUS_INTERFACE2)
            self._data_store.find({'tree_id': 'invalid'},
                {'metadata': ['tree_id']})
            logging.info('Data store with version support found')
            return

        except dbus.DBusException:
            logging.debug('No data store with version support found')

        self._data_store = dbus.Interface(bus.get_object(DS_DBUS_SERVICE,
            DS_DBUS_PATH1), DS_DBUS_INTERFACE1)
        self._data_store.find({'uid': 'invalid'}, ['uid'])
        logging.info('Data store without version support found')

        if 'uri' in self._data_store.mounts()[0]:
            self._data_store_version = 82
            data_store_path = '/home/olpc/.sugar/default/datastore'
            self._data_store_mount_id = [mount['id']
                for mount in self._data_store.mounts()
                if mount['uri'] == data_store_path][0]
            logging.info('0.82 data store found')
        else:
            logging.info('0.84+ data store without version support found')
            self._data_store_version = 84

    def _find_entries(self):
        """Retrieve a list of all entries from the data store."""
        if self._data_store.dbus_interface == DS_DBUS_INTERFACE2:
            return self._data_store.find({}, {'metadata':
                ['parent_id', 'tree_id', 'version_id'], 'all_versions': True,
                'order_by': ['-timestamp']},
                timeout=5*60, byte_arrays=True)[0]
        elif self._data_store_version == 82:
            return [entry for entry in self._data_store.find({},
                ['uid', 'mountpoint'], byte_arrays=True)[0]
                if entry['mountpoint'] == self._data_store_mount_id]
        else:
            return self._data_store.find({}, ['uid'], byte_arrays=True)[0]

    def _get_metadata(self, object_id):
        """Return metadata for data store entry identified by object_id."""
        if self._data_store.dbus_interface == DS_DBUS_INTERFACE2:
            tree_id, version_id = object_id
            return self._data_store.find(
                {'tree_id': tree_id, 'version_id': version_id}, {},
                byte_arrays=True)[0][0]
        else:
            metadata = self._data_store.get_properties(object_id,
                byte_arrays=True)
            metadata['uid'] = object_id
            return metadata

    def _get_data(self, object_id):
        """Return path to data for data store entry identified by object_id."""
        if self._data_store.dbus_interface == DS_DBUS_INTERFACE2:
            tree_id, version_id = object_id
            return self._data_store.get_data(tree_id, version_id,
                byte_arrays=True)
        else:
            return self._data_store.get_filename(object_id, byte_arrays=True)


class BackupActivity(activity.Activity):

    _METADATA_JSON_NAME = '_metadata.json'
    _MEDIA_COMBO_ICON_COLUMN = 0
    _MEDIA_COMBO_NAME_COLUMN = 1
    _MEDIA_COMBO_PATH_COLUMN = 2
    _MEDIA_COMBO_FREE_COLUMN = 3

    def __init__(self, handle):
        activity.Activity.__init__(self, handle, create_jobject=False)
        self.max_participants = 1
        self._progress_bar = None
        self._message_box = None
        self._media_combo_model = None
        self._media_combo = None
        self._backup_button = None
        self._backup = None
        self._hal_devices = {}
        self._color = profile.get_color()
        self._setup_widgets()
        self._find_media()

    def read_file(self, file_path):
        """We don't have any state to save in the Journal."""
        return

    def write_file(self, file_path):
        """We don't have any state to save in the Journal."""
        return

    def close(self, skip_save=False):
        """We don't have any state to save in the Journal."""
        activity.Activity.close(self, skip_save=True)

    def _setup_widgets(self):
        self._setup_toolbar()
        self._setup_main_view()

    def _setup_main_view(self):
        vbox = gtk.VBox()
        self.set_canvas(vbox)
        vbox.show()

    def _setup_toolbar(self):
        if pre_086_toolbars:
            self.toolbox = Toolbox()
            self.set_toolbox(self.toolbox)

            toolbar = gtk.Toolbar()
            self.toolbox.add_toolbar('Toolbar', toolbar)
            toolbar_box = self.toolbox

        else:
            toolbar_box = ToolbarBox()
            toolbar = toolbar_box.toolbar
            self.set_toolbar_box(toolbar_box)

        self._media_combo_model = gtk.ListStore(str, str, str, str)
        self._media_combo = gtk.ComboBox(self._media_combo_model)
        if not pre_086_toolbars:
            icon_renderer = CellRendererIcon(self._media_combo)
            icon_renderer.props.xo_color = self._color
            icon_renderer.props.width = style.STANDARD_ICON_SIZE + \
                style.DEFAULT_SPACING
            icon_renderer.props.height = style.STANDARD_ICON_SIZE
            icon_renderer.props.size = style.STANDARD_ICON_SIZE
            icon_renderer.props.xpad = style.DEFAULT_PADDING
            icon_renderer.props.ypad = style.DEFAULT_PADDING
            self._media_combo.pack_start(icon_renderer, False)
            self._media_combo.add_attribute(icon_renderer, 'icon_name',
                self._MEDIA_COMBO_ICON_COLUMN)
        # FIXME: icon_renderer for pre-0.86

        name_renderer = gtk.CellRendererText()
        self._media_combo.pack_start(name_renderer, False)
        self._media_combo.add_attribute(name_renderer, 'text',
            self._MEDIA_COMBO_NAME_COLUMN)
        free_renderer = gtk.CellRendererText()
        self._media_combo.pack_start(free_renderer, False)
        self._media_combo.add_attribute(free_renderer, 'text',
            self._MEDIA_COMBO_FREE_COLUMN)

        tool_item = gtk.ToolItem()
        tool_item.add(self._media_combo)
        # FIXME: looks like plain GTK, not like Sugar
        tooltip_text = _('Storage medium to store the backup on')
        tool_item.set_tooltip_text(tooltip_text.encode('utf-8'))
        toolbar.insert(tool_item, -1)

        if pre_086_toolbars:
            self._backup_button = gtk.ToolButton()
            self._backup_button.props.icon_name = 'journal-export'
            label_text = _('Backup Journal')
            try:
                self._backup_button.props.tooltip = label_text.encode('utf-8')
            except AttributeError:
                pass

        else:
            self._backup_button = BackupButton()

        self._backup_button.connect('clicked', self._backup_cb)
        self._backup_button.set_sensitive(False)
        toolbar.insert(self._backup_button, -1)

        separator = gtk.SeparatorToolItem()
        separator.props.draw = False
        separator.set_expand(True)
        toolbar.insert(separator, -1)

        stop_button = StopButton(self)

        toolbar.insert(stop_button, -1)

        toolbar_box.show_all()

    def _backup_cb(self, button):
        """Callback for Backup button."""
        if not len(self._media_combo_model):
            return

        row = self._media_combo_model[self._media_combo.get_active()]
        mount_point = row[self._MEDIA_COMBO_PATH_COLUMN]
        self._setup_backup_view(mount_point)
        self._backup_button.set_sensitive(False)
        self._start_backup(mount_point)

    def _start_backup(self, mount_point):
        """Set up and start background worker process."""
        self._backup = AsyncBackup(mount_point)
        self._backup.connect('progress', self._progress_cb)
        self._backup.connect('error', self._error_cb)
        self._backup.connect('done', self._done_cb)
        self._backup.start()

    def _setup_backup_view(self, mount_point):
        """Set up UI for showing feedback from worker process."""
        vbox = gtk.VBox(False)

        label_text = _('Backing up Journal to %s') % (mount_point, )
        label = gtk.Label(label_text.encode('utf-8'))
        label.show()
        vbox.pack_start(label)

        alignment = gtk.Alignment(xalign=0.5, yalign=0.5, xscale=0.5)
        alignment.show()

        self._progress_bar = gtk.ProgressBar()
        self._progress_bar.props.text = _('Scanning Journal').encode('utf-8')
        self._progress_bar.show()
        alignment.add(self._progress_bar)
        vbox.add(alignment)

        self._message_box = gtk.Label()
        vbox.pack_start(self._message_box)

        # FIXME
#        self._close_button = gtk.Button(_('Abort'))
#        self._close_button.connect('clicked', self._close_cb)
#        self._close_button.show()
#        button_box = gtk.HButtonBox()
#        button_box.show()
#        button_box.add(self._close_button)
#        vbox.pack_start(button_box, False)

        vbox.show()
        self.set_canvas(vbox)
        self.show()

    def _progress_cb(self, backup, position, num_entries):
        """Update progress bar with information from child process."""
        self._progress_bar.props.text = '%d / %d' % (position, num_entries)
        self._progress_bar.props.fraction = float(position) / num_entries

    def _done_cb(self, backup):
        """Backup finished."""
        self._backup_button.set_sensitive(True)
        logging.debug('_done_cb')

    def _error_cb(self, backup, message):
        """Receive error message from child process."""
        self._backup_button.set_sensitive(True)
        self._show_error(unicode(message, 'utf-8'))

    def _show_error(self, message):
        """Present error message to user."""
        self._message_box.props.label = unicode(message).encode('utf-8')
        self._message_box.show()

#    def _close_cb(self, button):
#        if not self._done:
#            self._backup.abort()

#        self.emit('close')

    def _find_media(self):
        """Fill the combo box with available storage media.

        Also sets up a callback to keep the list current.
        """
        try:
            import gio
        except ImportError:
            return self._find_media_hal()

        volume_monitor = gio.volume_monitor_get()
        volume_monitor.connect('mount-added', self._gio_mount_added_cb)
        volume_monitor.connect('mount-removed', self._gio_mount_removed_cb)
        for mount in volume_monitor.get_mounts():
            self._gio_mount_added_cb(volume_monitor, mount)

    def _gio_mount_added_cb(self, volume_monitor, mount):
        """Handle notification from GIO that a medium was mounted."""
        icon_name = self._choose_icon_name(mount.get_icon().props.names)
        path = mount.get_root().get_path()
        name = mount.get_name()
        self._add_medium(path, name, icon_name)

    def _gio_mount_removed_cb(self, volume_monitor, mount):
        """Handle notification from GIO that a medium was unmounted."""
        path = mount.get_root().get_path()
        self._remove_medium(path)

    def _find_media_hal(self):
        """Use HAL to fill in the available storage media."""
        bus = dbus.SystemBus()
        proxy = bus.get_object(HAL_SERVICE_NAME, HAL_MANAGER_PATH)
        hal_manager = dbus.Interface(proxy, HAL_MANAGER_IFACE)
        hal_manager.connect_to_signal('DeviceAdded',
            self._hal_device_added_cb)

        for udi in hal_manager.FindDeviceByCapability('volume'):
            self._hal_device_added_cb(udi)

    def _hal_device_added_cb(self, udi):
        """Handle notification from GIO that a device was added."""
        bus = dbus.SystemBus()
        device_object = bus.get_object(HAL_SERVICE_NAME, udi)
        device = dbus.Interface(device_object, HAL_DEVICE_IFACE)

        # A just-added device might lack one of the attributes we're
        # looking for, so we need to listen for changes on ALL devices.
        device.connect_to_signal('PropertyModified',
                lambda *args: self._hal_property_modified_cb(udi, *args))

        try:
            if device.GetProperty('volume.fsusage') != 'filesystem':
                logging.debug('Ignoring non-filesystem UDI %s', udi)
                return
        except dbus.DBusException:
            logging.debug('Ignoring UDI %s without volume.fsusage', udi)
            return

        self._hal_try_device(device, udi)

    def _hal_try_device(self, device, udi):
        """Possibly add device to UI."""
        if not device.GetProperty('volume.is_mounted'):
            return

        path = device.GetProperty('volume.mount_point')
        if path == '/':
            return

        name = device.GetProperty('volume.label')
        if not name:
            name = device.GetProperty('volume.uuid')

        self._hal_devices[udi] = path
        bus = dbus.SystemBus()
        bus.add_signal_receiver(self._hal_device_removed_cb,
                                'DeviceRemoved',
                                HAL_MANAGER_IFACE, HAL_SERVICE_NAME,
                                HAL_MANAGER_PATH, arg0=udi)

        self._add_medium(path, name,
            self._choose_icon_name(self._hal_get_icons_for_volume(device)))

    def _hal_device_removed_cb(self, udi):
        """Handle notification from GIO that a device was removed."""
        path = self._hal_devices.pop(udi, None)
        if not path:
            return

        self._remove_medium(path)

    def _hal_property_modified_cb(self, udi, count, changes):
        """Handle notification from HAL that a property has changed."""
        if 'volume.is_mounted' in [change[0] for change in changes]:
            logging.debug('volume.is_mounted changed for UDI %s', udi)

            bus = dbus.SystemBus()
            device_object = bus.get_object(HAL_SERVICE_NAME, udi)
            device = dbus.Interface(device_object, HAL_DEVICE_IFACE)

            # We can get multiple notifications, so need to figure out
            # the current state and ignore non-changes.
            if device.GetProperty('volume.is_mounted'):
                if udi not in self._hal_devices:
                    self._hal_try_device(device, udi)
                else:
                    logging.debug('Ignoring duplicate notification')
            else:
                if udi in self._hal_devices:
                    self._hal_device_removed_cb(udi)
                else:
                    logging.debug('Ignoring duplicate notification')
        else:
            logging.debug('Ignoring change for UDI %s', udi)

    def _hal_get_icons_for_volume(self, device):
        bus = dbus.SystemBus()
        storage_udi = device.GetProperty('block.storage_device')
        obj = bus.get_object(HAL_SERVICE_NAME, storage_udi)
        storage_device = dbus.Interface(obj, HAL_DEVICE_IFACE)

        storage_drive_type = storage_device.GetProperty('storage.drive_type')
        bus_type = storage_device.GetProperty('storage.bus')
        if storage_drive_type == 'sd_mmc':
            return ['media-flash-sd', 'media-flash-sd-mmc', 'media']
        elif bus_type == 'usb':
            return ['media-flash-usb', 'media', 'media-disk']
        else:
            return ['media', 'media-disk', 'media-flash-usb']

    def _add_medium(self, path, medium_name, icon_name):
        """Make storage medium selectable in the UI."""
        # FIXME: update space information periodically or at least after
        # backup run
        stat = os.statvfs(path)
        free_space = stat[statvfs.F_BSIZE] * stat[statvfs.F_BAVAIL]
#        total_space = stat[statvfs.F_BSIZE] * stat[statvfs.F_BLOCKS]
        self._media_combo_model.append([icon_name, medium_name, path,
            _('%s Free') % (format_size(free_space), )])
        self._backup_button.set_sensitive(True)
        if self._media_combo.get_active() == -1:
            self._media_combo.set_active(0)

    def _remove_medium(self, path):
        """Remove storage medium from UI."""
        active = self._media_combo.get_active()
        for position, row in enumerate(self._media_combo_model):
            if path == row[self._MEDIA_COMBO_PATH_COLUMN]:
                del self._media_combo_model[position]

                if not len(self._media_combo_model):
                    self._backup_button.set_sensitive(False)
                    return

                if position != active:
                    return

                self._media_combo.set_active(max(0, position-1))
                return

        logging.warning("Asked to remove %s from UI, but didn't find it!",
            path)

    def _choose_icon_name(self, names):
        """Choose the first valid icon name or fall back to a default name."""
        theme = gtk.icon_theme_get_default()
        for name in names:
            if theme.lookup_icon(name, gtk.ICON_SIZE_LARGE_TOOLBAR, 0):
                return name

        return 'drive'


# pylint isn't smart enough for the gettext.install() magic
_ = lambda msg: msg
gettext.install('backup', 'po', unicode=True)
sugar.logger.start()
