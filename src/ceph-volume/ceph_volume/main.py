from __future__ import print_function
import argparse
import os
import sys
import logging


# `iter_entry_points` from `pkg_resources` takes one argument whereas
# `entry_points` from `importlib.metadata` does not.
try:
    from importlib.metadata import entry_points

    def get_entry_points(group: str):  # type: ignore
        return entry_points().get(group, [])  # type: ignore
except ImportError:
    from pkg_resources import iter_entry_points as entry_points  # type: ignore

    def get_entry_points(group: str):  # type: ignore
        return entry_points(group=group)  # type: ignore

from ceph_volume.decorators import catches
from ceph_volume import log, devices, configuration, conf, exceptions, terminal, inventory, drive_group, activate


class Volume(object):
    _help = """
ceph-volume: Deploy Ceph OSDs using different device technologies like lvm or
physical disks.

Log Path: {log_path}
Ceph Conf: {ceph_path}

{sub_help}
{plugins}
{environ_vars}
{warning}
    """

    def __init__(self, argv=None, parse=True):
        self.mapper = {
            'lvm': devices.lvm.LVM,
            'simple': devices.simple.Simple,
            'raw': devices.raw.Raw,
            'inventory': inventory.Inventory,
            'activate': activate.Activate,
            'drive-group': drive_group.Deploy,
        }
        self.plugin_help = "No plugins found/loaded"
        if argv is None:
            self.argv = sys.argv
        else:
            self.argv = argv
        if parse:
            self.main(self.argv)

    def help(self, warning=False):
        warning = 'See "ceph-volume --help" for full list of options.' if warning else ''
        return self._help.format(
            warning=warning,
            log_path=conf.log_path,
            ceph_path=self.stat_ceph_conf(),
            plugins=self.plugin_help,
            sub_help=terminal.subhelp(self.mapper),
            environ_vars=self.get_environ_vars()
        )

    def get_environ_vars(self):
        environ_vars = []
        for key, value in os.environ.items():
            if key.startswith('CEPH_'):
                environ_vars.append("%s=%s" % (key, value))
        if not environ_vars:
            return ''
        else:
            environ_vars.insert(0, '\nEnviron Variables:')
            return '\n'.join(environ_vars)

    def enable_plugins(self):
        """
        Load all plugins available, add them to the mapper and extend the help
        string with the information from each one
        """
        plugins = _load_library_extensions()
        for plugin in plugins:
            self.mapper[plugin._ceph_volume_name_] = plugin
        self.plugin_help = '\n'.join(['%-19s %s\n' % (
            plugin.name, getattr(plugin, 'help_menu', ''))
            for plugin in plugins])
        if self.plugin_help:
            self.plugin_help = '\nPlugins:\n' + self.plugin_help

    def load_log_path(self):
        conf.log_path = os.getenv('CEPH_VOLUME_LOG_PATH', '/var/log/ceph')

    def stat_ceph_conf(self):
        try:
            configuration.load(conf.path)
            return terminal.green(conf.path)
        except exceptions.ConfigurationError as error:
            return terminal.red(error)

    def _get_split_args(self):
        subcommands = self.mapper.keys()
        slice_on_index = len(self.argv) + 1
        pruned_args = self.argv[1:]
        for count, arg in enumerate(pruned_args):
            if arg in subcommands:
                slice_on_index = count
                break
        return pruned_args[:slice_on_index], pruned_args[slice_on_index:]

    @catches()
    def main(self, argv):
        # these need to be available for the help, which gets parsed super
        # early
        configuration.load_ceph_conf_path()
        self.load_log_path()
        self.enable_plugins()
        main_args, subcommand_args = self._get_split_args()
        # no flags where passed in, return the help menu instead of waiting for
        # argparse which will end up complaning that there are no args
        if len(argv) <= 1:
            print(self.help(warning=True))
            raise SystemExit(0)
        parser = argparse.ArgumentParser(
            prog='ceph-volume',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.help(),
        )
        parser.add_argument(
            '--cluster',
            default='ceph',
            help='Cluster name (defaults to "ceph")',
        )
        parser.add_argument(
            '--log-level',
            default='debug',
            choices=['debug', 'info', 'warning', 'error', 'critical'],
            help='Change the file log level (defaults to debug)',
        )
        parser.add_argument(
            '--log-path',
            default='/var/log/ceph/',
            help='Change the log path (defaults to /var/log/ceph)',
        )
        args = parser.parse_args(main_args)
        conf.log_path = args.log_path
        if os.path.isdir(conf.log_path):
            conf.log_path = os.path.join(args.log_path, 'ceph-volume.log')
        log.setup(log_level=args.log_level)
        log.setup_console()
        logger = logging.getLogger(__name__)
        logger.info("Running command: ceph-volume %s %s", " ".join(main_args), " ".join(subcommand_args))
        # set all variables from args and load everything needed according to
        # them
        configuration.load_ceph_conf_path(cluster_name=args.cluster)
        try:
            conf.ceph = configuration.load(conf.path)
        except exceptions.ConfigurationError as error:
            # we warn only here, because it is possible that the configuration
            # file is not needed, or that it will be loaded by some other means
            # (like reading from lvm tags)
            logger.warning('ignoring inability to load ceph.conf', exc_info=1)
            terminal.yellow(error)
        # dispatch to sub-commands
        terminal.dispatch(self.mapper, subcommand_args)


def _load_library_extensions():
    """
    Locate all setuptools entry points by the name 'ceph_volume_handlers'
    and initialize them.
    Any third-party library may register an entry point by adding the
    following to their setup.py::

        entry_points = {
            'ceph_volume_handlers': [
                'plugin_name = mylib.mymodule:Handler_Class',
            ],
        },

    `plugin_name` will be used to load it as a sub command.
    """
    logger = logging.getLogger('ceph_volume.plugins')
    group = 'ceph_volume_handlers'

    plugins = []
    for ep in get_entry_points(group=group):
        try:
            logger.debug('loading %s' % ep.name)
            plugin = ep.load()
            plugin._ceph_volume_name_ = ep.name
            plugins.append(plugin)
        except Exception as error:
            logger.exception("Error initializing plugin %s: %s" % (ep, error))
    return plugins
