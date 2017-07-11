import os
import pkg_resources
import sys
import logging

from tambo import Transport
import ceph_volume
from ceph_volume.decorators import catches
from ceph_volume import log, devices, configuration, conf, exceptions, terminal


class Volume(object):
    _help = """
ceph-volume: Deploy Ceph OSDs using different device technologies like lvm or
physical disks

Version: {version}

Global Options:
--log, --logging    Set the level of logging. Acceptable values:
                    debug, warning, error, critical
--log-path          Change the default location ('/var/lib/ceph') for logging
--cluster           Change the default cluster name ('ceph')

Log Path: {log_path}
Ceph Conf: {ceph_path}

{sub_help}
{plugins}
{environ_vars}
    """

    def __init__(self, argv=None, parse=True):
        self.mapper = {'lvm': devices.lvm.LVM}
        self.plugin_help = "No plugins found/loaded"
        if argv is None:
            argv = sys.argv
        if parse:
            self.main(argv)

    def help(self, sub_help=None):
        return self._help.format(
            version=ceph_volume.__version__,
            log_path=conf.log_path,
            ceph_path=self.stat_ceph_conf(),
            plugins=self.plugin_help,
            sub_help=sub_help.strip('\n'),
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

    def load_ceph_conf_path(self, cluster_name='ceph'):
        abspath = '/etc/ceph/%s.conf' % cluster_name
        conf.path = os.getenv('CEPH_CONF', abspath)
        conf.cluster = cluster_name
        conf.ceph = configuration.load(conf.path)

    def stat_ceph_conf(self):
        try:
            configuration.load(conf.path)
            return terminal.green(conf.path)
        except exceptions.ConfigurationError as error:
            return terminal.red(error)

    @catches()
    def main(self, argv):
        # XXX Port the CLI args to user arparse
        options = [['--log', '--logging']]
        parser = Transport(argv, mapper=self.mapper,
                           options=options, check_help=False,
                           check_version=False)
        parser.parse_args()
        conf.verbosity = parser.get('--log', 'info')
        self.load_ceph_conf_path(parser.get('--cluster', 'ceph'))
        default_log_path = os.environ.get('CEPH_VOLUME_LOG_PATH', '/var/log/ceph/')
        conf.log_path = parser.get('--log-path', default_log_path)
        if os.path.isdir(conf.log_path):
            conf.log_path = os.path.join(conf.log_path, 'ceph-volume.log')
        log.setup()
        self.enable_plugins()
        parser.catch_help = self.help(parser.subhelp())
        parser.catch_version = ceph_volume.__version__
        parser.mapper = self.mapper
        if len(argv) <= 1:
            return parser.print_help()
        parser.dispatch()
        parser.catches_help()
        parser.catches_version()


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
    entry_points = pkg_resources.iter_entry_points(group=group)
    plugins = []
    for ep in entry_points:
        try:
            logger.debug('loading %s' % ep.name)
            plugin = ep.load()
            plugin._ceph_volume_name_ = ep.name
            plugins.append(plugin)
        except Exception as error:
            logger.exception("Error initializing plugin %s: %s" % (ep, error))
    return plugins
