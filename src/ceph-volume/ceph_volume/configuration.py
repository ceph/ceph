try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import logging
import os
import re
from ceph_volume import terminal
from ceph_volume import exceptions


logger = logging.getLogger(__name__)


class _TrimIndentFile(object):
    """
    This is used to take a file-like object and removes any
    leading tabs from each line when it's read. This is important
    because some ceph configuration files include tabs which break
    ConfigParser.
    """
    def __init__(self, fp):
        self.fp = fp

    def readline(self):
        line = self.fp.readline()
        return line.lstrip(' \t')

    def __iter__(self):
        return iter(self.readline, '')


def load(abspath=None):
    parser = Conf()
    try:
        if abspath and os.path.exists(abspath):
            parser.read(abspath)
        else:
            raise exceptions.ConfigurationError(abspath=abspath)
        return parser
    except configparser.ParsingError as error:
        terminal.error('Unable to read configuration file: %s' % abspath)
        terminal.error(str(error))
        logger.exception('Unable to parse INI-style file: %s' % abspath)


class Conf(configparser.SafeConfigParser):
    """
    Subclasses from SafeConfigParser to give a few helpers for Ceph
    configuration.
    """

    def get_safe(self, section, key, default=None):
        """
        Attempt to get a configuration value from a certain section
        in a ``cfg`` object but returning None if not found. Avoids the need
        to be doing try/except {ConfigParser Exceptions} every time.
        """
        try:
            return self.get(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default

    def get_list(self, section, key):
        """
        Assumes that the value for a given key is going to be a list separated
        by commas. It gets rid of trailing comments.  If just one item is
        present it returns a list with a single item, if no key is found an
        empty list is returned.
        """
        value = self.get_safe(section, key, [])
        if value == []:
            return value

        # strip comments
        value = re.split(r'\s+#', value)[0]

        # split on commas
        value = value.split(',')

        # strip spaces
        return [x.strip() for x in value]
