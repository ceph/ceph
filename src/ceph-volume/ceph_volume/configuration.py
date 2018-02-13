try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import contextlib
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
    if not os.path.exists(abspath):
        raise exceptions.ConfigurationError(abspath=abspath)

    parser = Conf()

    try:
        ceph_file = open(abspath)
        trimmed_conf = _TrimIndentFile(ceph_file)
        with contextlib.closing(ceph_file):
            parser.readfp(trimmed_conf)
            return parser
    except configparser.ParsingError as error:
        logger.exception('Unable to parse INI-style file: %s' % abspath)
        terminal.error(str(error))
        raise RuntimeError('Unable to read configuration file: %s' % abspath)


class Conf(configparser.SafeConfigParser):
    """
    Subclasses from SafeConfigParser to give a few helpers for Ceph
    configuration.
    """

    def read_path(self, path):
        self.path = path
        return self.read(path)

    def is_valid(self):
        try:
            self.get('global', 'fsid')
        except (configparser.NoSectionError, configparser.NoOptionError):
            raise exceptions.ConfigurationKeyError('global', 'fsid')

    def get_safe(self, section, key, default=None):
        """
        Attempt to get a configuration value from a certain section
        in a ``cfg`` object but returning None if not found. Avoids the need
        to be doing try/except {ConfigParser Exceptions} every time.
        """
        self.is_valid()
        try:
            return self.get(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default

    def get_list(self, section, key, default=None, split=','):
        """
        Assumes that the value for a given key is going to be a list separated
        by commas. It gets rid of trailing comments.  If just one item is
        present it returns a list with a single item, if no key is found an
        empty list is returned.

        Optionally split on other characters besides ',' and return a fallback
        value if no items are found.
        """
        self.is_valid()
        value = self.get_safe(section, key, [])
        if value == []:
            if default is not None:
                return default
            return value

        # strip comments
        value = re.split(r'\s+#', value)[0]

        # split on commas
        value = value.split(split)

        # strip spaces
        return [x.strip() for x in value]

    # XXX Almost all of it lifted from the original ConfigParser._read method,
    # except for the parsing of '#' in lines. This is only a problem in Python 2.7, and can be removed
    # once tooling is Python3 only with `Conf(inline_comment_prefixes=('#',';'))`
    def _read(self, fp, fpname):
        """Parse a sectioned setup file.

        The sections in setup file contains a title line at the top,
        indicated by a name in square brackets (`[]'), plus key/value
        options lines, indicated by `name: value' format lines.
        Continuations are represented by an embedded newline then
        leading whitespace.  Blank lines, lines beginning with a '#',
        and just about everything else are ignored.
        """
        cursect = None                        # None, or a dictionary
        optname = None
        lineno = 0
        e = None                              # None, or an exception
        while True:
            line = fp.readline()
            if not line:
                break
            lineno = lineno + 1
            # comment or blank line?
            if line.strip() == '' or line[0] in '#;':
                continue
            if line.split(None, 1)[0].lower() == 'rem' and line[0] in "rR":
                # no leading whitespace
                continue
            # continuation line?
            if line[0].isspace() and cursect is not None and optname:
                value = line.strip()
                if value:
                    cursect[optname].append(value)
            # a section header or option header?
            else:
                # is it a section header?
                mo = self.SECTCRE.match(line)
                if mo:
                    sectname = mo.group('header')
                    if sectname in self._sections:
                        cursect = self._sections[sectname]
                    elif sectname == 'DEFAULT':
                        cursect = self._defaults
                    else:
                        cursect = self._dict()
                        cursect['__name__'] = sectname
                        self._sections[sectname] = cursect
                    # So sections can't start with a continuation line
                    optname = None
                # no section header in the file?
                elif cursect is None:
                    raise configparser.MissingSectionHeaderError(fpname, lineno, line)
                # an option line?
                else:
                    mo = self._optcre.match(line)
                    if mo:
                        optname, vi, optval = mo.group('option', 'vi', 'value')
                        optname = self.optionxform(optname.rstrip())
                        # This check is fine because the OPTCRE cannot
                        # match if it would set optval to None
                        if optval is not None:
                            # XXX Added support for '#' inline comments
                            if vi in ('=', ':') and (';' in optval or '#' in optval):
                                # strip comments
                                optval = re.split(r'\s+(;|#)', optval)[0]
                                # if what is left is comment as a value, fallback to an empty string
                                # that is: `foo = ;` would mean `foo` is '', which brings parity with
                                # what ceph-conf tool does
                                if optval in [';','#']:
                                    optval = ''
                            optval = optval.strip()
                            # allow empty values
                            if optval == '""':
                                optval = ''
                            cursect[optname] = [optval]
                        else:
                            # valueless option handling
                            cursect[optname] = optval
                    else:
                        # a non-fatal parsing error occurred.  set up the
                        # exception but keep going. the exception will be
                        # raised at the end of the file and will contain a
                        # list of all bogus lines
                        if not e:
                            e = configparser.ParsingError(fpname)
                        e.append(lineno, repr(line))
        # if any parsing errors occurred, raise an exception
        if e:
            raise e

        # join the multi-line values collected while reading
        all_sections = [self._defaults]
        all_sections.extend(self._sections.values())
        for options in all_sections:
            for name, val in options.items():
                if isinstance(val, list):
                    options[name] = '\n'.join(val)
