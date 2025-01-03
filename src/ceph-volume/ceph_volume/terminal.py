import logging
import sys


terminal_logger = logging.getLogger('terminal')


class colorize(str):
    """
    Pretty simple to use::

        colorize.make('foo').bold
        colorize.make('foo').green
        colorize.make('foo').yellow
        colorize.make('foo').red
        colorize.make('foo').blue

    Otherwise you could go the long way (for example if you are
    testing this class)::

        string = colorize('foo')
        string._set_attributes()
        string.red

    """

    def __init__(self, string):
        self.appends = ''
        self.prepends = ''
        self.isatty = sys.__stderr__.isatty()

    def _set_attributes(self):
        """
        Sets the attributes here because the str class does not
        allow to pass in anything other than a string to the constructor
        so we can't really mess with the other attributes.
        """
        for k, v in self.__colors__.items():
            setattr(self, k, self.make_color(v))

    def make_color(self, color):
        if not self.isatty:
            return self
        return color + self + '\033[0m' + self.appends

    @property
    def __colors__(self):
        return dict(
            blue='\033[34m',
            green='\033[92m',
            yellow='\033[33m',
            red='\033[91m',
            bold='\033[1m',
            ends='\033[0m'
        )

    @classmethod
    def make(cls, string):
        """
        A helper method to return itself and workaround the fact that
        the str object doesn't allow extra arguments passed in to the
        constructor
        """
        obj = cls(string)
        obj._set_attributes()
        return obj

#
# Common string manipulations
#
yellow = lambda x: colorize.make(x).yellow  # noqa
blue = lambda x: colorize.make(x).blue  # noqa
green = lambda x: colorize.make(x).green  # noqa
red = lambda x: colorize.make(x).red  # noqa
bold = lambda x: colorize.make(x).bold  # noqa
red_arrow = red('--> ')
blue_arrow = blue('--> ')
green_arrow = green('--> ')
yellow_arrow = yellow('--> ')


class _Write(object):

    def __init__(self, _writer=None, prefix='', suffix='', flush=False):
        # we can't set sys.stderr as the default for _writer. otherwise
        # pytest's capturing gets confused
        self._writer = _writer or sys.stderr
        self.suffix = suffix
        self.prefix = prefix
        self.flush = flush

    def bold(self, string):
        self.write(bold(string))

    def raw(self, string):
        if not string.endswith('\n'):
            string = '%s\n' % string
        self.write(string)

    def write(self, line):
        entry = self.prefix + line + self.suffix

        try:
            self._writer.write(entry)
            if self.flush:
                self._writer.flush()
        except (UnicodeDecodeError, UnicodeEncodeError):
            try:
                terminal_logger.info(entry.strip('\n'))
            except (AttributeError, TypeError):
                terminal_logger.info(entry)


def stdout(msg):
    return _Write(prefix=blue(' stdout: ')).raw(msg)


def stderr(msg):
    return _Write(prefix=yellow(' stderr: ')).raw(msg)


def write(msg):
    return _Write().raw(msg)


def error(msg):
    return _Write(prefix=red_arrow).raw(msg)


def info(msg):
    return _Write(prefix=blue_arrow).raw(msg)


def debug(msg):
    return _Write(prefix=blue_arrow).raw(msg)


def warning(msg):
    return _Write(prefix=yellow_arrow).raw(msg)


def success(msg):
    return _Write(prefix=green_arrow).raw(msg)


class MultiLogger(object):
    """
    Proxy class to be able to report on both logger instances and terminal
    messages avoiding the issue of having to call them both separately

    Initialize it in the same way a logger object::

        logger = terminal.MultiLogger(__name__)
    """

    def __init__(self, name):
        self.logger = logging.getLogger(name)

    def _make_record(self, msg, *args):
        if len(str(args)):
            try:
                return msg % args
            except TypeError:
                self.logger.exception('unable to produce log record: %s' % msg)
        return msg

    def warning(self, msg, *args):
        record = self._make_record(msg, *args)
        warning(record)
        self.logger.warning(record)

    def debug(self, msg, *args):
        record = self._make_record(msg, *args)
        debug(record)
        self.logger.debug(record)

    def info(self, msg, *args):
        record = self._make_record(msg, *args)
        info(record)
        self.logger.info(record)

    def error(self, msg, *args):
        record = self._make_record(msg, *args)
        error(record)
        self.logger.error(record)


def dispatch(mapper, argv=None):
    argv = argv or sys.argv
    for count, arg in enumerate(argv, 1):
        if arg in mapper.keys():
            instance = mapper.get(arg)(argv[count:])
            if hasattr(instance, 'main'):
                instance.main()
                raise SystemExit(0)


def subhelp(mapper):
    """
    Look at every value of every key in the mapper and will output any
    ``class.help`` possible to return it as a string that will be sent to
    stderr.
    """
    help_text_lines = []
    for key, value in mapper.items():
        try:
            help_text = value.help
        except AttributeError:
            continue
        help_text_lines.append("%-24s %s" % (key, help_text))

    if help_text_lines:
        return "Available subcommands:\n\n%s" % '\n'.join(help_text_lines)
    return ''
