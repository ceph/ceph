import codecs
import logging
import sys


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
        self.stdout = sys.__stdout__
        self.appends = ''
        self.prepends = ''
        self.isatty = self.stdout.isatty()

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
        if _writer is None:
            _writer = sys.stdout
        self._writer = _Write._unicode_output_stream(_writer)
        if _writer is sys.stdout:
            sys.stdout = self._writer
        self.suffix = suffix
        self.prefix = prefix
        self.flush = flush

    @staticmethod
    def _unicode_output_stream(stream):
        # wrapper for given stream, so it can write unicode without throwing
        # exception
        # sys.stdout.encoding is None if !isatty
        encoding = stream.encoding or ''
        if encoding.upper() in ('UTF-8', 'UTF8'):
            # already using unicode encoding, nothing to do
            return stream
        encoding = encoding or 'UTF-8'
        if sys.version_info >= (3, 0):
            # try to use whatever writer class the stream was
            return stream.__class__(stream.buffer, encoding, 'replace',
                                    stream.newlines, stream.line_buffering)
        else:
            # in python2, stdout is but a "file"
            return codecs.getwriter(encoding)(stream, 'replace')

    def bold(self, string):
        self.write(bold(string))

    def raw(self, string):
        if not string.endswith('\n'):
            string = '%s\n' % string
        self.write(string)

    def write(self, line):
        self._writer.write(self.prefix + line + self.suffix)
        if self.flush:
            self._writer.flush()


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
    stdout.
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
