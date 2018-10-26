import logging
import sys


class Colorizer(object):
    """
    Pretty simple to use::

        Colorizer('\033[33m')("my test")

    """

    def __init__(self, color):
        self.color = color
        self.isatty = sys.__stdout__.isatty()

    def __call__(self, string):
        if self.isatty:
            return self.color + string + '\033[0m'
        else:
            return string

#
# Common string manipulations
#
yellow = Colorizer('\033[33m')
blue = Colorizer('\033[34m')
green = Colorizer('\033[92m')
red = Colorizer('\033[91m')
bold = Colorizer('\033[1m')
red_arrow = red('--> ')
blue_arrow = blue('--> ')
green_arrow = green('--> ')
yellow_arrow = yellow('--> ')


class _Write(object):

    def __init__(self, _writer=None, prefix='', suffix='', flush=False):
        self._writer = _writer or sys.stdout
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
