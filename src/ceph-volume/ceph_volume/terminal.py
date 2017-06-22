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
    return _Write(prefix=' stdout: ').raw(msg)


def stderr(msg):
    return _Write(prefix=' stderr: ').raw(msg)


def write(msg):
    return _Write().raw(msg)


def error(msg):
    return _Write(prefix=red_arrow).write(msg)


def warning(msg):
    return _Write(prefix=yellow_arrow).write(msg)


def success(msg):
    return _Write(prefix=green_arrow).write(msg)
