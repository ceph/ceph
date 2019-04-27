
(
    BLACK,
    RED,
    GREEN,
    YELLOW,
    BLUE,
    MAGENTA,
    CYAN,
    GRAY
) = range(8)

RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
COLOR_DARK_SEQ = "\033[0;%dm"
BOLD_SEQ = "\033[1m"
UNDERLINE_SEQ = "\033[4m"


def colorize(msg, color, dark=False):
    """
    Decorate `msg` with escape sequences to give the requested color
    """
    return (COLOR_DARK_SEQ if dark else COLOR_SEQ) % (30 + color) \
        + msg + RESET_SEQ


def bold(msg):
    """
    Decorate `msg` with escape sequences to make it appear bold
    """
    return BOLD_SEQ + msg + RESET_SEQ


def format_units(n, width, colored, decimal):
    """
    Format a number without units, so as to fit into `width` characters, substituting
    an appropriate unit suffix.

    Use decimal for dimensionless things, use base 2 (decimal=False) for byte sizes/rates.
    """

    factor = 1000 if decimal else 1024
    units = [' ', 'k', 'M', 'G', 'T', 'P', 'E']
    unit = 0
    while len("%s" % (int(n) // (factor**unit))) > width - 1:
        unit += 1

    if unit > 0:
        truncated_float = ("%f" % (n / (float(factor) ** unit)))[0:width - 1]
        if truncated_float[-1] == '.':
            truncated_float = " " + truncated_float[0:-1]
    else:
        truncated_float = "%{wid}d".format(wid=width - 1) % n
    formatted = "%s%s" % (truncated_float, units[unit])

    if colored:
        if n == 0:
            color = BLACK, False
        else:
            color = YELLOW, False
        return bold(colorize(formatted[0:-1], color[0], color[1])) \
            + bold(colorize(formatted[-1], BLACK, False))
    else:
        return formatted


def format_dimless(n, width, colored=True):
    return format_units(n, width, colored, decimal=True)


def format_bytes(n, width, colored=True):
    return format_units(n, width, colored, decimal=False)
