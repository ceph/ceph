import re
import sys
import argparse

# TODO: make this script compatible with python3

ALLOWED_TYPES = [
    "int",
    "long",
    "short",
    "char",
    "bool",
    "unsigned",
    "signed",
    "float",
    "double",
    "int8_t",
    "uint8_t",
    "int16_t",
    "uint16_t",
    "int32_t",
    "uint32_t",
    "int64_t",
    "uint64_t",
    "void",
    "size_t",
    "ssize_t",
]

LTTNG_LOGLEVEL_MAPPING = [
    "TRACE_EMERG",
    "TRACE_ALERT",
    "TRACE_CRIT",
    "TRACE_ERR",
    "TRACE_WARNING",
    "TRACE_NOTICE",
    "TRACE_INFO",
    "TRACE_DEBUG_SYSTEM",
    "TRACE_DEBUG_PROGRAM",
    "TRACE_DEBUG_PROCESS",
    "TRACE_DEBUG_MODULE",
    "TRACE_DEBUG_UNIT",
    "TRACE_DEBUG_FUNCTION",
    "TRACE_DEBUG_LINE",
    "TRACE_DEBUG"
]


"""
Notes: same tracepoint can be called from more than one callsite, we should
    account for duplicates.

TODO: add protection for number of given arguments. make sure the type, name, val
format is respected
"""

# trace_do_write(30, bluestore, Blob *, b, int, r, "This is the format: %s %d")
_CEPH_RE =  re.compile(r"\s*trace_"
            r"(?P<event_name>[^(]*)"
            r"\s*"
            r"\("
            r"(?P<loglevel>\b\d+\b)\s*,\s*"
            r"(?P<subsys>\b\w+\b)\s*,\s*"
            r"((?P<args>[^\"]*)\s*,\s*)?"
            r"(?P<fmt>\".+)?\s*"
            r"\)")


def is_valid_type(name):
    bits = name.split(" ")
    for bit in bits:
        bit = re.sub("\*", "", bit)
        if bit == "":
            continue
        if bit == "const":
            continue
        if bit not in ALLOWED_TYPES:
            return False
    return True


def out(fout, *lines, **kwargs):
    """Write a set of output lines.

    You can use kwargs as a shorthand for mapping variables when formating all
    the strings in lines.
    """
    lines = [ l % kwargs for l in lines ]
    fout.writelines("\n".join(lines) + "\n")


class Wrapper(object):
    def __init__(self):
        pass


class LTTngWrapper(Wrapper):
    def generate_tp_file(self, events, outdir):
        fhandles = {}
        for e in events:
            if e.subsys not in fhandles:
                fhandles[e.subsys] = open(outdir + "/tracing/" + e.subsys + ".tp", "w")
                out(fhandles[e.subsys], '#include "include/int_types.h"\n')

            fobj = fhandles[e.subsys]
            if len(e.args) > 0:
                # known bug: if the type is a pointer to a class, it should be a void*
                out(fobj,
                    'TRACEPOINT_EVENT(',
                    '   %(subsys)s,',
                    '   %(name)s,',
                    '   TP_ARGS(%(args)s),',
                    '   TP_FIELDS(',
                    subsys=e.subsys,
                    name=e.name,
                    args=",\n      ".join([', '.join([type_, name]) if is_valid_type(type_) else ', '.join(('char *', name)) for (type_, name, _) in e.args]))

                types = e.args.types()
                names = e.args.names()
                fmts = e.formats()
                for t,n,f in zip(types, names, fmts):
                    if ('char *' in t) or ('char*' in t) or ('string' in t):
                        out(fobj, '       ctf_string(' + n + ', ' + n + ')')
                    elif ("%p" in f) or ("x" in f) or ("PRIx" in f):
                        out(fobj, '       ctf_integer_hex('+ t + ', ' + n + ', ' + n + ')')
                    elif ("ptr" in t) or ("*" in t):
                        out(fobj, '       ctf_integer_hex('+ t + ', ' + n + ', ' + n + ')')
                    # Bug: if "int" in t is not good enough of a check. There's a bug if
                    # if t is 'interval_set', this script sees it as an int.
                    elif ('int' in t) or ('long' in t) or ('unsigned' in t) \
                            or ('size_t' in t) or ('bool' in t):
                        out(fobj, '       ctf_integer(' + t + ', ' + n + ', ' + n + ')')
                    elif ('double' in t) or ('float' in t):
                        out(fobj, '       ctf_float(' + t + ', ' + n + ', ' + n + ')')
                    elif ('void *' in t) or ('void*' in t):
                        out(fobj, '       ctf_integer_hex(unsigned long, ' + n + ', ' + n + ')')
                    else:
                        # Make non-basic types strings and implement a method operator std::string()
                        out(fobj, '       ctf_string(' + n + ', ' + n + ')')

                out(fobj, '   )',
                    ')',)

            else:
                out(fobj, 'TRACEPOINT_EVENT(',
                    '   %(subsys)s,',
                    '   %(name)s,',
                    '   TP_ARGS(void),',
                    '   TP_FIELDS()',
                    ')',
                    subsys=e.subsys,
                    name=e.name)

            out(fobj, 'TRACEPOINT_LOGLEVEL(%(subsys)s, %(name)s, %(loglevel)s)',
                '',
                subsys=e.subsys,
                name=e.name,
                loglevel=LTTNG_LOGLEVEL_MAPPING[0 if e.level < 0 else min(int(e.level), len(LTTNG_LOGLEVEL_MAPPING) - 1)])

        for v in fhandles.values():
            v.close()


    def generate_c(self, events, outdir):
        fhandles = {}
        for e in events:
            if e.subsys not in fhandles:
                fhandles[e.subsys] = open(outdir + "/tracing/" + e.subsys + ".c", "w")
                out(fhandles[e.subsys],
                    '/* This file was generated automatically by the tracetool script.',
                    '   Do not edit it manually. Refer to the documentation to add logging. */',
                    '')
                out(fhandles[e.subsys],
                    '#define TRACEPOINT_CREATE_PROBES',
                    ''
                    '/*',
                    ' * The header containing our TRACEPOINT_EVENTs.',
                    ' */',
                    '#include "tracing/%(subsys)s.h"',
                    subsys=e.subsys)

        for v in fhandles.values():
            v.close()


    def generate_header(self, events, outdir, disabled):
        fhandles = {}
        for e in events:
            if e.subsys not in fhandles:
                fhandles[e.subsys] = open(outdir + "/include/tracing/" + e.subsys + "_impl.h", "w")
                fobj = fhandles[e.subsys]
                out(fobj,
                    '/* This file was generated automatically by the tracetool script.',
                    '   Do not edit it manually. Refer to the documentation to add logging. */',
                    '')
                if not disabled:
                    out(fobj,
                        '#ifndef %(usubsys)s_IMPL',
                        '#define %(usubsys)s_IMPL',
                        '',
                        '#include "tracing/%(subsys)s.h"',
                        usubsys=e.subsys.upper(),
                        subsys=e.subsys)
                else:
                    out(fobj,
                        '#ifndef %(usubsys)s_IMPL',
                        '#define %(usubsys)s_IMPL',
                        usubsys=e.subsys.upper(),
                        subsys=e.subsys)


            fobj = fhandles[e.subsys]

            if not disabled:
                # Write the event's function
                out(fobj, '',
                    'static inline void __%(api)s(%(args)s)',
                    '{',
                    api=e.api(e.CEPH_TRACE),
                    args=", ".join([' '.join(arg[:2]) if is_valid_type(arg[0]) else ' '.join(('string', arg[1])) for arg in e.args]))

                # the c_str() is a temporary hack. we can use operator std::string()
                argnames = ", ".join(['(char*){}.c_str()'.format(arg[1]) if not is_valid_type(arg[0]) else arg[1] for arg in e.args])
                if len(e.args) > 0:
                    argnames = ", " + argnames

                out(fobj, '    tracepoint(%(subsys)s, %(name)s%(tp_args)s);',
                    subsys=e.subsys,
                    name=e.name,
                    tp_args=argnames)

                out(fobj, '}', '')

                # Write the event's #define
                define_str = '__loglevel, __subsys, '
                if len(e.args) > 0:
                    define_str += ', '.join( ['_type{0}, _name{0}, _value{0}'.format(i) for i in range(len(e.args))] )
                    define_str += ', '
                define_str += '__format'

                out(fobj,
                    '#define %(api)s(%(defstr)s) __%(api)s(%(args)s)',
                    api=e.api(e.CEPH_TRACE),
                    defstr=define_str,
                    args=', '.join( ['_value{0}'.format(i) for i in range(len(e.args))] )
                    )
            else: # if disabled
                # Write the event's #define
                out(fobj, '',
                    '#define %(api)s(...)',
                    api=e.api(e.CEPH_TRACE))

        for v in fhandles.values():
            out(v, '', '#endif')
            v.close()


class Arguments:
    """Event arguments description."""

    def __init__(self, args):
        """
        Parameters
        ----------
        args :
            List of (type, name) tuples or Arguments objects.
        """
        self._args = []
        for arg in args:
            if isinstance(arg, Arguments):
                self._args.extend(arg._args)
            else:
                self._args.append(arg)

    def copy(self):
        """Create a new copy."""
        return Arguments(list(self._args))

    @staticmethod
    def build(arg_str):
        """Build and Arguments instance from an argument string.

        Parameters
        ----------
        arg_str : str
            String describing the event arguments.
        """
        args_array = [i.strip() for i in arg_str.split(',')]
        args = zip(*[args_array[i::3] for i in range(3)])

        return Arguments(args)


    @staticmethod
    def old_build(arg_str):
        """Build and Arguments instance from an argument string.

        Parameters
        ----------
        arg_str : str
            String describing the event arguments.
        """
        print('arg_str:')
        print(arg_str)
        res = []
        for arg in arg_str.split(","):
            arg = arg.strip()
            if not arg:
                raise ValueError("Empty argument (did you forget to use 'void'?)")
            if arg == 'void':
                continue

            if '*' in arg:
                arg_type, identifier = arg.rsplit('*', 1)
                arg_type += '*'
                identifier = identifier.strip()
            else:
                print(arg)
                arg_type, identifier = arg.rsplit(None, 1)

            res.append((arg_type, identifier))
        return Arguments(res)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return Arguments(self._args[index])
        else:
            return self._args[index]

    def __iter__(self):
        """Iterate over the (type, name, value) tuples."""
        return iter(self._args)

    def __len__(self):
        """Number of arguments."""
        return len(self._args)

    def __str__(self):
        """String suitable for declaring function arguments."""
        if len(self._args) == 0:
            return "void"
        else:
            return ", ".join([ " ".join([vartype, varname, varvalue]) for vartype, varname, varvalue in self._args ])

    def __repr__(self):
        """Evaluable string representation for this object."""
        return "Arguments(\"%s\")" % str(self)

    def names(self):
        """List of argument names."""
        return [ name for _, name, __ in self._args ]

    def types(self):
        """List of argument types."""
        return [ type_ for type_, _, __ in self._args ]

    def values(self):
        """List of argument values."""
        return [ value for _, __, value in self._args ]
    
    def signature(self):
        """List of (type, name) of argument values"""
        return [ (type_, name) for type_, name, value in self._args ]

    def casted(self):
        """List of argument names casted to their type."""
        return ["(%s)%s" % (type_, name) for type_, name in self._args]

    def transform(self, *trans):
        """Return a new Arguments instance with transformed types.

        The types in the resulting Arguments instance are transformed according
        to tracetool.transform.transform_type.
        """
        res = []
        for type_, name in self._args:
            res.append((tracetool.transform.transform_type(type_, *trans),
                        name))
        return Arguments(res)


class Event(object):
    def __init__(self, name, level, fmt, args, subsys):
        """
        Parameters
        ----------
        name : string
            Event name.
        level: number
            Log level
        fmt : str, list of str
            Event printing format string(s).
        args : Arguments
            Event arguments.
        """
        self.name = name
        self.level = level
        self.subsys = subsys
        self.fmt = fmt
        self.args = args
        if len(args._args) > 10:
            raise ValueError("Event '%s' has more than maximum permitted "
                             "argument count" % name)

    @staticmethod
    def build(groups):
        """Build an Event"""
        name = groups["event_name"]
        level = groups["loglevel"]
        fmt = groups["fmt"]
        subsys = groups["subsys"]
        args = Arguments.build(groups["args"])

        event = Event(name, level, fmt, args, subsys)

        return event


    # Star matching on PRI is dangerous as one might have multiple
    # arguments with that format, hence the non-greedy version of it.
    _FMT = re.compile("(%[\d\.]*\w+|%.*?PRI\S+|{.*?})")

    def formats(self):
        """List conversion specifiers in the argument print format string."""
        assert not isinstance(self.fmt, list)
        return self._FMT.findall(self.fmt)

    CEPH_TRACE               = "trace_%(name)s"
    CEPH_TRACE_NOCHECK       = "_nocheck__" + CEPH_TRACE
    CEPH_TRACE_TCG           = CEPH_TRACE + "_tcg"
    CEPH_DSTATE              = "_TRACE_%(NAME)s_DSTATE"
    CEPH_BACKEND_DSTATE      = "TRACE_%(NAME)s_BACKEND_DSTATE"
    CEPH_EVENT               = "_TRACE_%(NAME)s_EVENT"

    def api(self, fmt=None):
        if fmt is None:
            fmt = Event.CEPH_TRACE
        return fmt % {"name": self.name, "NAME": self.name.upper()}


def test_re():
    """ A couple of unit tests """
    m = _CEPH_RE.match('trace_do_write(30, bluestore, Blob *, the_blob, b, int, ret, ret, "This is the string");')
    assert m is not None
    m = _CEPH_RE.match('trace_do_write(1, bluefs, Blob *, blob, (b->it).getBlog(), int, ret, ret, "This is the string %s %d %x" - (should work));')
    assert m is not None
    print('Passed unit tests\n')


def get_events_from_source(fname):
    events = []
    with open(fname, "r") as fobj:
        src = fobj.read()
        for match in _CEPH_RE.finditer(src):
            groups = match.groupdict('')
            event = Event.build(groups)
            events.append(event)

            # TODO: account for commented lines
    return events


def main(args):
    parser = argparse.ArgumentParser(description='Generate trace infrastructure.')
    parser.add_argument('-f', help='Source file', required=True)
    parser.add_argument('-t', help='Available types: tp, h, c', required=True)
    parser.add_argument('--outdir', help='Ceph build directory', required=True)
    parser.add_argument('--disabled', help='Generate headers for LTTng disabled', action='store_true')
    parser.add_argument('--test', help='Run unit tests', action='store_true')
    parsed_args = parser.parse_args(args[1:])

    events = get_events_from_source(parsed_args.f)

    wrapper = LTTngWrapper()


    if parsed_args.test:
        test_re()
        return

    if parsed_args.t == 'tp':
        wrapper.generate_tp_file(events, parsed_args.outdir)
    elif parsed_args.t == 'h':
        wrapper.generate_header(events, parsed_args.outdir, parsed_args.disabled)
    elif parsed_args.t == 'c':
        wrapper.generate_c(events, parsed_args.outdir)


if __name__ == "__main__":
    main(sys.argv)
