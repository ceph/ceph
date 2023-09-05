"""
Types and routines used by the ceph CLI as well as the RESTful
interface.  These have to do with querying the daemons for
command-description information, validating user command input against
those descriptions, and submitting the command to the appropriate
daemon.

Copyright (C) 2013 Inktank Storage, Inc.

LGPL-2.1 or LGPL-3.0.  See file COPYING.
"""
import copy
import enum
import math
import json
import os
import pprint
import re
import socket
import stat
import sys
import threading
import uuid

from collections import abc
from typing import cast, Any, Callable, Dict, Generic, List, Optional, Sequence, Tuple, Union

if sys.version_info >= (3, 8):
    from typing import get_args, get_origin
else:
    def get_args(tp):
        if tp is Generic:
            return tp
        else:
            return getattr(tp, '__args__', ())

    def get_origin(tp):
        return getattr(tp, '__origin__', None)


# Flags are from MonCommand.h
class Flag:
    NOFORWARD = (1 << 0)
    OBSOLETE = (1 << 1)
    DEPRECATED = (1 << 2)
    MGR = (1 << 3)
    POLL = (1 << 4)
    HIDDEN = (1 << 5)


KWARG_EQUALS = "--([^=]+)=(.+)"
KWARG_SPACE = "--([^=]+)"

try:
    basestring
except NameError:
    basestring = str


class ArgumentError(Exception):
    """
    Something wrong with arguments
    """
    pass


class ArgumentNumber(ArgumentError):
    """
    Wrong number of a repeated argument
    """
    pass


class ArgumentFormat(ArgumentError):
    """
    Argument value has wrong format
    """
    pass


class ArgumentMissing(ArgumentError):
    """
    Argument value missing in a command
    """
    pass


class ArgumentValid(ArgumentError):
    """
    Argument value is otherwise invalid (doesn't match choices, for instance)
    """
    pass


class ArgumentTooFew(ArgumentError):
    """
    Fewer arguments than descriptors in signature; may mean to continue
    the search, so gets a special exception type
    """


class ArgumentPrefix(ArgumentError):
    """
    Special for mismatched prefix; less severe, don't report by default
    """
    pass


class JsonFormat(Exception):
    """
    some syntactic or semantic issue with the JSON
    """
    pass


class CephArgtype(object):
    """
    Base class for all Ceph argument types

    Instantiating an object sets any validation parameters
    (allowable strings, numeric ranges, etc.).  The 'valid'
    method validates a string against that initialized instance,
    throwing ArgumentError if there's a problem.
    """
    def __init__(self, **kwargs):
        """
        set any per-instance validation parameters here
        from kwargs (fixed string sets, integer ranges, etc)
        """
        pass

    def valid(self, s, partial=False):
        """
        Run validation against given string s (generally one word);
        partial means to accept partial string matches (begins-with).
        If cool, set self.val to the value that should be returned
        (a copy of the input string, or a numeric or boolean interpretation
        thereof, for example)
        if not, throw ArgumentError(msg-as-to-why)
        """
        self.val = s

    def __repr__(self):
        """
        return string representation of description of type.  Note,
        this is not a representation of the actual value.  Subclasses
        probably also override __str__() to give a more user-friendly
        'name/type' description for use in command format help messages.
        """
        return self.__class__.__name__

    def __str__(self):
        """
        where __repr__ (ideally) returns a string that could be used to
        reproduce the object, __str__ returns one you'd like to see in
        print messages.  Use __str__ to format the argtype descriptor
        as it would be useful in a command usage message.
        """
        return '<{0}>'.format(self.__class__.__name__)

    def __call__(self, v):
        return v

    def complete(self, s):
        return []

    @staticmethod
    def _compound_type_to_argdesc(tp, attrs, positional):
        # generate argdesc from Sequence[T], Tuple[T,..] and Optional[T]
        orig_type = get_origin(tp)
        type_args = get_args(tp)
        if orig_type in (abc.Sequence, Sequence, List, list):
            assert len(type_args) == 1
            attrs['n'] = 'N'
            return CephArgtype.to_argdesc(type_args[0], attrs, positional=positional)
        elif orig_type is Tuple:
            assert len(type_args) >= 1
            inner_tp = type_args[0]
            assert type_args.count(inner_tp) == len(type_args), \
                f'all elements in {tp} should be identical'
            attrs['n'] = str(len(type_args))
            return CephArgtype.to_argdesc(inner_tp, attrs, positional=positional)
        elif get_origin(tp) is Union:
            # should be Union[t, NoneType]
            assert len(type_args) == 2 and isinstance(None, type_args[1])
            return CephArgtype.to_argdesc(type_args[0], attrs, True, positional)
        else:
            raise ValueError(f"unknown type '{tp}': '{attrs}'")

    @staticmethod
    def to_argdesc(tp, attrs, has_default=False, positional=True):
        if has_default:
            attrs['req'] = 'false'
        if not positional:
            attrs['positional'] = 'false'
        CEPH_ARG_TYPES = {
            str: CephString,
            int: CephInt,
            float: CephFloat,
            bool: CephBool
        }
        try:
            return CEPH_ARG_TYPES[tp]().argdesc(attrs)
        except KeyError:
            if isinstance(tp, CephArgtype):
                return tp.argdesc(attrs)
            elif isinstance(tp, type) and issubclass(tp, enum.Enum):
                return CephChoices(tp=tp).argdesc(attrs)
            else:
                return CephArgtype._compound_type_to_argdesc(tp, attrs, positional)

    def argdesc(self, attrs):
        attrs['type'] = type(self).__name__
        return ','.join(f'{k}={v}' for k, v in attrs.items())

    @staticmethod
    def _cast_to_compound_type(tp, v):
        orig_type = get_origin(tp)
        type_args = get_args(tp)
        if orig_type in (abc.Sequence, Sequence, List, list):
            if v is None:
                return None
            return [CephArgtype.cast_to(type_args[0], e) for e in v]
        elif orig_type is Tuple:
            return tuple(CephArgtype.cast_to(type_args[0], e) for e in v)
        elif get_origin(tp) is Union:
            # should be Union[t, NoneType]
            assert len(type_args) == 2 and isinstance(None, type_args[1])
            return CephArgtype.cast_to(type_args[0], v)
        else:
            raise ValueError(f"unknown type '{tp}': '{v}'")

    @staticmethod
    def cast_to(tp, v):
        PYTHON_TYPES = (
            str,
            int,
            float,
            bool
        )
        if tp in PYTHON_TYPES:
            return tp(v)
        elif isinstance(tp, type) and issubclass(tp, enum.Enum):
            return tp(v)
        else:
            return CephArgtype._cast_to_compound_type(tp, v)


class CephInt(CephArgtype):
    """
    range-limited integers, [+|-][0-9]+ or 0x[0-9a-f]+
    range: list of 1 or 2 ints, [min] or [min,max]
    """
    def __init__(self, range=''):
        if range == '':
            self.range = list()
        else:
            self.range = [int(x) for x in range.split('|')]

    def valid(self, s, partial=False):
        try:
            val = int(s, 0)
        except ValueError:
            raise ArgumentValid("{0} doesn't represent an int".format(s))
        if len(self.range) == 2:
            if val < self.range[0] or val > self.range[1]:
                raise ArgumentValid(f"{val} not in range {self.range}")
        elif len(self.range) == 1:
            if val < self.range[0]:
                raise ArgumentValid(f"{val} not in range {self.range}")
        self.val = val

    def __str__(self):
        r = ''
        if len(self.range) == 1:
            r = '[{0}-]'.format(self.range[0])
        if len(self.range) == 2:
            r = '[{0}-{1}]'.format(self.range[0], self.range[1])

        return '<int{0}>'.format(r)

    def argdesc(self, attrs):
        if self.range:
            attrs['range'] = '|'.join(str(v) for v in self.range)
        return super().argdesc(attrs)


class CephFloat(CephArgtype):
    """
    range-limited float type
    range: list of 1 or 2 floats, [min] or [min, max]
    """
    def __init__(self, range=''):
        if range == '':
            self.range = list()
        else:
            self.range = [float(x) for x in range.split('|')]

    def valid(self, s, partial=False):
        try:
            val = float(s)
        except ValueError:
            raise ArgumentValid("{0} doesn't represent a float".format(s))
        if len(self.range) == 2:
            if val < self.range[0] or val > self.range[1]:
                raise ArgumentValid(f"{val} not in range {self.range}")
        elif len(self.range) == 1:
            if val < self.range[0]:
                raise ArgumentValid(f"{val} not in range {self.range}")
        self.val = val

    def __str__(self):
        r = ''
        if len(self.range) == 1:
            r = '[{0}-]'.format(self.range[0])
        if len(self.range) == 2:
            r = '[{0}-{1}]'.format(self.range[0], self.range[1])
        return '<float{0}>'.format(r)

    def argdesc(self, attrs):
        if self.range:
            attrs['range'] = '|'.join(str(v) for v in self.range)
        return super().argdesc(attrs)


class CephString(CephArgtype):
    """
    String; pretty generic.  goodchars is a RE char class of valid chars
    """
    def __init__(self, goodchars=''):
        from string import printable
        try:
            re.compile(goodchars)
        except re.error:
            raise ValueError('CephString(): "{0}" is not a valid RE'.
                             format(goodchars))
        self.goodchars = goodchars
        self.goodset = frozenset(
            [c for c in printable if re.match(goodchars, c)]
        )

    def valid(self, s: str, partial: bool = False) -> None:
        sset = set(s)
        if self.goodset and not sset <= self.goodset:
            raise ArgumentFormat("invalid chars {0} in {1}".
                                 format(''.join(sset - self.goodset), s))
        self.val = s

    def __str__(self) -> str:
        b = ''
        if self.goodchars:
            b += '(goodchars {0})'.format(self.goodchars)
        return '<string{0}>'.format(b)

    def complete(self, s) -> List[str]:
        if s == '':
            return []
        else:
            return [s]

    def argdesc(self, attrs):
        if self.goodchars:
            attrs['goodchars'] = self.goodchars
        return super().argdesc(attrs)


class CephSocketpath(CephArgtype):
    """
    Admin socket path; check that it's readable and S_ISSOCK
    """
    def valid(self, s: str, partial: bool = False) -> None:
        mode = os.stat(s).st_mode
        if not stat.S_ISSOCK(mode):
            raise ArgumentValid('socket path {0} is not a socket'.format(s))
        self.val = s

    def __str__(self) -> str:
        return '<admin-socket-path>'


class CephIPAddr(CephArgtype):
    """
    IP address (v4 or v6) with optional port
    """
    def valid(self, s, partial=False):
        # parse off port, use socket to validate addr
        type = 6
        p: Optional[str] = None
        if s.startswith('v1:'):
            s = s[3:]
            type = 4
        elif s.startswith('v2:'):
            s = s[3:]
            type = 6
        elif s.startswith('any:'):
            s = s[4:]
            type = 4
        elif s.startswith('['):
            type = 6
        elif s.find('.') != -1:
            type = 4

        if type == 4:
            port = s.find(':')
            if port != -1:
                a = s[:port]
                p = s[port + 1:]
                if int(p) > 65535:
                    raise ArgumentValid('{0}: invalid IPv4 port'.format(p))
            else:
                a = s
                p = None
            try:
                socket.inet_pton(socket.AF_INET, a)
            except OSError:
                raise ArgumentValid('{0}: invalid IPv4 address'.format(a))
        else:
            # v6
            if s.startswith('['):
                end = s.find(']')
                if end == -1:
                    raise ArgumentFormat('{0} missing terminating ]'.format(s))
                if s[end + 1] == ':':
                    try:
                        p = s[end + 2]
                    except ValueError:
                        raise ArgumentValid('{0}: bad port number'.format(s))
                a = s[1:end]
            else:
                a = s
                p = None
            try:
                socket.inet_pton(socket.AF_INET6, a)
            except OSError:
                raise ArgumentValid('{0} not valid IPv6 address'.format(s))
        if p is not None and int(p) > 65535:
            raise ArgumentValid("{0} not a valid port number".format(p))
        self.val = s
        self.addr = a
        self.port = p

    def __str__(self):
        return '<IPaddr[:port]>'


class CephEntityAddr(CephIPAddr):
    """
    EntityAddress, that is, IP address[/nonce]
    """
    def valid(self, s: str, partial: bool = False) -> None:
        nonce = None
        if '/' in s:
            ip, nonce = s.split('/')
            if nonce.endswith(']'):
                nonce = nonce[:-1]
                ip += ']'
        else:
            ip = s
        super(self.__class__, self).valid(ip)
        if nonce:
            nonce_int = None
            try:
                nonce_int = int(nonce)
            except ValueError:
                pass
            if nonce_int is None or nonce_int < 0:
                raise ArgumentValid(
                    '{0}: invalid entity, nonce {1} not integer > 0'.
                    format(s, nonce)
                )
        self.val = s

    def __str__(self) -> str:
        return '<EntityAddr>'


class CephPoolname(CephArgtype):
    """
    Pool name; very little utility
    """
    def __str__(self) -> str:
        return '<poolname>'


class CephObjectname(CephArgtype):
    """
    Object name.  Maybe should be combined with Pool name as they're always
    present in pairs, and then could be checked for presence
    """
    def __str__(self) -> str:
        return '<objectname>'


class CephPgid(CephArgtype):
    """
    pgid, in form N.xxx (N = pool number, xxx = hex pgnum)
    """
    def valid(self, s, partial=False):
        if s.find('.') == -1:
            raise ArgumentFormat('pgid has no .')
        poolid_s, pgnum_s = s.split('.', 1)
        try:
            poolid = int(poolid_s)
        except ValueError:
            raise ArgumentFormat('pool {0} not integer'.format(poolid))
        if poolid < 0:
            raise ArgumentFormat('pool {0} < 0'.format(poolid))
        try:
            pgnum = int(pgnum_s, 16)
        except ValueError:
            raise ArgumentFormat('pgnum {0} not hex integer'.format(pgnum))
        self.val = s

    def __str__(self):
        return '<pgid>'


class CephName(CephArgtype):
    """
    Name (type.id) where:
    type is osd|mon|client|mds
    id is a base10 int, if type == osd, or a string otherwise

    Also accept '*'
    """
    def __init__(self) -> None:
        self.nametype: Optional[str] = None
        self.nameid: Optional[str] = None

    def valid(self, s, partial=False):
        if s == '*':
            self.val = s
            return
        elif s == "mgr":
            self.nametype = "mgr"
            self.val = s
            return
        elif s == "mon":
            self.nametype = "mon"
            self.val = s
            return
        if s.find('.') == -1:
            raise ArgumentFormat('CephName: no . in {0}'.format(s))
        else:
            t, i = s.split('.', 1)
            if t not in ('osd', 'mon', 'client', 'mds', 'mgr'):
                raise ArgumentValid('unknown type ' + t)
            if t == 'osd':
                if i != '*':
                    try:
                        int(i)
                    except ValueError:
                        raise ArgumentFormat(f'osd id {i} not integer')
            self.nametype = t
        self.val = s
        self.nameid = i

    def __str__(self):
        return '<name (type.id)>'


class CephOsdName(CephArgtype):
    """
    Like CephName, but specific to osds: allow <id> alone

    osd.<id>, or <id>, or *, where id is a base10 int
    """
    def __init__(self):
        self.nametype: Optional[str] = None
        self.nameid: Optional[int] = None

    def valid(self, s, partial=False):
        if s == '*':
            self.val = s
            return
        if s.find('.') != -1:
            t, i = s.split('.', 1)
            if t != 'osd':
                raise ArgumentValid('unknown type ' + t)
        else:
            t = 'osd'
            i = s
        try:
            v = int(i)
        except ValueError:
            raise ArgumentFormat(f'osd id {i} not integer')
        if v < 0:
            raise ArgumentFormat(f'osd id {v} is less than 0')
        self.nametype = t
        self.nameid = v
        self.val = v

    def __str__(self):
        return '<osdname (id|osd.id)>'


class CephChoices(CephArgtype):
    """
    Set of string literals; init with valid choices
    """
    def __init__(self, strings='', tp=None, **kwargs):
        self.strings = strings.split('|')
        self.enum = tp
        if self.enum is not None:
            self.strings = list(e.value for e in self.enum)

    def valid(self, s, partial=False):
        if not partial:
            if s not in self.strings:
                # show as __str__ does: {s1|s2..}
                raise ArgumentValid("{0} not in {1}".format(s, self))
            self.val = s
            return

        # partial
        for t in self.strings:
            if t.startswith(s):
                self.val = s
                return
        raise ArgumentValid("{0} not in {1}".  format(s, self))

    def __str__(self):
        if len(self.strings) == 1:
            return '{0}'.format(self.strings[0])
        else:
            return '{0}'.format('|'.join(self.strings))

    def __call__(self, v):
        if self.enum is None:
            return v
        else:
            return self.enum[v]

    def complete(self, s):
        all_elems = [token for token in self.strings if token.startswith(s)]
        return all_elems

    def argdesc(self, attrs):
        attrs['strings'] = '|'.join(self.strings)
        return super().argdesc(attrs)


class CephBool(CephArgtype):
    """
    A boolean argument, values may be case insensitive 'true', 'false', '0',
    '1'.  In keyword form, value may be left off (implies true).
    """
    def __init__(self, strings='', **kwargs):
        self.strings = strings.split('|')

    def valid(self, s, partial=False):
        lower_case = s.lower()
        if lower_case in ['true', '1']:
            self.val = True
        elif lower_case in ['false', '0']:
            self.val = False
        else:
            raise ArgumentValid("{0} not one of 'true', 'false'".format(s))

    def __str__(self):
        return '<bool>'


class CephFilepath(CephArgtype):
    """
    Openable file
    """
    def valid(self, s, partial=False):
        # set self.val if the specified path is readable or writable
        s = os.path.abspath(s)
        if not os.access(s, os.R_OK):
            self._validate_writable_file(s)
        self.val = s

    def _validate_writable_file(self, fname):
        if os.path.exists(fname):
            if os.path.isfile(fname):
                if not os.access(fname, os.W_OK):
                    raise ArgumentValid('{0} is not writable'.format(fname))
            else:
                raise ArgumentValid('{0} is not file'.format(fname))
        else:
            dirname = os.path.dirname(fname)
            if not os.access(dirname, os.W_OK):
                raise ArgumentValid('cannot create file in {0}'.format(dirname))

    def __str__(self):
        return '<outfilename>'


class CephFragment(CephArgtype):
    """
    'Fragment' ??? XXX
    """
    def valid(self, s, partial=False):
        if s.find('/') == -1:
            raise ArgumentFormat('{0}: no /'.format(s))
        val, bits = s.split('/')
        # XXX is this right?
        if not val.startswith('0x'):
            raise ArgumentFormat("{0} not a hex integer".format(val))
        try:
            int(val)
        except ValueError:
            raise ArgumentFormat('can\'t convert {0} to integer'.format(val))
        try:
            int(bits)
        except ValueError:
            raise ArgumentFormat('can\'t convert {0} to integer'.format(bits))
        self.val = s

    def __str__(self):
        return "<CephFS fragment ID (0xvvv/bbb)>"


class CephUUID(CephArgtype):
    """
    CephUUID: pretty self-explanatory
    """
    def valid(self, s: str, partial: bool = False) -> None:
        try:
            uuid.UUID(s)
        except Exception as e:
            raise ArgumentFormat('invalid UUID {0}: {1}'.format(s, e))
        self.val = s

    def __str__(self) -> str:
        return '<uuid>'


class CephPrefix(CephArgtype):
    """
    CephPrefix: magic type for "all the first n fixed strings"
    """
    def __init__(self, prefix: str = '') -> None:
        self.prefix = prefix

    def valid(self, s: str, partial: bool = False) -> None:
        try:
            s = str(s)
            if isinstance(s, bytes):
                # `prefix` can always be converted into unicode when being compared,
                # but `s` could be anything passed by user.
                s = s.decode('ascii')
        except UnicodeEncodeError:
            raise ArgumentPrefix(u"no match for {0}".format(s))
        except UnicodeDecodeError:
            raise ArgumentPrefix("no match for {0}".format(s))

        if partial:
            if self.prefix.startswith(s):
                self.val = s
                return
        else:
            if s == self.prefix:
                self.val = s
                return

        raise ArgumentPrefix("no match for {0}".format(s))

    def __str__(self) -> str:
        return self.prefix

    def complete(self, s) -> List[str]:
        if self.prefix.startswith(s):
            return [self.prefix.rstrip(' ')]
        else:
            return []


class argdesc(object):
    """
    argdesc(typename, name='name', n=numallowed|N,
            req=False, positional=True,
            helptext=helptext, **kwargs (type-specific))

    validation rules:
    typename: type(**kwargs) will be constructed
    later, type.valid(w) will be called with a word in that position

    name is used for parse errors and for constructing JSON output
    n is a numeric literal or 'n|N', meaning "at least one, but maybe more"
    req=False means the argument need not be present in the list
    positional=False means the argument name must be specified, e.g. "--myoption value"
    helptext is the associated help for the command
    anything else are arguments to pass to the type constructor.

    self.instance is an instance of type t constructed with typeargs.

    valid() will later be called with input to validate against it,
    and will store the validated value in self.instance.val for extraction.
    """
    def __init__(self, t, name=None, n=1, req=True, positional=True, **kwargs) -> None:
        if isinstance(t, basestring):
            self.t = CephPrefix
            self.typeargs = {'prefix': t}
            self.req = True
            self.positional = True
        else:
            self.t = t
            self.typeargs = kwargs
            self.req = req in (True, 'True', 'true')
            self.positional = positional in (True, 'True', 'true')
            if not positional:
                assert not req

        self.name = name
        self.N = (n in ['n', 'N'])
        if self.N:
            self.n = 1
        else:
            self.n = int(n)

        self.numseen = 0

        self.instance = self.t(**self.typeargs)

    def __repr__(self):
        r = 'argdesc(' + str(self.t) + ', '
        internals = ['N', 'typeargs', 'instance', 't']
        for (k, v) in self.__dict__.items():
            if k.startswith('__') or k in internals:
                pass
            else:
                # undo modification from __init__
                if k == 'n' and self.N:
                    v = 'N'
                r += '{0}={1}, '.format(k, v)
        for (k, v) in self.typeargs.items():
            r += '{0}={1}, '.format(k, v)
        return r[:-2] + ')'

    def __str__(self):
        if ((self.t == CephChoices and len(self.instance.strings) == 1)
           or (self.t == CephPrefix)):
            s = str(self.instance)
        else:
            s = '{0}({1})'.format(self.name, str(self.instance))
            if self.N:
                s += '...'
        if not self.req:
            s = '[' + s + ']'
        return s

    def helpstr(self):
        """
        like str(), but omit parameter names (except for CephString,
        which really needs them)
        """
        if self.positional:
            if self.t == CephBool:
                chunk = "--{0}".format(self.name.replace("_", "-"))
            elif self.t == CephPrefix:
                chunk = str(self.instance)
            elif self.t == CephChoices:
                if self.name == 'format':
                    # this is for talking to legacy clusters only; new clusters
                    # should properly mark format args as non-positional.
                    chunk = f'--{self.name} {{{str(self.instance)}}}'
                else:
                    chunk = f'<{self.name}:{self.instance}>'
            elif self.t == CephOsdName:
                # it just so happens all CephOsdName commands are named 'id' anyway,
                # so <id|osd.id> is perfect.
                chunk = '<id|osd.id>'
            elif self.t == CephName:
                # CephName commands similarly only have one arg of the
                # type, so <type.id> is good.
                chunk = '<type.id>'
            elif self.t == CephInt:
                chunk = '<{0}:int>'.format(self.name)
            elif self.t == CephFloat:
                chunk = '<{0}:float>'.format(self.name)
            else:
                chunk = '<{0}>'.format(self.name)
            s = chunk
            if self.N:
                s += '...'
            if not self.req:
                s = '[' + s + ']'
        else:
            # non-positional
            if self.t == CephBool:
                chunk = "--{0}".format(self.name.replace("_", "-"))
            elif self.t == CephPrefix:
                chunk = str(self.instance)
            elif self.t == CephChoices:
                chunk = f'--{self.name} {{{str(self.instance)}}}'
            elif self.t == CephOsdName:
                chunk = f'--{self.name} <id|osd.id>'
            elif self.t == CephName:
                chunk = f'--{self.name} <type.id>'
            elif self.t == CephInt:
                chunk = f'--{self.name} <int>'
            elif self.t == CephFloat:
                chunk = f'--{self.name} <float>'
            else:
                chunk = f'--{self.name} <value>'
            s = chunk
            if self.N:
                s += '...'
            if not self.req:  # req should *always* be false
                s = '[' + s + ']'

        return s

    def complete(self, s):
        return self.instance.complete(s)


def concise_sig(sig):
    """
    Return string representation of sig useful for syntax reference in help
    """
    return ' '.join([d.helpstr() for d in sig])


def descsort_key(sh):
    """
    sort descriptors by prefixes, defined as the concatenation of all simple
    strings in the descriptor; this works out to just the leading strings.
    """
    return concise_sig(sh['sig'])


def parse_funcsig(sig: Sequence[Union[str, Dict[str, Any]]]) -> List[argdesc]:
    """
    parse a single descriptor (array of strings or dicts) into a
    dict of function descriptor/validators (objects of CephXXX type)

    :returns: list of ``argdesc``
    """
    newsig = []
    argnum = 0
    for desc in sig:
        argnum += 1
        if isinstance(desc, basestring):
            t = CephPrefix
            desc = {'type': t, 'name': 'prefix', 'prefix': desc}
        else:
            # not a simple string, must be dict
            if 'type' not in desc:
                s = 'JSON descriptor {0} has no type'.format(sig)
                raise JsonFormat(s)
            # look up type string in our globals() dict; if it's an
            # object of type `type`, it must be a
            # locally-defined class. otherwise, we haven't a clue.
            if desc['type'] in globals():
                t = globals()[desc['type']]
                if not isinstance(t, type):
                    s = 'unknown type {0}'.format(desc['type'])
                    raise JsonFormat(s)
            else:
                s = 'unknown type {0}'.format(desc['type'])
                raise JsonFormat(s)

        kwargs = dict()
        for key, val in desc.items():
            if key not in ['type', 'name', 'n', 'req', 'positional']:
                kwargs[key] = val
        newsig.append(argdesc(t,
                              name=desc.get('name', None),
                              n=desc.get('n', 1),
                              req=desc.get('req', True),
                              positional=desc.get('positional', True),
                              **kwargs))
    return newsig


def parse_json_funcsigs(s: str,
                        consumer: str) -> Dict[str, Dict[str, List[argdesc]]]:
    """
    A function signature is mostly an array of argdesc; it's represented
    in JSON as
    {
      "cmd001": {"sig":[ "type": type, "name": name, "n": num, "req":true|false <other param>], "help":helptext, "module":modulename, "perm":perms, "avail":availability}
       .
       .
       .
      ]

    A set of sigs is in an dict mapped by a unique number:
    {
      "cmd1": {
         "sig": ["type.. ], "help":helptext...
      }
      "cmd2"{
         "sig": [.. ], "help":helptext...
      }
    }

    Parse the string s and return a dict of dicts, keyed by opcode;
    each dict contains 'sig' with the array of descriptors, and 'help'
    with the helptext, 'module' with the module name, 'perm' with a
    string representing required permissions in that module to execute
    this command (and also whether it is a read or write command from
    the cluster state perspective), and 'avail' as a hint for
    whether the command should be advertised by CLI, REST, or both.
    If avail does not contain 'consumer', don't include the command
    in the returned dict.
    """
    try:
        overall = json.loads(s)
    except Exception as e:
        print("Couldn't parse JSON {0}: {1}".format(s, e), file=sys.stderr)
        raise e
    sigdict = {}
    for cmdtag, cmd in overall.items():
        if 'sig' not in cmd:
            s = "JSON descriptor {0} has no 'sig'".format(cmdtag)
            raise JsonFormat(s)
        # check 'avail' and possibly ignore this command
        if 'avail' in cmd:
            if consumer not in cmd['avail']:
                continue
        # rewrite the 'sig' item with the argdesc-ized version, and...
        cmd['sig'] = parse_funcsig(cmd['sig'])
        # just take everything else as given
        sigdict[cmdtag] = cmd
    return sigdict


ArgValT = Union[bool, int, float, str, Tuple[str, str]]

def validate_one(word: str,
                 desc,
                 is_kwarg: bool,
                 partial: bool = False) -> List[ArgValT]:
    """
    validate_one(word, desc, is_kwarg, partial=False)

    validate word against the constructed instance of the type
    in desc.  May raise exception.  If it returns false (and doesn't
    raise an exception), desc.instance.val will
    contain the validated value (in the appropriate type).
    """
    vals = []
    # CephString option might contain "," in it
    allow_csv = is_kwarg or desc.t is not CephString
    if desc.N and allow_csv:
        for part in word.split(','):
            desc.instance.valid(part, partial)
            vals.append(desc.instance.val)
    else:
        desc.instance.valid(word, partial)
        vals.append(desc.instance.val)
    desc.numseen += 1
    if desc.N:
        desc.n = desc.numseen + 1
    return vals


def matchnum(args: List[str],
             signature: List[argdesc],
             partial: bool = False) -> int:
    """
    matchnum(s, signature, partial=False)

    Returns number of arguments matched in s against signature.
    Can be used to determine most-likely command for full or partial
    matches (partial applies to string matches).
    """
    words = args[:]
    mysig = copy.deepcopy(signature)
    matchcnt = 0
    for desc in mysig:
        desc.numseen = 0
        while desc.numseen < desc.n:
            # if there are no more arguments, return
            if not words:
                return matchcnt
            word = words.pop(0)

            try:
                # only allow partial matching if we're on the last supplied
                # word; avoid matching foo bar and foot bar just because
                # partial is set
                validate_one(word, desc, False, partial and (len(words) == 0))
                valid = True
            except ArgumentError:
                # matchnum doesn't care about type of error
                valid = False

            if not valid:
                if not desc.req:
                    # this wasn't required, so word may match the next desc
                    words.insert(0, word)
                    break
                else:
                    # it was required, and didn't match, return
                    return matchcnt
        if desc.req:
            matchcnt += 1
    return matchcnt


ValidatedArg = Union[bool, int, float, str,
                     Tuple[str, str],
                     Sequence[str]]
ValidatedArgs = Dict[str, ValidatedArg]


def store_arg(desc: argdesc, args: Sequence[ValidatedArg], d: ValidatedArgs):
    '''
    Store argument described by, and held in, thanks to valid(),
    desc into the dictionary d, keyed by desc.name.  Three cases:

    1) desc.N is set: use args for arg value in "d", desc.instance.val
       only contains the last parsed arg in the "args" list
    2) prefix: multiple args are joined with ' ' into one d{} item
    3) single prefix or other arg: store as simple value

    Used in validate() below.
    '''
    if desc.N:
        # value should be a list
        if desc.name in d:
            d[desc.name] += args
        else:
            d[desc.name] = args
    elif (desc.t == CephPrefix) and (desc.name in d):
        # prefixes' values should be a space-joined concatenation
        d[desc.name] += ' ' + desc.instance.val
    else:
        # if first CephPrefix or any other type, just set it
        d[desc.name] = desc.instance.val


def validate(args: List[str],
             signature: Sequence[argdesc],
             flags: int = 0,
             partial: Optional[bool] = False) -> ValidatedArgs:
    """
    validate(args, signature, flags=0, partial=False)

    args is a list of strings representing a possible
    command input following format of signature.  Runs a validation; no
    exception means it's OK.  Return a dict containing all arguments keyed
    by their descriptor name, with duplicate args per name accumulated
    into a list (or space-separated value for CephPrefix).

    Mismatches of prefix are non-fatal, as this probably just means the
    search hasn't hit the correct command.  Mismatches of non-prefix
    arguments are treated as fatal, and an exception raised.

    This matching is modified if partial is set: allow partial matching
    (with partial dict returned); in this case, there are no exceptions
    raised.
    """

    myargs = copy.deepcopy(args)
    mysig = copy.deepcopy(signature)
    reqsiglen = len([desc for desc in mysig if desc.req])
    matchcnt = 0
    d: ValidatedArgs = dict()
    save_exception = None

    arg_descs_by_name: Dict[str, argdesc] = \
        dict((desc.name, desc) for desc in mysig if desc.t != CephPrefix)

    # Special case: detect "injectargs" (legacy way of modifying daemon
    # configs) and permit "--" string arguments if so.
    injectargs = myargs and myargs[0] == "injectargs"

    # Make a pass through all arguments
    for desc in mysig:
        desc.numseen = 0

        while desc.numseen < desc.n:
            if myargs:
                myarg: Optional[str] = myargs.pop(0)
            else:
                myarg = None

            # no arg, but not required?  Continue consuming mysig
            # in case there are later required args
            if myarg in (None, []):
                if not desc.req:
                    break
                # did we already get this argument (as a named arg, earlier?)
                if desc.name in d:
                    break

            # A keyword argument?
            if myarg:
                # argdesc for the keyword argument, if we find one
                kwarg_desc = None

                # Try both styles of keyword argument
                kwarg_match = re.match(KWARG_EQUALS, myarg)
                if kwarg_match:
                    # We have a "--foo=bar" style argument
                    kwarg_k, kwarg_v = kwarg_match.groups()

                    # Either "--foo-bar" or "--foo_bar" style is accepted
                    kwarg_k = kwarg_k.replace('-', '_')

                    kwarg_desc = arg_descs_by_name.get(kwarg_k, None)
                else:
                    # Maybe this is a "--foo bar" or "--bool" style argument
                    key_match = re.match(KWARG_SPACE, myarg)
                    if key_match:
                        kwarg_k = key_match.group(1)

                        # Permit --foo-bar=123 form or --foo_bar=123 form,
                        # assuming all command definitions use foo_bar argument
                        # naming style
                        kwarg_k = kwarg_k.replace('-', '_')

                        kwarg_desc = arg_descs_by_name.get(kwarg_k, None)
                        if kwarg_desc:
                            if kwarg_desc.t == CephBool:
                                kwarg_v = 'true'
                            elif len(myargs):  # Some trailing arguments exist
                                kwarg_v = myargs.pop(0)
                            else:
                                # Forget it, this is not a valid kwarg
                                kwarg_desc = None

                if kwarg_desc:
                    args = validate_one(kwarg_v, kwarg_desc, True)
                    matchcnt += 1
                    store_arg(kwarg_desc, args, d)
                    continue

            if not desc.positional:
                # No more positional args!
                raise ArgumentValid(f"Unexpected argument '{myarg}'")

            # Don't handle something as a positional argument if it
            # has a leading "--" unless it's a CephChoices (used for
            # "--yes-i-really-mean-it")
            if myarg and myarg.startswith("--"):
                # Special cases for instances of confirmation flags
                # that were defined as CephString/CephChoices instead of CephBool
                # in pre-nautilus versions of Ceph daemons.
                is_value = desc.t == CephChoices \
                        or myarg == "--yes-i-really-mean-it" \
                        or myarg == "--yes-i-really-really-mean-it" \
                        or myarg == "--yes-i-really-really-mean-it-not-faking" \
                        or myarg == "--force" \
                        or injectargs

                if not is_value:
                    # Didn't get caught by kwarg handling, but has a "--", so
                    # we must assume it's something invalid, to avoid naively
                    # passing through mis-typed options as the values of
                    # positional arguments.
                    raise ArgumentValid("Unexpected argument '{0}'".format(
                        myarg))

            # out of arguments for a required param?
            # Either return (if partial validation) or raise
            if myarg in (None, []) and desc.req:
                if desc.N and desc.numseen < 1:
                    # wanted N, didn't even get 1
                    if partial:
                        return d
                    raise ArgumentNumber(
                        'saw {0} of {1}, expected at least 1'.
                        format(desc.numseen, desc)
                    )
                elif not desc.N and desc.numseen < desc.n:
                    # wanted n, got too few
                    if partial:
                        return d
                    # special-case the "0 expected 1" case
                    if desc.numseen == 0 and desc.n == 1:
                        raise ArgumentMissing(
                            'missing required parameter {0}'.format(desc)
                        )
                    raise ArgumentNumber(
                        'saw {0} of {1}, expected {2}'.
                        format(desc.numseen, desc, desc.n)
                    )
                break

            # Have an arg; validate it
            assert myarg is not None
            try:
                args = validate_one(myarg, desc, False)
            except ArgumentError as e:
                # argument mismatch
                if not desc.req:
                    # if not required, just push back; it might match
                    # the next arg
                    save_exception = [myarg, e]
                    myargs.insert(0, myarg)
                    break
                else:
                    # hm, it was required, so time to return/raise
                    if partial:
                        return d
                    raise

            # Whew, valid arg acquired.  Store in dict
            matchcnt += 1
            store_arg(desc, args, d)
            # Clear prior exception
            save_exception = None

    # Done with entire list of argdescs
    if matchcnt < reqsiglen:
        raise ArgumentTooFew("not enough arguments given")

    if myargs and not partial:
        if save_exception:
            print(save_exception[0], 'not valid: ', save_exception[1], file=sys.stderr)
        raise ArgumentError("unused arguments: " + str(myargs))

    if flags & Flag.MGR:
        d['target'] = ('mon-mgr', '')

    if flags & Flag.POLL:
        d['poll'] = True

    # Finally, success
    return d


def validate_command(sigdict: Dict[str, Dict[str, Any]],
                     args: List[str],
                     verbose: Optional[bool] = False) -> ValidatedArgs:
    """
    Parse positional arguments into a parameter dict, according to
    the command descriptions.

    Writes advice about nearly-matching commands ``sys.stderr`` if
    the arguments do not match any command.

    :param sigdict: A command description dictionary, as returned
                    from Ceph daemons by the get_command_descriptions
                    command.
    :param args: List of strings, should match one of the command
                 signatures in ``sigdict``

    :returns: A dict of parsed parameters (including ``prefix``),
              or an empty dict if the args did not match any signature
    """
    if verbose:
        print("validate_command: " + " ".join(args), file=sys.stderr)
    found: Optional[Dict[str, Any]] = None
    valid_dict = {}

    # look for best match, accumulate possibles in bestcmds
    # (so we can maybe give a more-useful error message)
    best_match_cnt = 0.0
    bestcmds: List[Dict[str, Any]] = []
    for cmd in sigdict.values():
        flags = cmd.get('flags', 0)
        if flags & Flag.OBSOLETE:
            continue
        sig = cmd['sig']
        matched: float = matchnum(args, sig, partial=True)
        if (matched >= math.floor(best_match_cnt) and
            matched == matchnum(args, sig, partial=False)):
            # prefer those fully matched over partial patch
            matched += 0.5
        if matched < best_match_cnt:
            continue
        if verbose:
            print("better match: {0} > {1}: {2} ".format(
                matched, best_match_cnt, concise_sig(sig)
            ), file=sys.stderr)
        if matched > best_match_cnt:
            best_match_cnt = matched
            bestcmds = [cmd]
        else:
            bestcmds.append(cmd)

    # Sort bestcmds by number of req args so we can try shortest first
    # (relies on a cmdsig being key,val where val is a list of len 1)

    def grade(cmd):
        # prefer optional arguments over required ones
        sigs = cmd['sig']
        return sum(map(lambda sig: sig.req, sigs))

    bestcmds_sorted = sorted(bestcmds, key=grade)
    if verbose:
        print("bestcmds_sorted: ", file=sys.stderr)
        pprint.PrettyPrinter(stream=sys.stderr).pprint(bestcmds_sorted)

    ex: Optional[ArgumentError] = None
    # for everything in bestcmds, look for a true match
    for cmd in bestcmds_sorted:
        sig = cmd['sig']
        try:
            valid_dict = validate(args, sig, flags=cmd.get('flags', 0))
            found = cmd
            break
        except ArgumentPrefix:
            # ignore prefix mismatches; we just haven't found
            # the right command yet
            pass
        except ArgumentMissing as e:
            ex = e
            if len(bestcmds) == 1:
                found = cmd
            break
        except ArgumentTooFew:
            # It looked like this matched the beginning, but it
            # didn't have enough args supplied.  If we're out of
            # cmdsigs we'll fall out unfound; if we're not, maybe
            # the next one matches completely.  Whine, but pass.
            if verbose:
                print('Not enough args supplied for ',
                      concise_sig(sig), file=sys.stderr)
        except ArgumentError as e:
            ex = e
            # Solid mismatch on an arg (type, range, etc.)
            # Stop now, because we have the right command but
            # some other input is invalid
            found = cmd
            break

    if found:
        if not valid_dict:
            print("Invalid command:", ex, file=sys.stderr)
            print(concise_sig(sig), ': ', cmd['help'], file=sys.stderr)
    else:
        bestcmds = [c for c in bestcmds
                    if not c.get('flags', 0) & (Flag.DEPRECATED | Flag.HIDDEN)]
        bestcmds = bestcmds[:10]  # top 10
        print('no valid command found; {0} closest matches:'.format(len(bestcmds)),
              file=sys.stderr)
        for cmd in bestcmds:
            print(concise_sig(cmd['sig']), file=sys.stderr)
    return valid_dict


def find_cmd_target(childargs: List[str]) -> Tuple[str, Optional[str]]:
    """
    Using a minimal validation, figure out whether the command
    should be sent to a monitor or an osd.  We do this before even
    asking for the 'real' set of command signatures, so we can ask the
    right daemon.
    Returns ('osd', osdid), ('pg', pgid), ('mgr', '') or ('mon', '')
    """
    sig = parse_funcsig(['tell', {'name': 'target', 'type': 'CephName'}])
    try:
        valid_dict = validate(childargs, sig, partial=True)
    except ArgumentError:
        pass
    else:
        if len(valid_dict) == 2:
            # revalidate to isolate type and id
            name = CephName()
            # if this fails, something is horribly wrong, as it just
            # validated successfully above
            name.valid(valid_dict['target'])
            assert name.nametype is not None
            return name.nametype, name.nameid

    sig = parse_funcsig(['tell', {'name': 'pgid', 'type': 'CephPgid'}])
    try:
        valid_dict = validate(childargs, sig, partial=True)
    except ArgumentError:
        pass
    else:
        if len(valid_dict) == 2:
            # pg doesn't need revalidation; the string is fine
            pgid = valid_dict['pgid']
            assert isinstance(pgid, str)
            return 'pg', pgid

    # If we reached this far it must mean that so far we've been unable to
    # obtain a proper target from childargs.  This may mean that we are not
    # dealing with a 'tell' command, or that the specified target is invalid.
    # If the latter, we likely were unable to catch it because we were not
    # really looking for it: first we tried to parse a 'CephName' (osd, mon,
    # mds, followed by and id); given our failure to parse, we tried to parse
    # a 'CephPgid' instead (e.g., 0.4a).  Considering we got this far though
    # we were unable to do so.
    #
    # We will now check if this is a tell and, if so, forcefully validate the
    # target as a 'CephName'.  This must be so because otherwise we will end
    # up sending garbage to a monitor, which is the default target when a
    # target is not explicitly specified.
    # e.g.,
    #   'ceph status' -> target is any one monitor
    #   'ceph tell mon.* status -> target is all monitors
    #   'ceph tell foo status -> target is invalid!
    if len(childargs) > 1 and childargs[0] == 'tell':
        name = CephName()
        # CephName.valid() raises on validation error; find_cmd_target()'s
        # caller should handle them
        name.valid(childargs[1])
        assert name.nametype is not None
        assert name.nameid is not None
        return name.nametype, name.nameid

    sig = parse_funcsig(['pg', {'name': 'pgid', 'type': 'CephPgid'}])
    try:
        valid_dict = validate(childargs, sig, partial=True)
    except ArgumentError:
        pass
    else:
        if len(valid_dict) == 2:
            pgid = valid_dict['pgid']
            assert isinstance(pgid, str)
            return 'pg', pgid

    return 'mon', ''


class RadosThread(threading.Thread):
    def __init__(self, func, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.func = func
        self.exception = None
        threading.Thread.__init__(self)

    def run(self):
        try:
            self.retval = self.func(*self.args, **self.kwargs)
        except Exception as e:
            self.exception = e


def run_in_thread(func: Callable[[Any, Any], Tuple[int, bytes, str]],
                  *args: Any, **kwargs: Any) -> Tuple[int, bytes, str]:
    timeout = kwargs.pop('timeout', 0)
    if timeout == 0 or timeout is None:
        # python threading module will just get blocked if timeout is `None`,
        # otherwise it will keep polling until timeout or thread stops.
        # timeout in integer when converting it to nanoseconds, but since
        # python3 uses `int64_t` for the deadline before timeout expires,
        # we have to use a safe value which does not overflow after being
        # added to current time in microseconds.
        timeout = 24 * 60 * 60
    t = RadosThread(func, *args, **kwargs)

    # allow the main thread to exit (presumably, avoid a join() on this
    # subthread) before this thread terminates.  This allows SIGINT
    # exit of a blocked call.  See below.
    t.daemon = True

    t.start()
    t.join(timeout=timeout)
    # ..but allow SIGINT to terminate the waiting.  Note: this
    # relies on the Linux kernel behavior of delivering the signal
    # to the main thread in preference to any subthread (all that's
    # strictly guaranteed is that *some* thread that has the signal
    # unblocked will receive it).  But there doesn't seem to be
    # any interface to create a thread with SIGINT blocked.
    if t.is_alive():
        raise Exception("timed out")
    elif t.exception:
        raise t.exception
    else:
        return t.retval


def send_command_retry(*args: Any, **kwargs: Any) -> Tuple[int, bytes, str]:
    while True:
        try:
            return send_command(*args, **kwargs)
        except Exception as e:
            # If our librados instance has not reached state 'connected'
            # yet, we'll see an exception like this and retry
            if ('get_command_descriptions' in str(e) and
                'object in state configuring' in str(e)):
                continue
            else:
                raise


def send_command(cluster,
                 target: Tuple[str, Optional[str]] = ('mon', ''),
                 cmd: Optional[str] = None,
                 inbuf: Optional[bytes] = b'',
                 timeout: Optional[int] = 0,
                 verbose: Optional[bool] = False) -> Tuple[int, bytes, str]:
    """
    Send a command to a daemon using librados's
    mon_command, osd_command, mgr_command, or pg_command.  Any bulk input data
    comes in inbuf.

    Returns (ret, outbuf, outs); ret is the return code, outbuf is
    the outbl "bulk useful output" buffer, and outs is any status
    or error message (intended for stderr).

    If target is osd.N, send command to that osd (except for pgid cmds)
    """
    try:
        if target[0] == 'osd':
            osdid = target[1]
            assert osdid is not None

            if verbose:
                print('submit {0} to osd.{1}'.format(cmd, osdid),
                      file=sys.stderr)
            ret, outbuf, outs = run_in_thread(
                cluster.osd_command, int(osdid), cmd, inbuf, timeout=timeout)

        elif target[0] == 'mgr':
            name = ''     # non-None empty string means "current active mgr"
            if len(target) > 1 and target[1] is not None:
                name = target[1]
            if verbose:
                print('submit {0} to {1} name {2}'.format(cmd, target[0], name),
                      file=sys.stderr)
            ret, outbuf, outs = run_in_thread(
                cluster.mgr_command, cmd, inbuf, timeout=timeout, target=name)

        elif target[0] == 'mon-mgr':
            if verbose:
                print('submit {0} to {1}'.format(cmd, target[0]),
                      file=sys.stderr)
            ret, outbuf, outs = run_in_thread(
                cluster.mgr_command, cmd, inbuf, timeout=timeout)

        elif target[0] == 'pg':
            pgid = target[1]
            # pgid will already be in the command for the pg <pgid>
            # form, but for tell <pgid>, we need to put it in
            if cmd:
                cmddict = json.loads(cmd)
                cmddict['pgid'] = pgid
            else:
                cmddict = dict(pgid=pgid)
            cmd = json.dumps(cmddict)
            if verbose:
                print('submit {0} for pgid {1}'.format(cmd, pgid),
                      file=sys.stderr)
            ret, outbuf, outs = run_in_thread(
                cluster.pg_command, pgid, cmd, inbuf, timeout=timeout)

        elif target[0] == 'mon':
            if verbose:
                print('{0} to {1}'.format(cmd, target[0]),
                      file=sys.stderr)
            if len(target) < 2 or target[1] == '':
                ret, outbuf, outs = run_in_thread(
                    cluster.mon_command, cmd, inbuf, timeout=timeout)
            else:
                ret, outbuf, outs = run_in_thread(
                    cluster.mon_command, cmd, inbuf, timeout=timeout, target=target[1])
        elif target[0] == 'mds':
            mds_spec = target[1]

            if verbose:
                print('submit {0} to mds.{1}'.format(cmd, mds_spec),
                      file=sys.stderr)

            try:
                from cephfs import LibCephFS
            except ImportError:
                raise RuntimeError("CephFS unavailable, have you installed libcephfs?")

            filesystem = LibCephFS(rados_inst=cluster)
            filesystem.init()
            ret, outbuf, outs = \
                filesystem.mds_command(mds_spec, cmd, inbuf)
            filesystem.shutdown()
        else:
            raise ArgumentValid("Bad target type '{0}'".format(target[0]))

    except Exception as e:
        if not isinstance(e, ArgumentError):
            raise RuntimeError('"{0}": exception {1}'.format(cmd, e))
        else:
            raise

    return ret, outbuf, outs


def json_command(cluster,
                 target: Tuple[str, Optional[str]] = ('mon', ''),
                 prefix: Optional[str] = None,
                 argdict: Optional[ValidatedArgs] = None,
                 inbuf: Optional[bytes] = b'',
                 timeout: Optional[int] = 0,
                 verbose: Optional[bool] = False) -> Tuple[int, bytes, str]:
    """
    Serialize a command and up a JSON command and send it with send_command() above.
    Prefix may be supplied separately or in argdict.  Any bulk input
    data comes in inbuf.

    If target is osd.N, send command to that osd (except for pgid cmds)

    :param cluster: ``rados.Rados`` instance
    :param prefix: String to inject into command arguments as 'prefix'
    :param argdict: Command arguments
    """
    cmddict: ValidatedArgs = {}
    if prefix:
        cmddict.update({'prefix': prefix})

    if argdict:
        cmddict.update(argdict)
        if 'target' in argdict:
            target = cast(Tuple[str, str], argdict['target'])

    try:
        if target[0] == 'osd':
            osdtarg = CephName()
            osdtarget = '{0}.{1}'.format(*target)
            # prefer target from cmddict if present and valid
            if 'target' in cmddict:
                osdtarget = cast(str, cmddict.pop('target'))
            try:
                osdtarg.valid(osdtarget)
                target = ('osd', osdtarg.nameid)
            except:
                # use the target we were originally given
                pass
        ret, outbuf, outs = send_command_retry(cluster,
                                               target, json.dumps(cmddict),
                                               inbuf, timeout, verbose)

    except Exception as e:
        if not isinstance(e, ArgumentError):
            raise RuntimeError('"{0}": exception {1}'.format(argdict, e))
        else:
            raise

    return ret, outbuf, outs
