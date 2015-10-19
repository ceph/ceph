"""
Types and routines used by the ceph CLI as well as the RESTful
interface.  These have to do with querying the daemons for
command-description information, validating user command input against
those descriptions, and submitting the command to the appropriate
daemon.

Copyright (C) 2013 Inktank Storage, Inc.

LGPL2.  See file COPYING.
"""
import copy
import errno
import json
import os
import pprint
import re
import socket
import stat
import sys
import threading
import types
import uuid


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
        a = ''
        if hasattr(self, 'typeargs'):
            a = self.typeargs
        return '{0}(\'{1}\')'.format(self.__class__.__name__, a)

    def __str__(self):
        """
        where __repr__ (ideally) returns a string that could be used to
        reproduce the object, __str__ returns one you'd like to see in
        print messages.  Use __str__ to format the argtype descriptor
        as it would be useful in a command usage message.
        """
        return '<{0}>'.format(self.__class__.__name__)

    def complete(self, s):
        return []


class CephInt(CephArgtype):
    """
    range-limited integers, [+|-][0-9]+ or 0x[0-9a-f]+
    range: list of 1 or 2 ints, [min] or [min,max]
    """
    def __init__(self, range=''):
        if range == '':
            self.range = list()
        else:
            self.range = list(range.split('|'))
            self.range = map(long, self.range)

    def valid(self, s, partial=False):
        try:
            val = long(s)
        except ValueError:
            raise ArgumentValid("{0} doesn't represent an int".format(s))
        if len(self.range) == 2:
            if val < self.range[0] or val > self.range[1]:
                raise ArgumentValid("{0} not in range {1}".format(val, self.range))
        elif len(self.range) == 1:
            if val < self.range[0]:
                raise ArgumentValid("{0} not in range {1}".format(val, self.range))
        self.val = val

    def __str__(self):
        r = ''
        if len(self.range) == 1:
            r = '[{0}-]'.format(self.range[0])
        if len(self.range) == 2:
            r = '[{0}-{1}]'.format(self.range[0], self.range[1])

        return '<int{0}>'.format(r)


class CephFloat(CephArgtype):
    """
    range-limited float type
    range: list of 1 or 2 floats, [min] or [min, max]
    """
    def __init__(self, range=''):
        if range == '':
            self.range = list()
        else:
            self.range = list(range.split('|'))
            self.range = map(float, self.range)

    def valid(self, s, partial=False):
        try:
            val = float(s)
        except ValueError:
            raise ArgumentValid("{0} doesn't represent a float".format(s))
        if len(self.range) == 2:
            if val < self.range[0] or val > self.range[1]:
                raise ArgumentValid("{0} not in range {1}".format(val, self.range))
        elif len(self.range) == 1:
            if val < self.range[0]:
                raise ArgumentValid("{0} not in range {1}".format(val, self.range))
        self.val = val

    def __str__(self):
        r = ''
        if len(self.range) == 1:
            r = '[{0}-]'.format(self.range[0])
        if len(self.range) == 2:
            r = '[{0}-{1}]'.format(self.range[0], self.range[1])
        return '<float{0}>'.format(r)


class CephString(CephArgtype):
    """
    String; pretty generic.  goodchars is a RE char class of valid chars
    """
    def __init__(self, goodchars=''):
        from string import printable
        try:
            re.compile(goodchars)
        except:
            raise ValueError('CephString(): "{0}" is not a valid RE'.
                             format(goodchars))
        self.goodchars = goodchars
        self.goodset = frozenset(
            [c for c in printable if re.match(goodchars, c)]
        )

    def valid(self, s, partial=False):
        sset = set(s)
        if self.goodset and not sset <= self.goodset:
            raise ArgumentFormat("invalid chars {0} in {1}".
                                 format(''.join(sset - self.goodset), s))
        self.val = s

    def __str__(self):
        b = ''
        if self.goodchars:
            b += '(goodchars {0})'.format(self.goodchars)
        return '<string{0}>'.format(b)

    def complete(self, s):
        if s == '':
            return []
        else:
            return [s]


class CephSocketpath(CephArgtype):
    """
    Admin socket path; check that it's readable and S_ISSOCK
    """
    def valid(self, s, partial=False):
        mode = os.stat(s).st_mode
        if not stat.S_ISSOCK(mode):
            raise ArgumentValid('socket path {0} is not a socket'.format(s))
        self.val = s

    def __str__(self):
        return '<admin-socket-path>'


class CephIPAddr(CephArgtype):
    """
    IP address (v4 or v6) with optional port
    """
    def valid(self, s, partial=False):
        # parse off port, use socket to validate addr
        type = 6
        if s.startswith('['):
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
            except:
                raise ArgumentValid('{0}: invalid IPv4 address'.format(a))
        else:
            # v6
            if s.startswith('['):
                end = s.find(']')
                if end == -1:
                    raise ArgumentFormat('{0} missing terminating ]'.format(s))
                if s[end + 1] == ':':
                    try:
                        p = int(s[end + 2])
                    except:
                        raise ArgumentValid('{0}: bad port number'.format(s))
                a = s[1:end]
            else:
                a = s
                p = None
            try:
                socket.inet_pton(socket.AF_INET6, a)
            except:
                raise ArgumentValid('{0} not valid IPv6 address'.format(s))
        if p is not None and long(p) > 65535:
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
    def valid(self, s, partial=False):
        nonce = None
        if '/' in s:
            ip, nonce = s.split('/')
        else:
            ip = s
        super(self.__class__, self).valid(ip)
        if nonce:
            nonce_long = None
            try:
                nonce_long = long(nonce)
            except ValueError:
                pass
            if nonce_long is None or nonce_long < 0:
                raise ArgumentValid(
                    '{0}: invalid entity, nonce {1} not integer > 0'.
                    format(s, nonce)
                )
        self.val = s

    def __str__(self):
        return '<EntityAddr>'


class CephPoolname(CephArgtype):
    """
    Pool name; very little utility
    """
    def __str__(self):
        return '<poolname>'


class CephObjectname(CephArgtype):
    """
    Object name.  Maybe should be combined with Pool name as they're always
    present in pairs, and then could be checked for presence
    """
    def __str__(self):
        return '<objectname>'


class CephPgid(CephArgtype):
    """
    pgid, in form N.xxx (N = pool number, xxx = hex pgnum)
    """
    def valid(self, s, partial=False):
        if s.find('.') == -1:
            raise ArgumentFormat('pgid has no .')
        poolid, pgnum = s.split('.', 1)
        if poolid < 0:
            raise ArgumentFormat('pool {0} < 0'.format(poolid))
        try:
            pgnum = int(pgnum, 16)
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
    def __init__(self):
        self.nametype = None
        self.nameid = None

    def valid(self, s, partial=False):
        if s == '*':
            self.val = s
            return
        if s.find('.') == -1:
            raise ArgumentFormat('CephName: no . in {0}'.format(s))
        else:
            t, i = s.split('.', 1)
            if t not in ('osd', 'mon', 'client', 'mds'):
                raise ArgumentValid('unknown type ' + t)
            if t == 'osd':
                if i != '*':
                    try:
                        i = int(i)
                    except:
                        raise ArgumentFormat('osd id ' + i + ' not integer')
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
        self.nametype = None
        self.nameid = None

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
            i = int(i)
        except:
            raise ArgumentFormat('osd id ' + i + ' not integer')
        if i < 0:
            raise ArgumentFormat('osd id {0} is less than 0'.format(i))
        self.nametype = t
        self.nameid = i
        self.val = i

    def __str__(self):
        return '<osdname (id|osd.id)>'


class CephChoices(CephArgtype):
    """
    Set of string literals; init with valid choices
    """
    def __init__(self, strings='', **kwargs):
        self.strings = strings.split('|')

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

    def complete(self, s):
        all_elems = [token for token in self.strings if token.startswith(s)]
        return all_elems


class CephFilepath(CephArgtype):
    """
    Openable file
    """
    def valid(self, s, partial=False):
        try:
            f = open(s, 'a+')
        except Exception as e:
            raise ArgumentValid('can\'t open {0}: {1}'.format(s, e))
        f.close()
        self.val = s

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
            long(val)
        except:
            raise ArgumentFormat('can\'t convert {0} to integer'.format(val))
        try:
            long(bits)
        except:
            raise ArgumentFormat('can\'t convert {0} to integer'.format(bits))
        self.val = s

    def __str__(self):
        return "<CephFS fragment ID (0xvvv/bbb)>"


class CephUUID(CephArgtype):
    """
    CephUUID: pretty self-explanatory
    """
    def valid(self, s, partial=False):
        try:
            uuid.UUID(s)
        except Exception as e:
            raise ArgumentFormat('invalid UUID {0}: {1}'.format(s, e))
        self.val = s

    def __str__(self):
        return '<uuid>'


class CephPrefix(CephArgtype):
    """
    CephPrefix: magic type for "all the first n fixed strings"
    """
    def __init__(self, prefix=''):
        self.prefix = prefix

    def valid(self, s, partial=False):
        try:
            # `prefix` can always be converted into unicode when being compared,
            # but `s` could be anything passed by user.
            s = unicode(s)
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

    def __str__(self):
        return self.prefix

    def complete(self, s):
        if self.prefix.startswith(s):
            return [self.prefix.rstrip(' ')]
        else:
            return []


class argdesc(object):
    """
    argdesc(typename, name='name', n=numallowed|N,
            req=False, helptext=helptext, **kwargs (type-specific))

    validation rules:
    typename: type(**kwargs) will be constructed
    later, type.valid(w) will be called with a word in that position

    name is used for parse errors and for constructing JSON output
    n is a numeric literal or 'n|N', meaning "at least one, but maybe more"
    req=False means the argument need not be present in the list
    helptext is the associated help for the command
    anything else are arguments to pass to the type constructor.

    self.instance is an instance of type t constructed with typeargs.

    valid() will later be called with input to validate against it,
    and will store the validated value in self.instance.val for extraction.
    """
    def __init__(self, t, name=None, n=1, req=True, **kwargs):
        if isinstance(t, types.StringTypes):
            self.t = CephPrefix
            self.typeargs = {'prefix': t}
            self.req = True
        else:
            self.t = t
            self.typeargs = kwargs
            self.req = bool(req == True or req == 'True')

        self.name = name
        self.N = (n in ['n', 'N'])
        if self.N:
            self.n = 1
        else:
            self.n = int(n)
        self.instance = self.t(**self.typeargs)

    def __repr__(self):
        r = 'argdesc(' + str(self.t) + ', '
        internals = ['N', 'typeargs', 'instance', 't']
        for (k, v) in self.__dict__.iteritems():
            if k.startswith('__') or k in internals:
                pass
            else:
                # undo modification from __init__
                if k == 'n' and self.N:
                    v = 'N'
                r += '{0}={1}, '.format(k, v)
        for (k, v) in self.typeargs.iteritems():
            r += '{0}={1}, '.format(k, v)
        return r[:-2] + ')'

    def __str__(self):
        if ((self.t == CephChoices and len(self.instance.strings) == 1)
           or (self.t == CephPrefix)):
            s = str(self.instance)
        else:
            s = '{0}({1})'.format(self.name, str(self.instance))
            if self.N:
                s += ' [' + str(self.instance) + '...]'
        if not self.req:
            s = '{' + s + '}'
        return s

    def helpstr(self):
        """
        like str(), but omit parameter names (except for CephString,
        which really needs them)
        """
        if self.t == CephString:
            chunk = '<{0}>'.format(self.name)
        else:
            chunk = str(self.instance)
        s = chunk
        if self.N:
            s += ' [' + chunk + '...]'
        if not self.req:
            s = '{' + s + '}'
        return s

    def complete(self, s):
        return self.instance.complete(s)


def concise_sig(sig):
    """
    Return string representation of sig useful for syntax reference in help
    """
    return ' '.join([d.helpstr() for d in sig])


def descsort(sh1, sh2):
    """
    sort descriptors by prefixes, defined as the concatenation of all simple
    strings in the descriptor; this works out to just the leading strings.
    """
    return cmp(concise_sig(sh1['sig']), concise_sig(sh2['sig']))


def parse_funcsig(sig):
    """
    parse a single descriptor (array of strings or dicts) into a
    dict of function descriptor/validators (objects of CephXXX type)
    """
    newsig = []
    argnum = 0
    for desc in sig:
        argnum += 1
        if isinstance(desc, types.StringTypes):
            t = CephPrefix
            desc = {'type': t, 'name': 'prefix', 'prefix': desc}
        else:
            # not a simple string, must be dict
            if 'type' not in desc:
                s = 'JSON descriptor {0} has no type'.format(sig)
                raise JsonFormat(s)
            # look up type string in our globals() dict; if it's an
            # object of type types.TypeType, it must be a
            # locally-defined class. otherwise, we haven't a clue.
            if desc['type'] in globals():
                t = globals()[desc['type']]
                if not isinstance(t, types.TypeType):
                    s = 'unknown type {0}'.format(desc['type'])
                    raise JsonFormat(s)
            else:
                s = 'unknown type {0}'.format(desc['type'])
                raise JsonFormat(s)

        kwargs = dict()
        for key, val in desc.items():
            if key not in ['type', 'name', 'n', 'req']:
                kwargs[key] = val
        newsig.append(argdesc(t,
                              name=desc.get('name', None),
                              n=desc.get('n', 1),
                              req=desc.get('req', True),
                              **kwargs))
    return newsig


def parse_json_funcsigs(s, consumer):
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
        print >> sys.stderr, "Couldn't parse JSON {0}: {1}".format(s, e)
        raise e
    sigdict = {}
    for cmdtag, cmd in overall.iteritems():
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


def validate_one(word, desc, partial=False):
    """
    validate_one(word, desc, partial=False)

    validate word against the constructed instance of the type
    in desc.  May raise exception.  If it returns false (and doesn't
    raise an exception), desc.instance.val will
    contain the validated value (in the appropriate type).
    """
    desc.instance.valid(word, partial)
    desc.numseen += 1
    if desc.N:
        desc.n = desc.numseen + 1


def matchnum(args, signature, partial=False):
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
        setattr(desc, 'numseen', 0)
        while desc.numseen < desc.n:
            # if there are no more arguments, return
            if not words:
                return matchcnt
            word = words.pop(0)

            try:
                # only allow partial matching if we're on the last supplied
                # word; avoid matching foo bar and foot bar just because
                # partial is set
                validate_one(word, desc, partial and (len(words) == 0))
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


def get_next_arg(desc, args):
    '''
    Get either the value matching key 'desc.name' or the next arg in
    the non-dict list.  Return None if args are exhausted.  Used in
    validate() below.
    '''
    arg = None
    if isinstance(args, dict):
        arg = args.pop(desc.name, None)
        # allow 'param=param' to be expressed as 'param'
        if arg == '':
            arg = desc.name
        # Hack, or clever?  If value is a list, keep the first element,
        # push rest back onto myargs for later processing.
        # Could process list directly, but nesting here is already bad
        if arg and isinstance(arg, list):
            args[desc.name] = arg[1:]
            arg = arg[0]
    elif args:
        arg = args.pop(0)
        if arg and isinstance(arg, list):
            args = arg[1:] + args
            arg = arg[0]
    return arg


def store_arg(desc, d):
    '''
    Store argument described by, and held in, thanks to valid(),
    desc into the dictionary d, keyed by desc.name.  Three cases:

    1) desc.N is set: value in d is a list
    2) prefix: multiple args are joined with ' ' into one d{} item
    3) single prefix or other arg: store as simple value

    Used in validate() below.
    '''
    if desc.N:
        # value should be a list
        if desc.name in d:
            d[desc.name] += [desc.instance.val]
        else:
            d[desc.name] = [desc.instance.val]
    elif (desc.t == CephPrefix) and (desc.name in d):
        # prefixes' values should be a space-joined concatenation
        d[desc.name] += ' ' + desc.instance.val
    else:
        # if first CephPrefix or any other type, just set it
        d[desc.name] = desc.instance.val


def validate(args, signature, partial=False):
    """
    validate(args, signature, partial=False)

    args is a list of either words or k,v pairs representing a possible
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
    d = dict()
    for desc in mysig:
        setattr(desc, 'numseen', 0)
        while desc.numseen < desc.n:
            myarg = get_next_arg(desc, myargs)

            # no arg, but not required?  Continue consuming mysig
            # in case there are later required args
            if not myarg and not desc.req:
                break

            # out of arguments for a required param?
            # Either return (if partial validation) or raise
            if not myarg and desc.req:
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
                        raise ArgumentNumber(
                            'missing required parameter {0}'.format(desc)
                        )
                    raise ArgumentNumber(
                        'saw {0} of {1}, expected {2}'.
                        format(desc.numseen, desc, desc.n)
                    )
                break

            # Have an arg; validate it
            try:
                validate_one(myarg, desc)
                valid = True
            except ArgumentError as e:
                valid = False
            if not valid:
                # argument mismatch
                if not desc.req:
                    # if not required, just push back; it might match
                    # the next arg
                    save_exception = [ myarg, e ]
                    myargs.insert(0, myarg)
                    break
                else:
                    # hm, it was required, so time to return/raise
                    if partial:
                        return d
                    raise e

            # Whew, valid arg acquired.  Store in dict
            matchcnt += 1
            store_arg(desc, d)
            # Clear prior exception
            save_exception = None

    # Done with entire list of argdescs
    if matchcnt < reqsiglen:
        raise ArgumentTooFew("not enough arguments given")

    if myargs and not partial:
        if save_exception:
            print >> sys.stderr, save_exception[0], 'not valid: ', str(save_exception[1])
        raise ArgumentError("unused arguments: " + str(myargs))

    # Finally, success
    return d


def cmdsiglen(sig):
    sigdict = sig.values()
    assert len(sigdict) == 1
    return len(sig.values()[0]['sig'])


def validate_command(sigdict, args, verbose=False):
    """
    turn args into a valid dictionary ready to be sent off as JSON,
    validated against sigdict.
    """
    if verbose:
        print >> sys.stderr, \
            "validate_command: " + " ".join(args)
    found = []
    valid_dict = {}
    if args:
        # look for best match, accumulate possibles in bestcmds
        # (so we can maybe give a more-useful error message)
        best_match_cnt = 0
        bestcmds = []
        for cmdtag, cmd in sigdict.iteritems():
            sig = cmd['sig']
            matched = matchnum(args, sig, partial=True)
            if matched > best_match_cnt:
                if verbose:
                    print >> sys.stderr, \
                        "better match: {0} > {1}: {2}:{3} ".\
                        format(matched, best_match_cnt, cmdtag,
                               concise_sig(sig))
                best_match_cnt = matched
                bestcmds = [{cmdtag: cmd}]
            elif matched == best_match_cnt:
                if verbose:
                    print >> sys.stderr, \
                        "equal match: {0} > {1}: {2}:{3} ".\
                        format(matched, best_match_cnt, cmdtag,
                               concise_sig(sig))
                bestcmds.append({cmdtag: cmd})

        # Sort bestcmds by number of args so we can try shortest first
        # (relies on a cmdsig being key,val where val is a list of len 1)
        bestcmds_sorted = sorted(bestcmds,
                                 cmp=lambda x, y: cmp(cmdsiglen(x), cmdsiglen(y)))

        if verbose:
            print >> sys.stderr, "bestcmds_sorted: "
            pprint.PrettyPrinter(stream=sys.stderr).pprint(bestcmds_sorted)

        # for everything in bestcmds, look for a true match
        for cmdsig in bestcmds_sorted:
            for cmd in cmdsig.itervalues():
                sig = cmd['sig']
                try:
                    valid_dict = validate(args, sig)
                    found = cmd
                    break
                except ArgumentPrefix:
                    # ignore prefix mismatches; we just haven't found
                    # the right command yet
                    pass
                except ArgumentTooFew:
                    # It looked like this matched the beginning, but it
                    # didn't have enough args supplied.  If we're out of
                    # cmdsigs we'll fall out unfound; if we're not, maybe
                    # the next one matches completely.  Whine, but pass.
                    if verbose:
                        print >> sys.stderr, 'Not enough args supplied for ', \
                            concise_sig(sig)
                except ArgumentError as e:
                    # Solid mismatch on an arg (type, range, etc.)
                    # Stop now, because we have the right command but
                    # some other input is invalid
                    print >> sys.stderr, "Invalid command: ", str(e)
                    print >> sys.stderr, concise_sig(sig), ': ', cmd['help']
                    return {}
            if found:
                break

        if not found:
            print >> sys.stderr, 'no valid command found; 10 closest matches:'
            for cmdsig in bestcmds[:10]:
                for (cmdtag, cmd) in cmdsig.iteritems():
                    print >> sys.stderr, concise_sig(cmd['sig'])
            return None

        return valid_dict


def find_cmd_target(childargs):
    """
    Using a minimal validation, figure out whether the command
    should be sent to a monitor or an osd.  We do this before even
    asking for the 'real' set of command signatures, so we can ask the
    right daemon.
    Returns ('osd', osdid), ('pg', pgid), or ('mon', '')
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
            return name.nametype, name.nameid

    sig = parse_funcsig(['tell', {'name': 'pgid', 'type': 'CephPgid'}])
    try:
        valid_dict = validate(childargs, sig, partial=True)
    except ArgumentError:
        pass
    else:
        if len(valid_dict) == 2:
            # pg doesn't need revalidation; the string is fine
            return 'pg', valid_dict['pgid']

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
        return name.nametype, name.nameid

    sig = parse_funcsig(['pg', {'name': 'pgid', 'type': 'CephPgid'}])
    try:
        valid_dict = validate(childargs, sig, partial=True)
    except ArgumentError:
        pass
    else:
        if len(valid_dict) == 2:
            return 'pg', valid_dict['pgid']

    return 'mon', ''


class RadosThread(threading.Thread):
    def __init__(self, target, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.target = target
	self.exception = None
        threading.Thread.__init__(self)

    def run(self):
        try:
		self.retval = self.target(*self.args, **self.kwargs)
	except Exception as e:
		self.exception = e


# time in seconds between each call to t.join() for child thread
POLL_TIME_INCR = 0.5


def run_in_thread(target, *args, **kwargs):
    interrupt = False
    timeout = kwargs.pop('timeout', 0)
    countdown = timeout
    t = RadosThread(target, *args, **kwargs)

    # allow the main thread to exit (presumably, avoid a join() on this
    # subthread) before this thread terminates.  This allows SIGINT
    # exit of a blocked call.  See below.
    t.daemon = True

    t.start()
    try:
        # poll for thread exit
        while t.is_alive():
            t.join(POLL_TIME_INCR)
            if timeout and t.is_alive():
                countdown = countdown - POLL_TIME_INCR
                if countdown <= 0:
                    raise KeyboardInterrupt

        t.join()        # in case t exits before reaching the join() above
    except KeyboardInterrupt:
        # ..but allow SIGINT to terminate the waiting.  Note: this
        # relies on the Linux kernel behavior of delivering the signal
        # to the main thread in preference to any subthread (all that's
        # strictly guaranteed is that *some* thread that has the signal
        # unblocked will receive it).  But there doesn't seem to be
        # any interface to create t with SIGINT blocked.
        interrupt = True

    if interrupt:
        t.retval = -errno.EINTR
    if t.exception:
        raise t.exception
    return t.retval


def send_command(cluster, target=('mon', ''), cmd=None, inbuf='', timeout=0,
                 verbose=False):
    """
    Send a command to a daemon using librados's
    mon_command, osd_command, or pg_command.  Any bulk input data
    comes in inbuf.

    Returns (ret, outbuf, outs); ret is the return code, outbuf is
    the outbl "bulk useful output" buffer, and outs is any status
    or error message (intended for stderr).

    If target is osd.N, send command to that osd (except for pgid cmds)
    """
    cmd = cmd or []
    try:
        if target[0] == 'osd':
            osdid = target[1]

            if verbose:
                print >> sys.stderr, 'submit {0} to osd.{1}'.\
                    format(cmd, osdid)
            ret, outbuf, outs = run_in_thread(
                cluster.osd_command, osdid, cmd, inbuf, timeout)

        elif target[0] == 'pg':
            pgid = target[1]
            # pgid will already be in the command for the pg <pgid>
            # form, but for tell <pgid>, we need to put it in
            if cmd:
                cmddict = json.loads(cmd[0])
                cmddict['pgid'] = pgid
            else:
                cmddict = dict(pgid=pgid)
            cmd = [json.dumps(cmddict)]
            if verbose:
                print >> sys.stderr, 'submit {0} for pgid {1}'.\
                    format(cmd, pgid)
            ret, outbuf, outs = run_in_thread(
                cluster.pg_command, pgid, cmd, inbuf, timeout)

        elif target[0] == 'mon':
            if verbose:
                print >> sys.stderr, '{0} to {1}'.\
                    format(cmd, target[0])
            if target[1] == '':
                ret, outbuf, outs = run_in_thread(
                    cluster.mon_command, cmd, inbuf, timeout)
            else:
                ret, outbuf, outs = run_in_thread(
                    cluster.mon_command, cmd, inbuf, timeout, target[1])
        elif target[0] == 'mds':
            mds_spec = target[1]

            if verbose:
                print >> sys.stderr, 'submit {0} to mds.{1}'.\
                    format(cmd, mds_spec)

            try:
                from cephfs import LibCephFS
            except ImportError:
                raise RuntimeError("CephFS unavailable, have you installed libcephfs?")

            filesystem = LibCephFS(cluster.conf_defaults, cluster.conffile)
            filesystem.conf_parse_argv(cluster.parsed_args)

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


def json_command(cluster, target=('mon', ''), prefix=None, argdict=None,
                 inbuf='', timeout=0, verbose=False):
    """
    Format up a JSON command and send it with send_command() above.
    Prefix may be supplied separately or in argdict.  Any bulk input
    data comes in inbuf.

    If target is osd.N, send command to that osd (except for pgid cmds)
    """
    cmddict = {}
    if prefix:
        cmddict.update({'prefix': prefix})
    if argdict:
        cmddict.update(argdict)

    # grab prefix for error messages
    prefix = cmddict['prefix']

    try:
        if target[0] == 'osd':
            osdtarg = CephName()
            osdtarget = '{0}.{1}'.format(*target)
            # prefer target from cmddict if present and valid
            if 'target' in cmddict:
                osdtarget = cmddict.pop('target')
            try:
                osdtarg.valid(osdtarget)
                target = ('osd', osdtarg.nameid)
            except:
                # use the target we were originally given
                pass

        ret, outbuf, outs = send_command(cluster, target, [json.dumps(cmddict)],
                                         inbuf, timeout, verbose)

    except Exception as e:
        if not isinstance(e, ArgumentError):
            raise RuntimeError('"{0}": exception {1}'.format(argdict, e))
        else:
            raise

    return ret, outbuf, outs

