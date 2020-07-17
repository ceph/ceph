"""
Prints a statically compiled list of all commands.

See

* /admin/doc-requirements.txt
* /doc/scripts/gen_mon_command_api.py

Rational for putting this file here is to allow others to make use of this output.
"""
import json
import os
import io
import sys

from pcpp.preprocessor import Preprocessor, OutputDirective, Action

os.environ['UNITTEST'] = 'true'

script_dir = os.path.dirname(os.path.realpath(__file__))

mgr_dir = os.path.abspath(script_dir + '/../../src/pybind/mgr')

sys.path.insert(0, mgr_dir)

from tests import mock, M


def param_to_sig(p):
    try:
        return {kv.split('=')[0]: kv.split('=')[1] for kv in p.split(',')}
    except IndexError:
        return p

def cmd_to_sig(cmd):
    sig = cmd.split()
    return [param_to_sig(s) or s for s in sig]


def list_mgr_module(m_name):
    sys.modules['rados'] = mock.Mock()
    sys.modules['rbd'] = mock.Mock()
    sys.modules['cephfs'] = mock.Mock()
    sys.modules['dateutil'] = mock.Mock()
    sys.modules['dateutil.parser'] = mock.Mock()

    # make dashboard happy
    sys.modules['OpenSSL'] = mock.Mock()
    sys.modules['jwt'] = mock.Mock()
    sys.modules['bcrypt'] = mock.Mock()

    sys.modules['scipy'] = mock.Mock()
    sys.modules['jsonpatch'] = mock.Mock()
    sys.modules['rook.rook_client'] = mock.Mock()
    sys.modules['rook.rook_client.ceph'] = mock.Mock()

    sys.modules['cherrypy'] = mock.Mock(__version__="3.2.3")

    # make restful happy:
    sys.modules['pecan'] = mock.Mock()
    sys.modules['pecan.rest'] = mock.Mock()
    sys.modules['pecan.hooks'] = mock.Mock()
    sys.modules['werkzeug'] = mock.Mock()
    sys.modules['werkzeug.serving'] = mock.Mock()

    mgr_mod = __import__(m_name, globals(), locals(), [], 0)

    def subclass(x):
        try:
            return issubclass(x, M)
        except TypeError:
            return False

    ms = [c for c in mgr_mod.__dict__.values() if subclass(c) and 'Standby' not in c.__name__]
    [m] = ms
    assert isinstance(m.COMMANDS, list)
    return m.COMMANDS


def from_mgr_modules():
    names = [name for name in os.listdir(mgr_dir)
             if os.path.isdir(os.path.join(mgr_dir, name)) and
             os.path.isfile(os.path.join(mgr_dir, name, '__init__.py')) and
             name not in ['tests']]

    comms = sum([list_mgr_module(name) for name in names], [])
    for c in comms:
        if 'handler' in c:
            del c['handler']
        c['sig'] = cmd_to_sig(c['cmd'])
        del c['cmd']
        c['flags'] = (1 << 3)
        c['module'] = 'mgr'
    return comms


def from_mon_commands_h():
    input_str = """
    #include "{script_dir}/../mon/MonCommands.h"
    #include "{script_dir}/../mgr/MgrCommands.h"
    """.format(script_dir=script_dir)

    cmds = []

    class MyProcessor(Preprocessor):
        def __init__(self):
            super(MyProcessor, self).__init__()
            self.undef('__DATE__')
            self.undef('__TIME__')
            self.expand_linemacro = False
            self.expand_filemacro = False
            self.expand_countermacro = False
            self.line_directive = '#line'
            self.define("__PCPP_VERSION__ " + '')
            self.define("__PCPP_ALWAYS_FALSE__ 0")
            self.define("__PCPP_ALWAYS_TRUE__ 1")
            self.parse(input_str)
            out = io.StringIO()
            self.write(out)
            out.seek(0)
            s = out.read()

            NONE = 0
            NOFORWARD = (1 << 0)
            OBSOLETE = (1 << 1)
            DEPRECATED = (1 << 2)
            MGR = (1 << 3)
            POLL = (1 << 4)
            HIDDEN = (1 << 5)
            TELL = (1 << 6)

            def FLAG(a):
                return a

            def COMMAND(cmd, desc, module, perm):
                cmds.append({
                    'cmd': cmd,
                    'desc': desc,
                    'module': module,
                    'perm': perm
                })

            def COMMAND_WITH_FLAG(cmd, desc, module, perm, flag):
                cmds.append({
                    'cmd': cmd,
                    'desc': desc,
                    'module': module,
                    'perm': perm,
                    'flags': flag
                })

            exec(s, globals(), locals())

    MyProcessor()
    for c in cmds:
        if 'handler' in c:
            del c['handler']
        c['sig'] = cmd_to_sig(c['cmd'])
        del c['cmd']
    return cmds


def gen_commands_dicts():
    comms = from_mon_commands_h() + from_mgr_modules()
    comms = sorted(comms, key=lambda c: [e for e in c['sig'] if isinstance(e, str)])
    return comms

if __name__ == '__main__':
    print(json.dumps(gen_commands_dicts(), indent=2, sort_keys=True))
