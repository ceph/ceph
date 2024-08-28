import io
import os
import sys
import contextlib

from docutils.parsers.rst import directives
from docutils.parsers.rst import Directive
from jinja2 import Template
from pcpp.preprocessor import Preprocessor
from sphinx.util import logging
from sphinx.util.console import bold
from importlib import reload

logger = logging.getLogger(__name__)


class Flags:
    NOFORWARD = (1 << 0)
    OBSOLETE = (1 << 1)
    DEPRECATED = (1 << 2)
    MGR = (1 << 3)
    POLL = (1 << 4)
    HIDDEN = (1 << 5)

    VALS = {
        NOFORWARD: 'no_forward',
        OBSOLETE: 'obsolete',
        DEPRECATED: 'deprecated',
        MGR: 'mgr',
        POLL: 'poll',
        HIDDEN: 'hidden',
    }

    def __init__(self, fs):
        self.fs = fs

    def __contains__(self, other):
        return other in str(self)

    def __str__(self):
        keys = Flags.VALS.keys()
        es = {Flags.VALS[k] for k in keys if self.fs & k == k}
        return ', '.join(sorted(es))

    def __bool__(self):
        return bool(str(self))


class CmdParam(object):
    t = {
        'CephInt': 'int',
        'CephString': 'str',
        'CephChoices': 'str',
        'CephPgid': 'str',
        'CephOsdName': 'str',
        'CephPoolname': 'str',
        'CephObjectname': 'str',
        'CephUUID': 'str',
        'CephEntityAddr': 'str',
        'CephIPAddr': 'str',
        'CephName': 'str',
        'CephBool': 'bool',
        'CephFloat': 'float',
        'CephFilepath': 'str',
    }

    bash_example = {
        'CephInt': '1',
        'CephString': 'string',
        'CephChoices': 'choice',
        'CephPgid': '0',
        'CephOsdName': 'osd.0',
        'CephPoolname': 'poolname',
        'CephObjectname': 'objectname',
        'CephUUID': 'uuid',
        'CephEntityAddr': 'entityaddr',
        'CephIPAddr': '0.0.0.0',
        'CephName': 'name',
        'CephBool': 'true',
        'CephFloat': '0.0',
        'CephFilepath': '/path/to/file',
    }

    def __init__(self, type, name,
                 who=None, n=None, req=True, range=None, strings=None,
                 goodchars=None, positional=True):
        self.type = type
        self.name = name
        self.who = who
        self.n = n == 'N'
        self.req = req != 'false'
        self.range = range.split('|') if range else []
        self.strings = strings.split('|') if strings else []
        self.goodchars = goodchars
        self.positional = positional != 'false'

        assert who == None

    def help(self):
        advanced = []
        if self.type != 'CephString':
            advanced.append(self.type + ' ')
        if self.range:
            advanced.append('range= ``{}`` '.format('..'.join(self.range)))
        if self.strings:
            advanced.append('strings=({}) '.format(' '.join(self.strings)))
        if self.goodchars:
            advanced.append('goodchars= ``{}`` '.format(self.goodchars))
        if self.n:
            advanced.append('(can be repeated)')

        advanced = advanced or ["(string)"]
        return ' '.join(advanced)

    def mk_example_value(self):
        if self.type == 'CephChoices' and self.strings:
            return self.strings[0]
        if self.range:
            return self.range[0]
        return CmdParam.bash_example[self.type]

    def mk_bash_example(self, simple):
        val = self.mk_example_value()

        if self.type == 'CephBool':
            return '--' + self.name
        if simple:
            if self.type == "CephChoices" and self.strings:
                return val
            elif self.type == "CephString" and self.name != 'who':
                return 'my_' + self.name
            else:
                return CmdParam.bash_example[self.type]
        else:
            return '--{}={}'.format(self.name, val)


class CmdCommand(object):
    def __init__(self, prefix, args, desc,
                 module=None, perm=None, flags=0, poll=None):
        self.prefix = prefix
        self.params = sorted([CmdParam(**arg) for arg in args],
                             key=lambda p: p.req, reverse=True)
        self.help = desc
        self.module = module
        self.perm = perm
        self.flags = Flags(flags)
        self.needs_overload = False

    def is_reasonably_simple(self):
        if len(self.params) > 3:
            return False
        if any(p.n for p in self.params):
            return False
        return True

    def mk_bash_example(self):
        simple = self.is_reasonably_simple()
        line = ' '.join(['ceph', self.prefix] + [p.mk_bash_example(simple) for p in self.params])
        return line


class Sig:
    @staticmethod
    def _parse_arg_desc(desc):
        try:
            return dict(kv.split('=', 1) for kv in desc.split(',') if kv)
        except ValueError:
            return desc

    @staticmethod
    def parse_cmd(cmd):
        parsed = [Sig._parse_arg_desc(s) or s for s in cmd.split()]
        prefix = [s for s in parsed if isinstance(s, str)]
        params = [s for s in parsed if not isinstance(s, str)]
        return ' '.join(prefix), params

    @staticmethod
    def parse_args(args):
        return [Sig._parse_arg_desc(arg) for arg in args]


TEMPLATE = '''
{%- set punct_char = '-' -%}
{# add a header if we have multiple commands in this section #}
{% if commands | length > 1 %}
{{ section }}
{{ section | length * '-' }}
{# and demote the subsection #}
{% set punct_char = '^' %}
{% endif %}
{% for command in commands %}
{{ command.prefix }}
{{ command.prefix | length * punct_char }}

{{ command.help | wordwrap(70) }}

:Example command:
    .. code-block:: bash

       {{ command.mk_bash_example() | wordwrap(70) | indent(9) }}

{%- if command.params %}
:Parameters:{% for param in command.params -%}
{{" -" | indent(12, not loop.first) }} **{% if param.positional %}{{param.name}}{% else %}--{{param.name}}{% endif %}**: {{ param.help() }}
{% endfor %}
{% endif %}
:Ceph Module: {{ command.module }}
:Required Permissions: ``{{ command.perm }}``
{%- if command.flags %}
:Command Flags: ``{{ command.flags }}``
{% endif %}
{% endfor %}
'''


def group_by_prefix(commands):
    last_prefix = None
    grouped = []
    for cmd in commands:
        prefix = cmd.prefix.split(' ', 1)[0]
        if prefix == last_prefix:
            grouped.append(cmd)
        elif last_prefix is None:
            last_prefix = prefix
            grouped = [cmd]
        else:
            yield last_prefix, grouped
            last_prefix = prefix
            grouped = [cmd]
    assert grouped
    yield last_prefix, grouped


def render_commands(commands):
    rendered = io.StringIO()
    for section, grouped in group_by_prefix(commands):
        logger.debug('rendering commands: %s: %d', section, len(grouped))
        rendered.write(Template(TEMPLATE).render(
            section=section,
            commands=grouped))
    return rendered.getvalue().split('\n')


class CephMgrCommands(Directive):
    """
    extracts commands from specified mgr modules
    """
    has_content = True
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = {'python_path': directives.unchanged}

    def _normalize_path(self, dirname):
        my_dir = os.path.dirname(os.path.realpath(__file__))
        src_dir = os.path.abspath(os.path.join(my_dir, '../..'))
        return os.path.join(src_dir, dirname)

    def _is_mgr_module(self, dirname, name):
        if not os.path.isdir(os.path.join(dirname, name)):
            return False
        if not os.path.isfile(os.path.join(dirname, name, '__init__.py')):
            return False
        return name not in ['tests']

    @contextlib.contextmanager
    def mocked_modules(self):
        # src/pybind/mgr/tests
        from tests import mock
        mock_imports = ['rados',
                        'rbd',
                        'cephfs',
                        'dateutil',
                        'dateutil.parser']
        # make dashboard happy
        mock_imports += ['OpenSSL',
                         'jwt',
                         'bcrypt',
                         'jsonpatch',
                         'rook.rook_client',
                         'rook.rook_client.ceph',
                         'rook.rook_client._helper',
                         'cherrypy=3.2.3']
        # make diskprediction_local happy
        mock_imports += ['numpy',
                         'scipy']
        # make restful happy
        mock_imports += ['pecan',
                         'pecan.rest',
                         'pecan.hooks',
                         'werkzeug',
                         'werkzeug.serving']

        for m in mock_imports:
            args = {}
            parts = m.split('=', 1)
            mocked = parts[0]
            if len(parts) > 1:
                args['__version__'] = parts[1]
            sys.modules[mocked] = mock.Mock(**args)

        try:
            yield
        finally:
            for m in mock_imports:
                mocked = m.split('=', 1)[0]
                sys.modules.pop(mocked)

    def _collect_module_commands(self, name):
        with self.mocked_modules():
            logger.info(bold(f"loading mgr module '{name}'..."))
            mgr_mod = __import__(name, globals(), locals(), [], 0)
            reload(mgr_mod)
            from tests import M

            def subclass(x):
                try:
                    return issubclass(x, M)
                except TypeError:
                    return False
            ms = [c for c in mgr_mod.__dict__.values()
                  if subclass(c) and 'Standby' not in c.__name__]
            [m] = ms
            assert isinstance(m.COMMANDS, list)
            return m.COMMANDS

    def _normalize_command(self, command):
        if 'handler' in command:
            del command['handler']
        if 'cmd' in command:
            command['prefix'], command['args'] = Sig.parse_cmd(command['cmd'])
            del command['cmd']
        else:
            command['args'] = Sig.parse_args(command['args'])
        command['flags'] = (1 << 3)
        command['module'] = 'mgr'
        return command

    def _render_cmds(self, commands):
        lines = render_commands(commands)
        assert lines
        lineno = self.lineno - self.state_machine.input_offset - 1
        source = self.state_machine.input_lines.source(lineno)
        self.state_machine.insert_input(lines, source)

    def run(self):
        module_path = self._normalize_path(self.arguments[0])
        sys.path.insert(0, module_path)
        for path in self.options.get('python_path', '').split(':'):
            sys.path.insert(0, self._normalize_path(path))
        os.environ['UNITTEST'] = 'true'
        modules = [name for name in os.listdir(module_path)
                   if self._is_mgr_module(module_path, name)]
        commands = sum([self._collect_module_commands(name) for name in modules], [])
        cmds = [CmdCommand(**self._normalize_command(c)) for c in commands]
        cmds = [cmd for cmd in cmds if 'hidden' not in cmd.flags]
        cmds = sorted(cmds, key=lambda cmd: cmd.prefix)
        self._render_cmds(cmds)

        if 'pybind_rgw_mod' in sys.modules:
            orig_rgw_mod = sys.modules['pybind_rgw_mod']
            sys.modules['rgw'] = orig_rgw_mod

        return []


class MyProcessor(Preprocessor):
    def __init__(self):
        super().__init__()
        self.cmds = []
        self.undef('__DATE__')
        self.undef('__TIME__')
        self.expand_linemacro = False
        self.expand_filemacro = False
        self.expand_countermacro = False
        self.line_directive = '#line'
        self.define("__PCPP_VERSION__ " + '')
        self.define("__PCPP_ALWAYS_FALSE__ 0")
        self.define("__PCPP_ALWAYS_TRUE__ 1")

    def eval(self, src):
        _cmds = []

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
            _cmds.append({
                'cmd': cmd,
                'desc': desc,
                'module': module,
                'perm': perm
            })

        def COMMAND_WITH_FLAG(cmd, desc, module, perm, flag):
            _cmds.append({
                'cmd': cmd,
                'desc': desc,
                'module': module,
                'perm': perm,
                'flags': flag
            })

        self.parse(src)
        out = io.StringIO()
        self.write(out)
        out.seek(0)
        s = out.read()
        exec(s, globals(), locals())
        return _cmds


class CephMonCommands(Directive):
    """
    extracts commands from specified header file
    """
    has_content = True
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = True

    def _src_dir(self):
        my_dir = os.path.dirname(os.path.realpath(__file__))
        return os.path.abspath(os.path.join(my_dir, '../..'))

    def _parse_headers(self, headers):
        src_dir = self._src_dir()
        src = '\n'.join(f'#include "{src_dir}/{header}"' for header in headers)
        return MyProcessor().eval(src)

    def _normalize_command(self, command):
        if 'handler' in command:
            del command['handler']
        command['prefix'], command['args'] = Sig.parse_cmd(command['cmd'])
        del command['cmd']
        return command

    def _render_cmds(self, commands):
        lines = render_commands(commands)
        assert lines
        lineno = self.lineno - self.state_machine.input_offset - 1
        source = self.state_machine.input_lines.source(lineno)
        self.state_machine.insert_input(lines, source)

    def run(self):
        headers = self.arguments[0].split()
        commands = self._parse_headers(headers)
        cmds = [CmdCommand(**self._normalize_command(c)) for c in commands]
        cmds = [cmd for cmd in cmds if 'hidden' not in cmd.flags]
        cmds = sorted(cmds, key=lambda cmd: cmd.prefix)
        self._render_cmds(cmds)
        return []


def setup(app):
    app.add_directive("ceph-mgr-commands", CephMgrCommands)
    app.add_directive("ceph-mon-commands", CephMonCommands)

    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
