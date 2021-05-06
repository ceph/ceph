import io
import contextlib
import os
import sys
from typing import Any, Dict, List, Union

from docutils.nodes import Node
from docutils.parsers.rst import directives
from docutils.parsers.rst import Directive
from sphinx import addnodes
from sphinx.domains.python import PyField
from sphinx.environment import BuildEnvironment
from sphinx.locale import _
from sphinx.util import logging, status_iterator, ws_re
from sphinx.util.docfields import Field

import jinja2
import jinja2.filters
import yaml

logger = logging.getLogger(__name__)


TEMPLATE = '''
.. confval_option:: {{ opt.name }}
{% if desc %}
   {{ desc | wordwrap(70) | indent(3) }}
{% endif %}
   :type: ``{{opt.type}}``
{%- if default is not none %}
  {%- if opt.type == 'size' %}
   :default: ``{{ default | eval_size | iec_size }}``
  {%- elif opt.type == 'secs' %}
   :default: ``{{ default | readable_duration(opt.type) }}``
  {%- elif opt.type in ('uint', 'int', 'float') %}
   :default: ``{{ default | readable_num(opt.type) }}``
  {%- elif opt.type == 'millisecs' %}
   :default: ``{{ default }}`` milliseconds
  {%- elif opt.type == 'bool' %}
   :default: ``{{ default | string | lower }}``
  {%- else %}
   :default: ``{{ default }}``
  {%- endif -%}
{%- endif %}
{%- if opt.enum_values %}
   :valid choices:{% for enum_value in opt.enum_values -%}
{{" -" | indent(18, not loop.first) }} {{ enum_value | literal }}
{% endfor %}
{%- endif %}
{%- if opt.min is defined and opt.max is defined %}
   :allowed range: ``[{{ opt.min }}, {{ opt.max }}]``
{%- elif opt.min is defined %}
   :min: ``{{ opt.min }}``
{%- elif opt.max is defined %}
   :max: ``{{ opt.max }}``
{%- endif %}
{%- if opt.see_also %}
   :see also: {{ opt.see_also | map('ref_confval') | join(', ') }}
{%- endif %}
{% if opt.note %}
   .. note::
      {{ opt.note }}
{%- endif -%}
{%- if opt.warning %}
   .. warning::
      {{ opt.warning }}
{%- endif %}
'''


def eval_size(value) -> int:
    try:
        return int(value)
    except ValueError:
        times = dict(_K=1 << 10,
                     _M=1 << 20,
                     _G=1 << 30,
                     _T=1 << 40)
        for unit, m in times.items():
            if value.endswith(unit):
                return int(value[:-len(unit)]) * m
        raise ValueError(f'unknown value: {value}')


def readable_duration(value: str, typ: str) -> str:
    try:
        if typ == 'sec':
            v = int(value)
            postfix = 'second' if v == 1 else 'seconds'
            return f'{v} {postfix}'
        elif typ == 'float':
            return str(float(value))
        else:
            return str(int(value))
    except ValueError:
        times = dict(_min=['minute', 'minutes'],
                     _hr=['hour', 'hours'],
                     _day=['day', 'days'])
        for unit, readables in times.items():
            if value.endswith(unit):
                v = int(value[:-len(unit)])
                postfix = readables[0 if v == 1 else 1]
                return f'{v} {postfix}'
        raise ValueError(f'unknown value: {value}')


def do_plain_num(value: str, typ: str) -> str:
    if typ == 'float':
        return str(float(value))
    else:
        return str(int(value))


def iec_size(value: int) -> str:
    if value == 0:
        return '0B'
    units = dict(Ei=60,
                 Pi=50,
                 Ti=40,
                 Gi=30,
                 Mi=20,
                 Ki=10,
                 B=0)
    for unit, bits in units.items():
        m = 1 << bits
        if value % m == 0:
            value //= m
            return f'{value}{unit}'
    raise Exception(f'iec_size() failed to convert {value}')


def do_fileize_num(value: str, typ: str) -> str:
    v = eval_size(value)
    return iec_size(v)


def readable_num(value: str, typ: str) -> str:
    e = ValueError()
    for eval_func in [do_plain_num,
                      readable_duration,
                      do_fileize_num]:
        try:
            return eval_func(value, typ)
        except ValueError as ex:
            e = ex
    raise e


def literal(name) -> str:
    if name:
        return f'``{name}``'
    else:
        return f'<empty string>'


def ref_confval(name) -> str:
    return f':confval:`{name}`'


def jinja_template() -> jinja2.Template:
    env = jinja2.Environment()
    env.filters['eval_size'] = eval_size
    env.filters['iec_size'] = iec_size
    env.filters['readable_duration'] = readable_duration
    env.filters['readable_num'] = readable_num
    env.filters['literal'] = literal
    env.filters['ref_confval'] = ref_confval
    return env.from_string(TEMPLATE)


FieldValueT = Union[bool, float, int, str]


class CephModule(Directive):
    """
    Directive to name the mgr module for which options are documented.
    """
    has_content = False
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False

    def run(self) -> List[Node]:
        module = self.arguments[0].strip()
        env = self.state.document.settings.env
        if module == 'None':
            env.ref_context.pop('ceph:module', None)
        else:
            env.ref_context['ceph:module'] = module
        return []


class CephOption(Directive):
    """
    emit option loaded from given command/options/<name>.yaml.in file
    """
    has_content = True
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = {'default': directives.unchanged}

    template = jinja_template()
    opts: Dict[str, Dict[str, FieldValueT]] = {}
    mgr_opts: Dict[str,            # module name
                   Dict[str,       # option name
                        Dict[str,  # field_name
                             FieldValueT]]] = {}

    def _load_yaml(self) -> Dict[str, Dict[str, FieldValueT]]:
        if CephOption.opts:
            return CephOption.opts
        env = self.state.document.settings.env
        opts = []
        for fn in status_iterator(env.config.ceph_confval_imports,
                                  'loading options...', 'red',
                                  len(env.config.ceph_confval_imports),
                                  env.app.verbosity):
            env.note_dependency(fn)
            try:
                with open(fn, 'r') as f:
                    yaml_in = io.StringIO()
                    for line in f:
                        if '@' not in line:
                            yaml_in.write(line)
                    yaml_in.seek(0)
                    opts += yaml.safe_load(yaml_in)['options']
            except OSError as e:
                message = f'Unable to open option file "{fn}": {e}'
                raise self.error(message)
        CephOption.opts = dict((opt['name'], opt) for opt in opts)
        return CephOption.opts

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

    def _collect_options_from_module(self, name):
        with self.mocked_modules():
            mgr_mod = __import__(name, globals(), locals(), [], 0)
            # import 'M' from src/pybind/mgr/tests
            from tests import M

            def subclass(x):
                try:
                    return issubclass(x, M)
                except TypeError:
                    return False
            ms = [c for c in mgr_mod.__dict__.values()
                  if subclass(c) and 'Standby' not in c.__name__]
            [m] = ms
            assert isinstance(m.MODULE_OPTIONS, list)
            return m.MODULE_OPTIONS

    def _load_module(self, module) -> Dict[str, Dict[str, FieldValueT]]:
        mgr_opts = CephOption.mgr_opts.get(module)
        if mgr_opts is not None:
            return mgr_opts
        env = self.state.document.settings.env
        python_path = env.config.ceph_confval_mgr_python_path
        for path in python_path.split(':'):
            sys.path.insert(0, self._normalize_path(path))
        module_path = env.config.ceph_confval_mgr_module_path
        module_path = self._normalize_path(module_path)
        sys.path.insert(0, module_path)
        os.environ['UNITTEST'] = 'true'

        modules = [name for name in os.listdir(module_path)
                   if self._is_mgr_module(module_path, name)]
        opts = []
        for module in status_iterator(modules,
                                      'loading module...', 'darkgreen',
                                      len(modules),
                                      env.app.verbosity):
            fn = os.path.join(module_path, module, 'module.py')
            if os.path.exists(fn):
                env.note_dependency(fn)
            opts += self._collect_options_from_module(module)
        CephOption.mgr_opts[module] = dict((opt['name'], opt) for opt in opts)
        return CephOption.mgr_opts[module]

    def run(self) -> List[Any]:
        name = self.arguments[0]
        env = self.state.document.settings.env
        cur_module = env.ref_context.get('ceph:module')
        if cur_module:
            opt = self._load_module(cur_module).get(name)
        else:
            opt = self._load_yaml().get(name)
        if opt is None:
            raise self.error(f'Option "{name}" not found!')
        desc = opt.get('fmt_desc') or opt.get('long_desc') or opt.get('desc')
        opt_default = opt.get('default')
        default = self.options.get('default', opt_default)
        try:
            rendered = self.template.render(opt=opt,
                                            desc=desc,
                                            default=default)
        except Exception as e:
            message = (f'Unable to render option "{name}": {e}. ',
                       f'opt={opt}, desc={desc}, default={default}')
            raise self.error(message)

        lineno = self.lineno - self.state_machine.input_offset - 1
        source = self.state_machine.input_lines.source(lineno)
        self.state_machine.insert_input(rendered.split('\n'), source)
        return []


def setup(app) -> Dict[str, Any]:
    app.add_config_value('ceph_confval_imports',
                         default=[],
                         rebuild='html',
                         types=[str])
    app.add_config_value('ceph_confval_mgr_module_path',
                         default=[],
                         rebuild='html',
                         types=[str])
    app.add_config_value('ceph_confval_mgr_python_path',
                         default=[],
                         rebuild='',
                         types=[str])
    app.add_directive('confval', CephOption)
    app.add_directive('mgr_module', CephModule)
    app.add_object_type(
        'confval_option',
        'confval',
        objname='configuration value',
        indextemplate='pair: %s; configuration value',
        parse_node=_parse_option_desc,
        doc_field_types=[
            PyField(
                'type',
                label=_('Type'),
                has_arg=False,
                names=('type',),
                bodyrolename='class'
            ),
            Field(
                'default',
                label=_('Default'),
                has_arg=False,
                names=('default',),
            ),
            Field(
                'required',
                label=_('Required'),
                has_arg=False,
                names=('required',),
            ),
            Field(
                'example',
                label=_('Example'),
                has_arg=False,
            )
        ]
    )
    app.add_object_type(
        'confsec',
        'confsec',
        objname='configuration section',
        indextemplate='pair: %s; configuration section',
        doc_field_types=[
            Field(
                'example',
                label=_('Example'),
                has_arg=False,
            )]
    )

    return {
        'version': 'builtin',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
