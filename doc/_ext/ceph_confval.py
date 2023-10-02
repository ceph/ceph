import io
import contextlib
import os
import sys
from typing import Any, Dict, List, Union

from docutils.nodes import Node
from docutils.parsers.rst import directives
from docutils.statemachine import StringList

from sphinx import addnodes
from sphinx.directives import ObjectDescription
from sphinx.domains.python import PyField
from sphinx.environment import BuildEnvironment
from sphinx.locale import _
from sphinx.util import logging, status_iterator, ws_re
from sphinx.util.docutils import switch_source_input, SphinxDirective
from sphinx.util.docfields import Field
from sphinx.util.nodes import make_id
import jinja2
import jinja2.filters
import yaml

logger = logging.getLogger(__name__)


TEMPLATE = '''
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
   :default: {{ default | literal }}
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
{%- if opt.constraint %}
   :constraint: {{ opt.constraint }}
{% endif %}
{%- if opt.policies %}
   :policies: {{ opt.policies }}
{% endif %}
{%- if opt.example %}
   :example: {{ opt.example }}
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


class CephModule(SphinxDirective):
    """
    Directive to name the mgr module for which options are documented.
    """
    has_content = False
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False

    def run(self) -> List[Node]:
        module = self.arguments[0].strip()
        if module == 'None':
            self.env.ref_context.pop('ceph:module', None)
        else:
            self.env.ref_context['ceph:module'] = module
        return []


class CephOption(ObjectDescription):
    """
    emit option loaded from given command/options/<name>.yaml.in file
    """
    has_content = True
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = {
        'module': directives.unchanged,
        'default': directives.unchanged
    }


    doc_field_types = [
        Field('default',
              label=_('Default'),
              has_arg=False,
              names=('default',)),
        Field('type',
              label=_('Type'),
              has_arg=False,
              names=('type',),
              bodyrolename='class'),
    ]

    template = jinja_template()
    opts: Dict[str, Dict[str, FieldValueT]] = {}
    mgr_opts: Dict[str,            # module name
                   Dict[str,       # option name
                        Dict[str,  # field_name
                             FieldValueT]]] = {}

    def _load_yaml(self) -> Dict[str, Dict[str, FieldValueT]]:
        if CephOption.opts:
            return CephOption.opts
        opts = []
        for fn in status_iterator(self.config.ceph_confval_imports,
                                  'loading options...', 'red',
                                  len(self.config.ceph_confval_imports),
                                  self.env.app.verbosity):
            self.env.note_dependency(fn)
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
        python_path = self.config.ceph_confval_mgr_python_path
        for path in python_path.split(':'):
            sys.path.insert(0, self._normalize_path(path))
        module_path = self.env.config.ceph_confval_mgr_module_path
        module_path = self._normalize_path(module_path)
        sys.path.insert(0, module_path)
        if not self._is_mgr_module(module_path, module):
            raise self.error(f'module "{module}" not found under {module_path}')
        fn = os.path.join(module_path, module, 'module.py')
        if os.path.exists(fn):
            self.env.note_dependency(fn)
        os.environ['UNITTEST'] = 'true'
        opts = self._collect_options_from_module(module)
        CephOption.mgr_opts[module] = dict((opt['name'], opt) for opt in opts)
        return CephOption.mgr_opts[module]

    def _current_module(self) -> str:
        return self.options.get('module',
                                self.env.ref_context.get('ceph:module'))

    def _render_option(self, name) -> str:
        cur_module = self._current_module()
        if cur_module:
            try:
                opt = self._load_module(cur_module).get(name)
            except Exception as e:
                message = f'Unable to load module "{cur_module}": {e}'
                raise self.error(message)
        else:
            opt = self._load_yaml().get(name)
        if opt is None:
            raise self.error(f'Option "{name}" not found!')
        if cur_module and 'type' not in opt:
            # the type of module option defaults to 'str'
            opt['type'] = 'str'
        desc = opt.get('fmt_desc') or opt.get('long_desc') or opt.get('desc')
        opt_default = opt.get('default')
        default = self.options.get('default', opt_default)
        try:
            return self.template.render(opt=opt,
                                        desc=desc,
                                        default=default)
        except Exception as e:
            message = (f'Unable to render option "{name}": {e}. ',
                       f'opt={opt}, desc={desc}, default={default}')
            raise self.error(message)

    def handle_signature(self,
                         sig: str,
                         signode: addnodes.desc_signature) -> str:
        signode.clear()
        signode += addnodes.desc_name(sig, sig)
        # normalize whitespace like XRefRole does
        name = ws_re.sub(' ', sig)
        cur_module = self._current_module()
        if cur_module:
            return '/'.join(['mgr', cur_module, name])
        else:
            return name

    def transform_content(self, contentnode: addnodes.desc_content) -> None:
        name = self.arguments[0]
        source, lineno = self.get_source_info()
        source = f'{source}:{lineno}:<confval>'
        fields = StringList(self._render_option(name).splitlines() + [''],
                            source=source, parent_offset=lineno)
        with switch_source_input(self.state, fields):
            self.state.nested_parse(fields, 0, contentnode)

    def add_target_and_index(self,
                             name: str,
                             sig: str,
                             signode: addnodes.desc_signature) -> None:
        node_id = make_id(self.env, self.state.document, self.objtype, name)
        signode['ids'].append(node_id)
        self.state.document.note_explicit_target(signode)
        entry = f'{name}; configuration option'
        self.indexnode['entries'].append(('pair', entry, node_id, '', None))
        std = self.env.get_domain('std')
        std.note_object(self.objtype, name, node_id, location=signode)


def _reset_ref_context(app, env, docname):
    env.ref_context.pop('ceph:module', None)


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
    app.add_object_type(
        'confval',
        'confval',
        objname='configuration option',
    )
    app.add_directive_to_domain('std', 'mgr_module', CephModule)
    app.add_directive_to_domain('std', 'confval', CephOption, override=True)
    app.connect('env-purge-doc', _reset_ref_context)

    return {
        'version': 'builtin',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
