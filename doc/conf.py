import datetime
import fileinput
import json
import os
import shutil
import sys

import yaml

# project information
project = 'Ceph'
copyright = ('2016, Ceph authors and contributors. '
             'Licensed under Creative Commons Attribution Share Alike 3.0 '
             '(CC-BY-SA-3.0)')
version = 'dev'
release = 'dev'
pygments_style = 'sphinx'

# HTML output options
html_theme = 'ceph'
html_theme_path = ['_themes']
html_title = "Ceph Documentation"
html_logo = 'logo.png'
html_favicon = 'favicon.ico'
html_show_sphinx = False
html_static_path = ["_static"]
html_sidebars = {
    '**': ['smarttoc.html', 'searchbox.html']
    }

html_css_files = ['css/custom.css']

# general configuration
templates_path = ['_templates']
source_suffix = '.rst'
exclude_patterns = ['**/.#*',
                    '**/*~',
                    'start/quick-common.rst',
                    '**/*.inc.rst']
if tags.has('man'):             # noqa: F821
    master_doc = 'man_index'
    exclude_patterns += ['index.rst',
                         'architecture.rst',
                         'glossary.rst',
                         'release*.rst',
                         'api/*',
                         'cephadm/*',
                         'cephfs/*',
                         'dev/*',
                         'governance.rst',
                         'foundation.rst',
                         'install/*',
                         'mon/*',
                         'rados/*',
                         'mgr/*',
                         'ceph-volume/*',
                         'radosgw/*',
                         'rbd/*',
                         'start/*',
                         'releases/*']
else:
    master_doc = 'index'
    exclude_patterns += ['man_index.rst']

build_with_rtd = os.environ.get('READTHEDOCS') == 'True'

if build_with_rtd:
    exclude_patterns += ['**/api/*',
                         '**/api.rst']

sys.path.insert(0, os.path.abspath('_ext'))

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.graphviz',
    'sphinx.ext.mathjax',
    'sphinx.ext.todo',
    'sphinx-prompt',
    'sphinx_autodoc_typehints',
    'sphinx_substitution_extensions',
    'breathe',
    'edit_on_github',
    'ceph_releases',
    'sphinxcontrib.openapi'
    ]

ditaa = shutil.which("ditaa")
if ditaa is not None:
    extensions += ['sphinxcontrib.ditaa']
else:
    extensions += ['plantweb.directive']
    plantweb_defaults = {
        'engine': 'ditaa'
    }

if build_with_rtd:
    extensions += ['sphinx_search.extension']

# sphinx.ext.todo options
todo_include_todos = True

# sphinx_substitution_extensions options
# TODO: read from doc/releases/releases.yml
rst_prolog = """
.. |stable-release| replace:: octopus
"""

top_level = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

# breath options
breathe_default_project = "Ceph"
# see $(top_srcdir)/Doxyfile

breathe_build_directory = os.path.join(top_level, "build-doc")
breathe_projects = {"Ceph": os.path.join(top_level, breathe_build_directory)}
breathe_projects_source = {
    "Ceph": (os.path.join(top_level, "src/include/rados"),
             ["rados_types.h", "librados.h"])
}
breathe_domain_by_extension = {'py': 'py',
                               'c': 'c', 'h': 'c',
                               'cc': 'cxx', 'hpp': 'cxx'}
breathe_doxygen_config_options = {
    'EXPAND_ONLY_PREDEF': 'YES',
    'MACRO_EXPANSION': 'YES',
    'PREDEFINED': 'CEPH_RADOS_API= '
}

# edit_on_github options
# the docs are rendered with github links pointing to master. the javascript
# snippet in _static/ceph.js rewrites the edit links when a page is loaded, to
# point to the correct branch.
edit_on_github_project = 'ceph/ceph'
edit_on_github_branch = 'master'

# graphviz options
graphviz_output_format = 'svg'

def generate_state_diagram(input_paths, output_path):
    sys.path.append(os.path.join(top_level, 'doc', 'scripts'))
    from gen_state_diagram import do_filter, StateMachineRenderer
    inputs = [os.path.join(top_level, fn) for fn in input_paths]
    output = os.path.join(top_level, output_path)

    def process(app):
        with fileinput.input(files=inputs) as f:
            input = do_filter(f)
            render = StateMachineRenderer()
            render.read_input(input)
            with open(output, 'w') as dot_output:
                render.emit_dot(dot_output)

    return process


# mocking ceph_module offered by ceph-mgr. `ceph_module` is required by
# mgr.mgr_module
class Dummy(object):
    def __getattr__(self, _):
        return lambda *args, **kwargs: None


class Mock(object):
    __all__ = []

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return Mock()

    @classmethod
    def __getattr__(cls, name):
        mock = type(name, (Dummy,), {})
        mock.__module__ = __name__
        return mock


# autodoc options
sys.modules['ceph_module'] = Mock()

if build_with_rtd:
    autodoc_mock_imports = ['cephfs',
                            'rados',
                            'rbd',
                            'ceph']
    pybinds = ['pybind/mgr',
               'python-common']
else:
    pybinds = ['pybind',
               'pybind/mgr',
               'python-common']

for c in pybinds:
    pybind = os.path.join(top_level, 'src', c)
    if pybind not in sys.path:
        sys.path.insert(0, pybind)


# build the releases.json. this reads in the yaml version and dumps
# out the json representation of the same file. the resulting releases.json
# should be served from '/_static/releases.json' of hosted site.
def yaml_to_json(input_path, output_path):
    def json_serialize(obj):
        if isinstance(obj, datetime.date):
            return obj.isoformat()

    yaml_fn = os.path.join(top_level, input_path)
    json_fn = os.path.join(top_level, output_path)

    def process(app):
        with open(yaml_fn) as input:
            with open(json_fn, 'w') as output:
                releases = yaml.safe_load(input)
                s = json.dumps(releases, indent=2, default=json_serialize)
                output.write(s)

    return process


# handles edit-on-github and old version warning display
def setup(app):
    app.add_js_file('js/ceph.js')
    if ditaa is None:
        # add "ditaa" as an alias of "diagram"
        from plantweb.directive import DiagramDirective
        app.add_directive('ditaa', DiagramDirective)
    app.connect('builder-inited',
                generate_state_diagram(['src/osd/PeeringState.h',
                                        'src/osd/PeeringState.cc'],
                                       'doc/dev/peering_graph.generated.dot'))
    app.connect('builder-inited',
                yaml_to_json('doc/releases/releases.yml',
                             'doc/_static/releases.json'))
