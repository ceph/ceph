import fileinput
import glob
import logging
import os
import shutil
import sys
import yaml
import sphinx.util


top_level = \
    os.path.dirname(
        os.path.dirname(
            os.path.abspath(__file__)))

# it could be that ceph was built without RGW support
# e.g. in a local development environment
try:
    pybind_rgw_mod = __import__('rgw', globals(), locals(), [], 0)
    sys.modules['pybind_rgw_mod'] = pybind_rgw_mod
except Exception:
    pass

def parse_ceph_release():
    with open(os.path.join(top_level, 'src/ceph_release')) as f:
        lines = f.readlines()
        assert(len(lines) == 3)
        # 16, pacific, dev
        version, codename, status = [line.strip() for line in lines]
        return version, codename, status


def latest_stable_release():
    with open(os.path.join(top_level, 'doc/releases/releases.yml')) as input:
        releases = yaml.safe_load(input)['releases']
        # get the first release
        return next(iter(releases.keys()))


def is_release_eol(codename):
    with open(os.path.join(top_level, 'doc/releases/releases.yml')) as input:
        releases = yaml.safe_load(input)['releases']
        return 'actual_eol' in releases.get(codename, {})


# project information
project = 'Ceph'
copyright = ('2016, Ceph authors and contributors. '
             'Licensed under Creative Commons Attribution Share Alike 3.0 '
             '(CC-BY-SA-3.0)')
version, codename, release = parse_ceph_release()
pygments_style = 'sphinx'

# HTML output options
html_theme = 'ceph'
html_theme_options = {
    'logo_only': True,
    'display_version': False,
    'prev_next_buttons_location': 'bottom',
    'style_external_links': False,
    'vcs_pageview_mode': 'edit',
    'style_nav_header_background': '#eee',
    # Toc options
    'collapse_navigation': True,
    'sticky_navigation': True,
    'navigation_depth': 4,
    'includehidden': True,
    'titles_only': False
}
html_theme_path = ['_themes']
html_title = "Ceph Documentation"
html_logo = 'logo.png'
html_context = {'is_release_eol': is_release_eol(codename)}
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

sys.path.insert(0, os.path.abspath('_ext'))

smartquotes_action = "qe"

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.graphviz',
    'sphinx.ext.mathjax',
    'sphinx.ext.todo',
    'sphinx-prompt',
    'sphinx_autodoc_typehints',
    'sphinx_substitution_extensions',
    'breathe',
    'ceph_commands',
    'ceph_releases',
    'ceph_confval',
    'sphinxcontrib.mermaid',
    'sphinxcontrib.openapi',
    'sphinxcontrib.seqdiag',
    ]

ditaa = shutil.which("ditaa")
if ditaa is not None:
    # in case we don't have binfmt_misc enabled or jar is not registered
    ditaa_args = ['-jar', ditaa]
    ditaa = 'java'
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
rst_prolog = f"""
.. |stable-release| replace:: {latest_stable_release()}
"""

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
    'PREDEFINED': 'CEPH_RADOS_API= ',
    'WARN_IF_UNDOCUMENTED': 'NO',
}

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
    autodoc_mock_imports = ['ceph']
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

# openapi
openapi_logger = sphinx.util.logging.getLogger('sphinxcontrib.openapi.openapi30')
openapi_logger.setLevel(logging.WARNING)

# seqdiag
seqdiag_antialias = True
seqdiag_html_image_format = 'SVG'

# ceph_confval
ceph_confval_imports = glob.glob(os.path.join(top_level,
                                              'src/common/options',
                                              '*.yaml.in'))
ceph_confval_mgr_module_path = 'src/pybind/mgr'
ceph_confval_mgr_python_path = 'src/pybind'

# handles edit-on-github and old version warning display
def setup(app):
    if ditaa is None:
        # add "ditaa" as an alias of "diagram"
        from plantweb.directive import DiagramDirective
        app.add_directive('ditaa', DiagramDirective)
    app.connect('builder-inited',
                generate_state_diagram(['src/osd/PeeringState.h',
                                        'src/osd/PeeringState.cc'],
                                       'doc/dev/peering_graph.generated.dot'))
