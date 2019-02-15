import sys
import os

project = u'Ceph'
copyright = u'2016, Red Hat, Inc, and contributors. Licensed under Creative Commons Attribution Share Alike 3.0 (CC-BY-SA-3.0)'
version = 'dev'
release = 'dev'

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
exclude_patterns = ['**/.#*', '**/*~', 'start/quick-common.rst', '**/*.inc.rst']
if tags.has('man'):
    master_doc = 'man_index'
    exclude_patterns += ['index.rst', 'architecture.rst', 'glossary.rst', 'release*.rst',
                         'api/*',
                         'cephfs/*',
                         'dev/*',
                         'governance.rst',
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
    exclude_patterns += ['man_index.rst']

pygments_style = 'sphinx'

html_theme = 'ceph'
html_theme_path = ['_themes']
html_title = "Ceph Documentation"
html_logo = 'logo.png'
html_favicon = 'favicon.ico'
html_show_sphinx = False
html_static_path = ["_static"]
html_sidebars = {
    '**': ['smarttoc.html', 'searchbox.html'],
    }

sys.path.insert(0, os.path.abspath('_ext'))

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.graphviz',
    'sphinx.ext.todo',
    'sphinxcontrib.ditaa',
    'breathe',
    'edit_on_github',
    'ceph_releases',
    ]
ditaa = 'ditaa'
todo_include_todos = True

top_level = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

breathe_default_project = "Ceph"
# see $(top_srcdir)/Doxyfile

breathe_build_directory = os.path.join(top_level, "build-doc")
breathe_projects = {"Ceph": os.path.join(top_level, breathe_build_directory)}
breathe_projects_source = {
    "Ceph": (os.path.join(top_level, "src/include/rados"),
             ["rados_types.h", "librados.h"])
}
breathe_domain_by_extension = {'py': 'py', 'c': 'c', 'h': 'c', 'cc': 'cxx', 'hpp': 'cxx'}

# the docs are rendered with github links pointing to master. the javascript
# snippet in _static/ceph.js rewrites the edit links when a page is loaded, to
# point to the correct branch.
edit_on_github_project = 'ceph/ceph'
edit_on_github_branch = 'master'

# handles edit-on-github and old version warning display
def setup(app):
    app.add_javascript('js/ceph.js')

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

sys.modules['ceph_module'] = Mock()

for pybind in [os.path.join(top_level, 'src/pybind'),
               os.path.join(top_level, 'src/pybind/mgr')]:
    if pybind not in sys.path:
        sys.path.insert(0, pybind)
