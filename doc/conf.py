import sys
import os

project = u'Ceph'
copyright = u'2010-2014, Inktank Storage, Inc. and contributors. Licensed under Creative Commons BY-SA'
version = 'dev'
release = 'dev'

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
exclude_patterns = ['**/.#*', '**/*~', 'start/quick-common.rst']
pygments_style = 'sphinx'

html_theme = 'ceph'
html_theme_path = ['_themes']
html_title = "Ceph Documentation"
html_logo = 'logo.png'
html_favicon = 'favicon.ico'
html_use_smartypants = True
html_show_sphinx = False
html_sidebars = {
    '**': ['smarttoc.html', 'searchbox.html'],
    }

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.graphviz',
    'sphinx.ext.todo',
    'sphinx_ditaa',
    'breathe',
    ]
todo_include_todos = True

def _get_manpages():
    import os
    man_dir = os.path.join(
        os.path.dirname(__file__),
        'man',
        )
    sections = os.listdir(man_dir)
    for section in sections:
        section_dir = os.path.join(man_dir, section)
        if not os.path.isdir(section_dir):
            continue
        for filename in os.listdir(section_dir):
            base, ext = os.path.splitext(filename)
            if ext != '.rst':
                continue
            if base == 'index':
                continue
            with file(os.path.join(section_dir, filename)) as f:
                one = f.readline()
                two = f.readline()
                three = f.readline()
                assert one == three
                assert all(c=='=' for c in one.rstrip('\n'))
                two = two.strip()
                name, rest = two.split('--', 1)
                assert name.strip() == base
                description = rest.strip()
            yield (
                os.path.join('man', section, base),
                base,
                description,
                '',
                section,
                )

man_pages = list(_get_manpages())

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
pybind = os.path.join(top_level, 'src/pybind')
if pybind not in sys.path:
    sys.path.insert(0, pybind)
