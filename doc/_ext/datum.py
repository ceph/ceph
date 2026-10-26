"""Publish Datum's single authored guide and design through Ceph's existing docs build."""

from functools import lru_cache
from pathlib import Path
import os
import re
import zipfile

from docutils import nodes
from sphinx import addnodes

import m2r2

COURSE = Path(__file__).resolve().parents[2] / 'src/rgw/datum/tutorials'
PREFIX = 'dev/radosgw/datum/'
FENCES = re.compile(r'(^```[^\n]*\n.*?^```[ \t]*$)', re.M | re.S)
PARTS = [
    ('Bytes, keys, and ranges', 1, 6),
    ('Keeping and observing data', 7, 12),
    ('Work and continuation', 13, 15),
    ('RGW integration preview', 16, 17)
]


def slug(text):
    return re.sub(r'[^\w\s-]', '', text.lower()).replace(' ', '-')


@lru_cache(maxsize=1)
def guide():
    text = (COURSE / 'tutorial.md').read_text(encoding='utf-8')
    sections = list(re.finditer(r'^## (\d+½?):? (.+)\n', text, re.M))
    stems = sorted(p.stem for p in (COURSE / 'examples').glob('*.cpp')
                   if not p.stem.endswith('_variation'))
    stems.sort(key=lambda stem: (int(stem[:2]), 'half' in stem))
    chapters = {}
    links = {}
    for index, (section, stem) in enumerate(zip(sections, stems, strict=True)):
        end = sections[index + 1].start() if index + 1 < len(sections) else text.index('## Appendix A')
        title = section.group(0).removeprefix('## ').strip()
        name = stem.replace('_', '-')
        chapters[name] = (section[1], title, text[section.start():end])
        links[slug(title)] = (name, '')
    appendix = text.split('## Appendix A Building and running\n', 1)[1].split('## Bibliography\n', 1)[0]
    links['appendix-a-building-and-running'] = ('build-and-run', '')
    links['bibliography'] = ('references', '')
    for title in re.findall(r'^### (.+)$', FENCES.sub('', appendix), re.M):
        links[slug(title)] = ('build-and-run', nodes.make_id(title))
    return text, sections, chapters, links


def promote(text):
    return ''.join(part if index % 2 else re.sub(r'^##', '#', part, flags=re.M)
                   for index, part in enumerate(FENCES.split(text)))


def archive(app):
    target = Path(app.doctreedir) / 'datum-tutorials.zip'
    with zipfile.ZipFile(target, 'w', compression=zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(COURSE.rglob('*')):
            relative = path.relative_to(COURSE)
            if path.is_file() and not set(relative.parts) & {'build', '__pycache__', '.git'}:
                bundle.write(path, 'datum-tutorials/' + relative.as_posix())
    return target


def source_read(app, docname, source):
    if not docname.startswith(PREFIX):
        return
    navigation = 'Provisional documentation · ' + ' · '.join(f':doc:`{label} <{name}>`' for label, name in [
        ('Datum', 'index'), ('Tutorial', 'tutorial'), ('Sales pitch', 'in-practice'), ('Design', 'design'),
        ('Build and run', 'build-and-run'), ('Examples', 'examples'), ('Index', 'lookup')])
    if not source[0].startswith('.. datum-source:: '):
        if docname == PREFIX + 'in-practice':
            source[0] = navigation + '\n\n' + source[0]
        return
    key = source[0].strip().split(':: ', 1)[1]
    text, sections, chapters, _ = guide()
    app.env.note_dependency(str(COURSE / 'tutorial.md'))
    if key in chapters:
        content = promote(chapters[key][2])
    elif key == 'design':
        path = COURSE.parent / 'README.md'
        app.env.note_dependency(str(path))
        content = '# Design and reference\n\n' + path.read_text(encoding='utf-8').split('\n', 1)[1]
    elif key == 'lookup':
        path = COURSE / 'docs/lookup.md'
        app.env.note_dependency(str(path))
        content = path.read_text(encoding='utf-8')
    elif key == 'build-and-run':
        appendix = text.split('## Appendix A Building and running\n', 1)[1]
        content = '# Build and run\n' + promote(appendix.split('## Bibliography\n', 1)[0])
    elif key == 'references':
        content = '# References\n' + promote(text.split('## Bibliography\n', 1)[1])
    elif key == 'examples':
        archive(app)
        content = ('# Complete examples\n\n[Download the tutorial sources](datum-tutorials.zip), '
                   'then follow [Build and run](build-and-run.rst).\n\n')
        content += '| Lesson | Code | Expected output |\n| --- | --- | --- |\n'
        for name, (_, title, _) in chapters.items():
            stem = name.replace('-', '_')
            expected = '01_hello_fragments' if stem.startswith('01_') else stem
            variation = expected if stem.startswith('01_') else stem + '_variation'
            content += (f'| [{title}]({name}.rst) | [Program](examples/{stem}.cpp) · '
                        f'[Variation](examples/{stem}_variation.cpp) | [Output](expected/{expected}.txt) · '
                        f'[Variation output](expected/{variation}.txt) |\n')
    elif key == 'tutorial':
        content = text[:sections[0].start()].replace('# Datum tutorials', '# Tutorial', 1)
    else:
        raise ValueError(f'Unknown Datum document: {key}')
    source[0] = navigation + '\n\n' + m2r2.convert(content, anonymous_references=True)
    if key == 'tutorial':
        for title, first, last in PARTS:
            entries = [f'   {name}' for name, (number, _, _) in chapters.items()
                       if first <= int(re.match(r'\d+', number)[0]) <= last]
            source[0] += (f'\n{title}\n' + '-' * len(title)
                          + '\n\n.. toctree::\n   :maxdepth: 1\n\n' + '\n'.join(entries) + '\n')
        source[0] += '\n:doc:`References <references>`\n'


def doctree_read(app, tree):
    docname = app.env.docname
    if not docname.startswith(PREFIX):
        return
    text, _, _, links = guide()
    if docname == PREFIX + 'references':
        # Keep citation numbers when m2r2 converts ordered lists to automatic numbering.
        bibliography = text.split('## Bibliography\n', 1)[1]
        starts = re.findall(r'(?:\A|\n\n)(\d+)\. ', bibliography)
        for ordered, start in zip(tree.findall(nodes.enumerated_list), starts, strict=True):
            ordered['start'] = int(start)
    docdir = Path(app.env.doc2path(docname)).parent
    for image in tree.findall(nodes.image):
        uri = image.get('uri', '')
        if uri.startswith('figures/'):
            image['uri'] = os.path.relpath(COURSE / uri, docdir)
            image['width'] = '680px'
    for reference in list(tree.findall(nodes.reference)):
        uri = reference.get('refuri', '')
        path, separator, anchor = uri.partition('#')
        if path.startswith('figures/') and len(reference.children) == 1 and isinstance(
                reference.children[0], nodes.image):
            # Sphinx supplies the full-size link for a scaled image.
            reference.replace_self(reference.children[0])
            continue
        target = None
        if path in {'tutorial.md', '../tutorial.md', 'tutorials/tutorial.md'} or (
                not path and anchor in links and docname != PREFIX + 'design'):
            target = links.get(anchor, ('tutorial', ''))
        elif path == '../../../doc/dev/radosgw/datum/index.rst':
            target = ('index', '')
        elif path == 'docs/lookup.md':
            target = ('lookup', '')
        elif re.fullmatch(r'https://github.com/chardan/ceph/blob/[0-9a-f]{40}/src/rgw/datum/README.md', path):
            target = ('design', anchor)
        elif path.endswith('.rst') and '/' not in path:
            target = (path.removesuffix('.rst'), anchor)
        if target:
            reference['refuri'] = app.builder.get_relative_uri(docname, PREFIX + target[0])
            if target[1]:
                reference['refuri'] += '#' + target[1]
            continue
        if path.startswith(('examples/', 'expected/', 'figures/', 'cmake/')) or path == 'datum-tutorials.zip':
            actual = Path(app.doctreedir) / path if path == 'datum-tutorials.zip' else COURSE / path
            app.env.note_dependency(str(actual))
            download = addnodes.download_reference('', '', reftarget=os.path.relpath(actual, docdir))
            download.extend(reference.children)
            reference.replace_self(download)


def html_page_context(app, pagename, templatename, context, doctree):
    if pagename.startswith(PREFIX):
        app.add_css_file('datum.css')
    if pagename == PREFIX + 'in-practice':
        app.add_css_file('datum-bindings.css')
        app.add_js_file('datum-bindings.js')


def setup(app):
    app.connect('source-read', source_read)
    app.connect('html-page-context', html_page_context)
    # Resolve local source assets before Sphinx's image and download collectors run.
    app.connect('doctree-read', doctree_read, priority=100)
    return {'version': '1', 'parallel_read_safe': True, 'parallel_write_safe': True}
