"""Keep document links usable through both HTTP and local file browsing."""

from html import escape, unescape
import posixpath
import re
from urllib.parse import urlsplit, urlunsplit


def explicit_document_links(output, current_path, document_paths):
    def rewrite(match):
        url = urlsplit(unescape(match[3]))
        if url.scheme or url.netloc or not url.path or url.path.startswith('/'):
            return match[0]
        target = posixpath.normpath(posixpath.join(posixpath.dirname(current_path), url.path))
        if posixpath.normpath(posixpath.join(target, 'index.html')) not in document_paths:
            return match[0]
        path = url.path.rstrip('/') + '/index.html'
        destination = urlunsplit(('', '', path, url.query, url.fragment))
        return match[1] + match[2] + escape(destination, quote=True) + match[2]

    return re.sub(r'''(\bhref\s*=\s*)(["'])(.*?)\2''', rewrite, output, flags=re.I)


def on_files(files, config, **kwargs):
    config.extra['document_paths'] = {file.dest_uri for file in files.documentation_pages()}
    return files


def on_post_page(output, page, config, **kwargs):
    return explicit_document_links(output, page.file.dest_uri, config.extra['document_paths'])


def on_post_template(output, template_name, config, **kwargs):
    return explicit_document_links(output, template_name, config.extra['document_paths'])
