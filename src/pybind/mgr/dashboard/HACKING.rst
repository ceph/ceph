
HACKING
=======

See the top-level docs in the ceph repository for general information about how to submit code.  This
file is about the specifics of how this module fits together.

This module uses deliberately simple/explicit structure so that it is accessbile
to people who are not specialists in webapps.  Very little javascript knowledge
is needed.  The absence of a javascript toolchain (node, bower, grunt, etc) is
considered a feature rather than a bug.  All of the CSS and Javascript in
the git repository is used as-is without any compilation/minification.

On the server (i.e. python-side)
--------------------------------

In the serve() method, there's a cherrypy request handler object with methods that correspond to URLs.
See the cherrypy documentatino for how that stuff works.

There is a mixture of endpoints that return JSON (for live updates of pages) and endpoints that return
HTML (for initial loads of pages).  The HTML files are rendered from jinja2 templates (the .html files
in the source tree).

The initial render of the HTML template includes some json-ized inline data, so that the page can
be rendered immediately without having to make another request to the server to load the data
after loading the markup.

The pattern is that for some resource 'foo' we would generally have three methods, along the lines of:

::

    def _foo(self):
        return {...the data of interest...}

    @cherrypy.expose
    def foo(self):
        data = self._foo()
        *render a foo.html template with data inline*

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def foo_data(self):
        return self._foo()

In the browser (Javascript and markup)
--------------------------------------

The javascript code uses the rivets.js (http://rivetsjs.com/) library to render the data from
the server into HTML templates, and to enable subsequent updates without reloading the whole
page.

The use of rivets.js looks something like this:

::

    <tr rv-each-server="servers">
        <td>
            {server.hostname}
        </td>

The "rv-each" attributes do iteration over lists, and the curly braces substitute
values into place.  See the rivets docs for more.

If you see double-curly-brace items in the markup, those are the ones that
were populated server side, including the initial data, like this:

::

     var content_data = {{ content_data }};

If you really wanted to, you could skip rivets on any given page, and just
render the whole page server-side with ``{{}}`` entries, but that of course
leaves you with a page that won't update without the user actively refreshing.

The common markup such as headers and footers lives in ``base.html``, and
all the other pages' templates start with ``{% extends "base.html" %}``.  This
is jinja2 syntax that is handled server-side.

Styles/CSS
----------

This module includes the AdminLTE dashboard layout.  This is perhaps overkill,
but saves us from writing any significant amount of CSS.  If you need to fudge
something, don't be shy about putting "style=" attributes inline within reason.

URLs
----

The URLs follow a convention that CherryPy knows how to route for us: the
name of the handler function, followed by positional arguments between
slashes.  For example, a handler like ``def filesystem(self, fscid)`` is
automatically called on requests like ``/filesystem/123```



