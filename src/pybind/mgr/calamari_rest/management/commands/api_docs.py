#
# If Django is installed locally, run the following command from the top of
# the ceph source tree:
#
#   PYTHONPATH=src/pybind/mgr \
#   DJANGO_SETTINGS_MODULE=calamari_rest.settings \
#   CALAMARI_CONFIG=src/pybind/mgr/calamari.conf \
#       django-admin api_docs
#
# This will create resources.rst (the API docs), and rest.log (which will
# probably just be empty).
#
# TODO: Add the above to a makefile, so the docs land somewhere sane.
#

from collections import defaultdict
import json
from optparse import make_option
import os
from django.core.management.base import NoArgsCommand
import importlib
from jinja2 import Environment
import re
import rest_framework.viewsets
import traceback
from django.core.urlresolvers import RegexURLPattern, RegexURLResolver
import sys
import codecs

class ceph_state:
    pass

sys.modules["ceph_state"] = ceph_state

# Needed to avoid weird import loops
from rest import global_instance

from calamari_rest.serializers.v2 import ValidatingSerializer

GENERATED_PREFIX = "."


EXAMPLES_FILE = os.path.join(GENERATED_PREFIX, "api_examples.json")
RESOURCES_FILE = os.path.join("resources.rst")
EXAMPLES_PREFIX = "api_example_"


old_as_view = rest_framework.viewsets.ViewSetMixin.as_view


@classmethod
def as_view(cls, actions=None, **initkwargs):
    view = old_as_view.__func__(cls, actions, **initkwargs)
    view._actions = actions
    return view

rest_framework.viewsets.ViewSetMixin.as_view = as_view

# >>> RsT table code borrowed from http://stackoverflow.com/a/17203834/99876


def make_table(grid):
    max_cols = [max(out) for out in map(list, zip(*[[len(item) for item in row] for row in grid]))]
    rst = table_div(max_cols, 1)

    for i, row in enumerate(grid):
        header_flag = False
        if i == 0 or i == len(grid) - 1:
            header_flag = True
        rst += normalize_row(row, max_cols)
        rst += table_div(max_cols, header_flag)
    return rst


def table_div(max_cols, header_flag=1):
    out = ""
    if header_flag == 1:
        style = "="
    else:
        style = "-"

    for max_col in max_cols:
        out += max_col * style + " "

    out += "\n"
    return out


def normalize_row(row, max_cols):
    r = ""
    for i, max_col in enumerate(max_cols):
        r += row[i] + (max_col - len(row[i]) + 1) * " "

    return r + "\n"
# <<< RsT table code borrowed from http://stackoverflow.com/a/17203834/99876


PAGE_TEMPLATE = """

:tocdepth: 3

API resources
=============

URL summary
-----------

{{url_summary_rst}}

API reference
-------------

{{resources_rst}}

Examples
--------

.. toctree::
   :maxdepth: 1

{% for example_doc in example_docs %}
   {{example_doc}}
{% endfor %}


"""


RESOURCE_TEMPLATE = """

.. _{{class_name}}:

{{name}}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

{{help_text}}

URLs
____

{{url_table}}

Fields
______

{{field_table}}

"""


VERBS = ["GET", "PUT", "POST", "PATCH", "DELETE"]


def _url_pattern_methods(url_pattern):
    view_class = url_pattern.callback.cls

    if hasattr(url_pattern.callback, '_actions'):
        # An APIViewSet
        methods = [k.upper() for k in url_pattern.callback._actions.keys()]
    else:
        methods = view_class().allowed_methods

    if not methods:
        # A view that isn't using django rest framework?
        raise RuntimeError("No methods for url %s" % url_pattern.regex.pattern)

    return methods


def _stripped_url(prefix, url_pattern):
    """
    Convert a URL regex into something for human eyes

    ^server/(?P<pk>[^/]+)$ becomes server/<pk>
    """
    url = prefix + url_pattern.regex.pattern.strip("^$")
    url = re.sub("\(.+?<(.+?)>.+?\)", "<\\1>", url)
    return url


def _pretty_url(prefix, url_pattern):
    return "%s" % _stripped_url(prefix, url_pattern).replace("<", "\\<").replace(">", "\\>")


def _find_prefix(toplevel_mod, sub_mod):
    """
    Find the URL prefix of sub_mod in toplevel_mod
    """
    for toplevel_pattern in importlib.import_module(toplevel_mod).urlpatterns:
        if isinstance(toplevel_pattern, RegexURLResolver):
            if toplevel_pattern.urlconf_name.__name__ == sub_mod:
                regex_str = toplevel_pattern.regex.pattern
                return regex_str.strip("^")

    raise RuntimeError("'%s' not included in '%s', cannot find prefix" % (sub_mod, toplevel_mod))


class ApiIntrospector(object):
    def __init__(self, url_module):
        view_to_url_patterns = defaultdict(list)

        def parse_urls(urls):
            for url_pattern in urls:
                if isinstance(url_pattern, RegexURLResolver):
                    parse_urls(url_pattern.urlconf_module)
                elif isinstance(url_pattern, RegexURLPattern):
                    if url_pattern.regex.pattern.endswith('\.(?P<format>[a-z0-9]+)$'):
                        # Suppress the .<format> urls that rest_framework generates
                        continue

                    if hasattr(url_pattern.callback, 'cls'):
                        # This is a rest_framework as_view wrapper
                        view_cls = url_pattern.callback.cls
                        if view_cls.__name__.endswith("APIRoot"):
                            continue
                        view_to_url_patterns[view_cls].append(url_pattern)

        self.prefix = _find_prefix("calamari_rest.urls", url_module)
        parse_urls(importlib.import_module(url_module).urlpatterns)

        self.view_to_url_patterns = sorted(view_to_url_patterns.items(), cmp=lambda x, y: cmp(x[0].__name__, y[0].__name__))

        self.all_url_patterns = []
        for view, url_patterns in self.view_to_url_patterns:
            self.all_url_patterns.extend(url_patterns)
        self.all_url_patterns = sorted(self.all_url_patterns,
                                       lambda a, b: cmp(_pretty_url(self.prefix, a), _pretty_url(self.prefix, b)))

    def _view_rst(self, view, url_patterns):
        """
        Output RsT for one API view
        """
        name = view().get_view_name()

        if view.__doc__:
            view_help_text = view.__doc__
        else:
            view_help_text = "*No description available*"

        url_table = [["URL"] + VERBS]
        for url_pattern in url_patterns:
            methods = _url_pattern_methods(url_pattern)

            row = [":doc:`%s <%s>`" % (_pretty_url(self.prefix, url_pattern),
                                       self._example_document_name(_stripped_url(self.prefix, url_pattern)))]
            for v in VERBS:
                if v in methods:
                    row.append("Yes")
                else:
                    row.append("")
            url_table.append(row)

        url_table_rst = make_table(url_table)

        if hasattr(view, 'serializer_class') and view.serializer_class:
            field_table = [["Name", "Type", "Readonly", "Create", "Modify", "Description"]]

            serializer = view.serializer_class()
            if isinstance(serializer, ValidatingSerializer):
                allowed_during_create = serializer.Meta.create_allowed
                required_during_create = serializer.Meta.create_required
                allowed_during_modify = serializer.Meta.modify_allowed
                required_during_modify = serializer.Meta.modify_required
            else:
                allowed_during_create = required_during_create = allowed_during_modify = required_during_modify = ()

            fields = serializer.get_fields()
            for field_name, field in fields.items():
                create = modify = ''
                if field_name in allowed_during_create:
                    create = 'Allowed'
                if field_name in required_during_create:
                    create = 'Required'

                if field_name in allowed_during_modify:
                    modify = 'Allowed'
                if field_name in required_during_modify:
                    modify = 'Required'

                if hasattr(field, 'help_text'):
                    field_help_text = field.help_text
                else:
                    field_help_text = ""
                field_table.append(
                    [field_name,
                     field.__class__.__name__,
                     str(field.read_only),
                     create,
                     modify,
                     field_help_text if field_help_text else ""])
            field_table_rst = make_table(field_table)
        else:
            field_table_rst = "*No field data available*"

        return Environment().from_string(RESOURCE_TEMPLATE).render(
            name=name,
            class_name=view.__name__,
            help_text=view_help_text,
            field_table=field_table_rst,
            url_table=url_table_rst
        )

    def _url_table(self, url_patterns):
        url_table = [["URL", "View", "Examples"] + VERBS]
        for view, url_patterns in self.view_to_url_patterns:
            for url_pattern in url_patterns:
                methods = _url_pattern_methods(url_pattern)

                row = [_pretty_url(self.prefix, url_pattern)]

                view_name = view().get_view_name()
                row.append(
                    u":ref:`{0} <{1}>`".format(view_name.replace(" ", unichr(0x00a0)), view.__name__)
                )

                example_doc_name = self._example_document_name(_stripped_url(self.prefix, url_pattern))
                if os.path.exists("{0}/{1}.rst".format(GENERATED_PREFIX, example_doc_name)):
                    print "It exists: {0}".format(example_doc_name)
                    row.append(":doc:`%s <%s>`" % ("Example", example_doc_name))
                else:
                    row.append("")
                for v in VERBS:
                    if v in methods:
                        row.append("Yes")
                    else:
                        row.append("")
                url_table.append(row)

        return make_table(url_table)

    def _flatten_path(self, path):
        """
        Escape a URL pattern to something suitable for use as a filename
        """
        return path.replace("/", "_").replace("<", "_").replace(">", "_")

    def _example_document_name(self, pattern):
        return EXAMPLES_PREFIX + self._flatten_path(pattern)

    def _write_example(self, example_pattern, example_results):
        """
        Write RsT file with API examples for a particular pattern
        """
        rst = ""
        title = "Examples for %s" % example_pattern
        rst += "%s\n%s\n\n" % (title, "=" * len(title))
        for url, content in example_results.items():
            rst += "%s\n" % url
            rst += "-" * len(url)
            rst += "\n\n.. code-block:: json\n\n"
            data_dump = json.dumps(json.loads(content), indent=2)
            data_dump = "\n".join(["   %s" % l for l in data_dump.split("\n")])
            rst += data_dump
            rst += "\n\n"
        codecs.open("{0}/{1}.rst".format(GENERATED_PREFIX, self._example_document_name(example_pattern)), 'w',
                    encoding="UTF-8").write(rst)

    def write_docs(self, examples):
        resources_rst = ""
        for view, url_patterns in self.view_to_url_patterns:
            resources_rst += self._view_rst(view, url_patterns)

        url_table_rst = self._url_table(self.all_url_patterns)

        example_docs = [self._example_document_name(p) for p in examples.keys()]

        resources_rst = Environment().from_string(PAGE_TEMPLATE).render(
            resources_rst=resources_rst, url_summary_rst=url_table_rst, example_docs=example_docs)
        codecs.open(RESOURCES_FILE, 'w', encoding="UTF-8").write(resources_rst)

        for example_pattern, example_results in examples.items():
            self._write_example(example_pattern, example_results)

    def get_url_list(self, method="GET"):
        return [_stripped_url(self.prefix, u)
                for u in self.all_url_patterns
                if method in _url_pattern_methods(u)]


class Command(NoArgsCommand):
    help = "Print introspected REST API documentation"
    option_list = NoArgsCommand.option_list + (
        make_option('--list-urls',
                    action='store_true',
                    dest='list_urls',
                    default=False,
                    help='Print a list of URL patterns instead of RsT documentation'),
    )

    def handle_noargs(self, list_urls, **options):
        introspector = ApiIntrospector("calamari_rest.urls.v2")
        if list_urls:
            # TODO: this just prints an empty array (not sure why)
            print json.dumps(introspector.get_url_list())
        else:
            try:
                try:
                    examples = json.load(open(EXAMPLES_FILE, 'r'))
                except IOError:
                    examples = {}
                    print >>sys.stderr, "Examples data '%s' not found, no examples will be generated" % EXAMPLES_FILE

                introspector.write_docs(examples)
            except:
                print >>sys.stderr, traceback.format_exc()
                raise
