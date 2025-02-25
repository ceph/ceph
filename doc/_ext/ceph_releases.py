# cobbled together from:
#   https://github.com/sphinx-contrib/documentedlist/blob/master/sphinxcontrib/documentedlist.py
#   https://github.com/sphinx-doc/sphinx/blob/v1.6.3/sphinx/ext/graphviz.py
#   https://github.com/thewtex/sphinx-contrib/blob/master/exceltable/sphinxcontrib/exceltable.py
#   https://bitbucket.org/prometheus/sphinxcontrib-htsql/src/331a542c29a102eec9f8cba44797e53a49de2a49/sphinxcontrib/htsql.py?at=default&fileviewer=file-view-default
# into the glory that follows:
import json
import yaml
import jinja2
import sphinx
import datetime
from docutils.parsers.rst import Directive
from docutils import nodes
from sphinx.util import logging

logger = logging.getLogger(__name__)


class CephReleases(Directive):
    has_content = False
    required_arguments = 2
    optional_arguments = 0
    option_spec = {}

    def run(self):
        filename = self.arguments[0]
        current = self.arguments[1] == 'current'
        document = self.state.document
        env = document.settings.env
        rel_filename, filename = env.relfn2path(filename)
        env.note_dependency(filename)
        try:
            with open(filename, 'r') as fp:
                releases = yaml.safe_load(fp)
                releases = releases["releases"]
        except Exception as e:
            return [document.reporter.warning(
                "Failed to open Ceph releases file {}: {}".format(filename, e),
                line=self.lineno)]

        table = nodes.table()
        tgroup = nodes.tgroup(cols=3)
        table += tgroup

        tgroup.extend(
            nodes.colspec(colwidth=30, colname='c'+str(idx))
            for idx, _ in enumerate(range(4)))

        thead = nodes.thead()
        tgroup += thead
        row_node = nodes.row()
        thead += row_node
        row_node.extend(
            nodes.entry(h, nodes.paragraph(text=h))
            for h in ["Name", "Initial release", "Latest",
                      "End of life (estimated)" if current else "End of life"])

        releases = releases.items()
        releases = sorted(releases, key=lambda t: t[0], reverse=True)

        tbody = nodes.tbody()
        tgroup += tbody

        rows = []
        for code_name, info in releases:
            actual_eol = info.get("actual_eol", None)

            if current:
                if actual_eol and actual_eol <= datetime.datetime.now().date():
                    continue
            else:
                if not actual_eol:
                    continue

            trow = nodes.row()

            entry = nodes.entry()
            para = nodes.paragraph(text=f"`{code_name.title()} <{code_name}>`_")
            sphinx.util.nodes.nested_parse_with_titles(
                    self.state, para, entry)
            #entry += para
            trow += entry

            sorted_releases = sorted(info["releases"],
                    key=lambda t: [t["released"]] + list(map(lambda v: int(v), t["version"].split("."))))
            oldest_release = sorted_releases[0]
            newest_release = sorted_releases[-1]

            entry = nodes.entry()
            para = nodes.paragraph(text="{}".format(
                oldest_release["released"]))
            entry += para
            trow += entry

            entry = nodes.entry()
            if newest_release.get("skip_ref", False):
                para = nodes.paragraph(text="{}".format(
                    newest_release["version"]))
            else:
                para = nodes.paragraph(text="`{}`_".format(
                    newest_release["version"]))
            sphinx.util.nodes.nested_parse_with_titles(
                    self.state, para, entry)
            #entry += para
            trow += entry

            entry = nodes.entry()
            if current:
                para = nodes.paragraph(text=info.get("target_eol", '--'))
            else:
                para = nodes.paragraph(text=info.get('actual_eol', '--'))
            entry += para
            trow += entry

            rows.append(trow)

        tbody.extend(rows)

        return [table]


RELEASES_TEMPLATE = '''
.. mermaid::

   gantt
       dateFormat  YYYY-MM-DD
       axisFormat  %Y
       section Active Releases
{% for release in active_releases %}
       {{ release.code_name }} (latest {{ release.last_version }}): done, {{ release.debute_date }},{{ release.lifetime }}d
{% endfor %}
       section Archived Releases
{% for release in archived_releases %}
       {{ release.code_name }} (latest {{ release.last_version }}): done, {{ release.debute_date }},{{ release.lifetime }}d
{% endfor %}
'''


class ReleasesGantt(Directive):
    has_content = True
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False

    template = jinja2.Environment().from_string(RELEASES_TEMPLATE)

    def _render_time_line(self, filename):
        try:
            with open(filename) as f:
                releases = yaml.safe_load(f)['releases']
        except Exception as e:
            message = f'Unable read release file: "{filename}": {e}'
            self.error(message)

        active_releases = []
        archived_releases = []
        # just update `releases` with extracted info
        for code_name, info in releases.items():
            last_release = info['releases'][0]
            first_release = info['releases'][-1]
            last_version = last_release['version']
            debute_date = first_release['released']
            if 'actual_eol' in info:
                lifetime = info['actual_eol'] - first_release['released']
            else:
                lifetime = info['target_eol'] - first_release['released']
            release = dict(code_name=code_name,
                           last_version=last_version,
                           debute_date=debute_date,
                           lifetime=lifetime.days)
            if 'actual_eol' in info:
                archived_releases.append(release)
            else:
                active_releases.append(release)
        rendered = self.template.render(active_releases=active_releases,
                                        archived_releases=archived_releases)
        return rendered.splitlines()

    def run(self):
        filename = self.arguments[0]
        document = self.state.document
        env = document.settings.env
        rel_filename, filename = env.relfn2path(filename)
        env.note_dependency(filename)
        lines = self._render_time_line(filename)
        lineno = self.lineno - self.state_machine.input_offset - 1
        source = self.state_machine.input_lines.source(lineno)
        self.state_machine.insert_input(lines, source)
        return []


class CephTimeline(Directive):
    has_content = False
    required_arguments = 4
    optional_arguments = 0
    option_spec = {}

    def run(self):
        filename = self.arguments[0]
        document = self.state.document
        env = document.settings.env
        rel_filename, filename = env.relfn2path(filename)
        env.note_dependency(filename)
        try:
            with open(filename, 'r') as fp:
                releases = yaml.safe_load(fp)
        except Exception as e:
            return [document.reporter.warning(
                "Failed to open Ceph releases file {}: {}".format(filename, e),
                line=self.lineno)]

        display_releases = self.arguments[1:]

        timeline = []
        for code_name, info in releases["releases"].items():
            if code_name in display_releases:
                for release in info.get("releases", []):
                    released = release["released"]
                    timeline.append((released, code_name, release["version"],
                        release.get("skip_ref", False)))

        assert "development" not in releases["releases"]
        if "development" in display_releases:
            for release in releases["development"]["releases"]:
                released = release["released"]
                timeline.append((released, "development", release["version"],
                    release.get("skip_ref", False)))

        timeline = sorted(timeline, key=lambda t: t[0], reverse=True)

        table = nodes.table()
        tgroup = nodes.tgroup(cols=3)
        table += tgroup

        columns = ["Date"] + display_releases
        tgroup.extend(
            nodes.colspec(colwidth=30, colname='c'+str(idx))
            for idx, _ in enumerate(range(len(columns))))

        thead = nodes.thead()
        tgroup += thead
        row_node = nodes.row()
        thead += row_node
        for col in columns:
            entry = nodes.entry()
            if col.lower() in ["date", "development"]:
                para = nodes.paragraph(text=col.title())
            else:
                para = nodes.paragraph(text=f"`{col.title()} <{col}>`_".format(col))
            sphinx.util.nodes.nested_parse_with_titles(
                    self.state, para, entry)
            row_node += entry

        tbody = nodes.tbody()
        tgroup += tbody

        rows = []
        for row_info in timeline:
            trow = nodes.row()

            entry = nodes.entry()
            para = nodes.paragraph(text=row_info[0])
            entry += para
            trow += entry

            for release in display_releases:
                entry = nodes.entry()
                if row_info[1] == release:
                    if row_info[3]: # if skip ref
                        para = nodes.paragraph(text=row_info[2])
                    else:
                        para = nodes.paragraph(text="`{}`_".format(row_info[2]))
                    sphinx.util.nodes.nested_parse_with_titles(
                            self.state, para, entry)
                else:
                    para = nodes.paragraph(text="--")
                    entry += para
                trow += entry
            rows.append(trow)

        tbody.extend(rows)

        return [table]


TIMELINE_TEMPLATE = '''
.. mermaid::

   gantt
       dateFormat  YYYY-MM-DD
       axisFormat  %Y-%m
{% if title %}
       title       {{title}}
{% endif %}
{% for display_release in display_releases %}
       section {{ display_release }}
{%if releases[display_release].actual_eol %}
       End of life:             crit,            {{ releases[display_release].actual_eol }},4d
{% else %}
       End of life (estimated): crit,            {{ releases[display_release].target_eol }},4d
{% endif %}
{% for release in releases[display_release].releases | sort(attribute='released', reverse=True) %}
       {{ release.version }}:   milestone, done, {{ release.released }},0d
{% endfor %}
{% endfor %}
'''


class TimeLineGantt(Directive):
    has_content = True
    required_arguments = 2
    optional_arguments = 0
    final_argument_whitespace = True

    template = jinja2.Environment().from_string(TIMELINE_TEMPLATE)

    def _render_time_line(self, filename, display_releases):
        try:
            with open(filename) as f:
                releases = yaml.safe_load(f)['releases']
        except Exception as e:
            message = f'Unable read release file: "{filename}": {e}'
            self.error(message)

        rendered = self.template.render(display_releases=display_releases,
                                        releases=releases)
        return rendered.splitlines()

    def run(self):
        filename = self.arguments[0]
        display_releases = self.arguments[1].split()
        document = self.state.document
        env = document.settings.env
        rel_filename, filename = env.relfn2path(filename)
        env.note_dependency(filename)
        lines = self._render_time_line(filename, display_releases)
        lineno = self.lineno - self.state_machine.input_offset - 1
        source = self.state_machine.input_lines.source(lineno)
        self.state_machine.insert_input(lines, source)
        return []


def setup(app):
    app.add_directive('ceph_releases', CephReleases)
    app.add_directive('ceph_releases_gantt', ReleasesGantt)
    app.add_directive('ceph_timeline', CephTimeline)
    app.add_directive('ceph_timeline_gantt', TimeLineGantt)
    return {
        'parallel_read_safe': True,
        'parallel_write_safe': True
    }
