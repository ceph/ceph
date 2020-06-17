# maybe run pcp role?
import datetime
import dateutil.tz
import jinja2
import logging
import os
import requests
import time

from teuthology.util.compat import urljoin, urlencode

from teuthology.config import config as teuth_config
from teuthology.orchestra import run

from teuthology import misc
from teuthology.task import Task

log = logging.getLogger(__name__)


# Because PCP output is nonessential, set a timeout to avoid stalling
# tests if the server does not respond promptly.
GRAPHITE_DOWNLOAD_TIMEOUT = 60


class PCPDataSource(object):
    def __init__(self, hosts, time_from, time_until='now'):
        self.hosts = hosts
        self.time_from = time_from
        self.time_until = time_until


class PCPArchive(PCPDataSource):
    archive_base_path = '/var/log/pcp/pmlogger'
    archive_file_extensions = ('0', 'index', 'meta')

    def get_archive_input_dir(self, host):
        return os.path.join(
            self.archive_base_path,
            host,
        )

    def get_pmlogextract_cmd(self, host):
        cmd = [
            'pmlogextract',
            '-S', self._format_time(self.time_from),
            '-T', self._format_time(self.time_until),
            run.Raw(os.path.join(
                self.get_archive_input_dir(host),
                '*.0')),
        ]
        return cmd

    @staticmethod
    def _format_time(seconds):
        if isinstance(seconds, str):
            return seconds
        return "@ %s" % time.asctime(time.gmtime(seconds))


class PCPGrapher(PCPDataSource):
    _endpoint = '/'

    def __init__(self, hosts, time_from, time_until='now'):
        super(PCPGrapher, self).__init__(hosts, time_from, time_until)
        self.base_url = urljoin(
            teuth_config.pcp_host,
            self._endpoint)


class GrafanaGrapher(PCPGrapher):
    _endpoint = '/grafana/index.html#/dashboard/script/index.js'

    def __init__(self, hosts, time_from, time_until='now', job_id=None):
        super(GrafanaGrapher, self).__init__(hosts, time_from, time_until)
        self.job_id = job_id

    def build_graph_url(self):
        config = dict(
            hosts=','.join(self.hosts),
            time_from=self._format_time(self.time_from),
        )
        if self.time_until:
            config['time_to'] = self._format_time(self.time_until)
        args = urlencode(config)
        template = "{base_url}?{args}"
        return template.format(base_url=self.base_url, args=args)

    @staticmethod
    def _format_time(seconds):
        if isinstance(seconds, str):
            return seconds
        seconds = int(seconds)
        dt = datetime.datetime.fromtimestamp(seconds, dateutil.tz.tzutc())
        return dt.strftime('%Y-%m-%dT%H:%M:%S')


class GraphiteGrapher(PCPGrapher):
    metrics = [
        'kernel.all.load.1 minute',
        'mem.util.free',
        'mem.util.used',
        'network.interface.*.bytes.*',
        'disk.all.read_bytes',
        'disk.all.write_bytes',
    ]

    graph_defaults = dict(
        width='1200',
        height='300',
        hideLegend='false',
        format='png',
    )
    _endpoint = '/graphite/render'

    def __init__(self, hosts, time_from, time_until='now', dest_dir=None,
                 job_id=None):
        super(GraphiteGrapher, self).__init__(hosts, time_from, time_until)
        self.dest_dir = dest_dir
        self.job_id = job_id

    def build_graph_urls(self):
        if not hasattr(self, 'graphs'):
            self.graphs = dict()
        for metric in self.metrics:
            metric_dict = self.graphs.get(metric, dict())
            metric_dict['url'] = self.get_graph_url(metric)
            self.graphs[metric] = metric_dict

    def _check_dest_dir(self):
        if not self.dest_dir:
            raise RuntimeError("Must provide a dest_dir!")

    def write_html(self, mode='dynamic'):
        self._check_dest_dir()
        generated_html = self.generate_html(mode=mode)
        html_path = os.path.join(self.dest_dir, 'pcp.html')
        with open(html_path, 'w') as f:
            f.write(generated_html)

    def generate_html(self, mode='dynamic'):
        self.build_graph_urls()
        cwd = os.path.dirname(__file__)
        loader = jinja2.loaders.FileSystemLoader(cwd)
        env = jinja2.Environment(loader=loader)
        template = env.get_template('pcp.j2')
        data = template.render(
            job_id=self.job_id,
            graphs=self.graphs,
            mode=mode,
        )
        return data

    def download_graphs(self):
        self._check_dest_dir()
        self.build_graph_urls()
        for metric in self.graphs.keys():
            url = self.graphs[metric]['url']
            filename = self._sanitize_metric_name(metric) + '.png'
            self.graphs[metric]['file'] = graph_path = os.path.join(
                self.dest_dir,
                filename,
            )
            resp = requests.get(url, timeout=GRAPHITE_DOWNLOAD_TIMEOUT)
            if not resp.ok:
                log.warn(
                    "Graph download failed with error %s %s: %s",
                    resp.status_code,
                    resp.reason,
                    url,
                )
                continue
            with open(graph_path, 'wb') as f:
                f.write(resp.content)

    def get_graph_url(self, metric):
        config = dict(self.graph_defaults)
        config.update({
            'from': self.time_from,
            'until': self.time_until,
            # urlencode with doseq=True encodes each item as a separate
            # 'target=' arg
            'target': self.get_target_globs(metric),
        })
        args = urlencode(config, doseq=True)
        template = "{base_url}?{args}"
        return template.format(base_url=self.base_url, args=args)

    def get_target_globs(self, metric=''):
        globs = ['*{}*'.format(host) for host in self.hosts]
        if metric:
            globs = ['{}.{}'.format(glob, metric) for glob in globs]
        return globs

    @staticmethod
    def _sanitize_metric_name(metric):
        result = metric
        replacements = [
            (' ', '_'),
            ('*', '_all_'),
        ]
        for rep in replacements:
            result = result.replace(rep[0], rep[1])
        return result


class PCP(Task):
    """
    Collects performance data using PCP during a job.

    Configuration options include:
        ``graphite``: Whether to render PNG graphs using Graphite (default:
            True)
        ``grafana``: Whether to build (and submit to paddles) a link to a
            dynamic Grafana dashboard containing graphs of performance data
            (default: True)
        ``fetch_archives``: Whether to assemble and ship a raw PCP archive
        containing performance data to the job's output archive (default:
            False)
    """
    enabled = True

    def __init__(self, ctx, config):
        super(PCP, self).__init__(ctx, config)
        if teuth_config.get('pcp_host') is None:
            self.enabled = False
        self.log = log
        self.job_id = self.ctx.config.get('job_id')
        # until the job stops, we may want to render graphs reflecting the most
        # current data
        self.stop_time = 'now'
        self.use_graphite = self.config.get('graphite', True)
        self.use_grafana = self.config.get('grafana', True)
        # fetch_archives defaults to False for now because of various bugs in
        # pmlogextract
        self.fetch_archives = self.config.get('fetch_archives', False)

    def setup(self):
        if not self.enabled:
            return
        super(PCP, self).setup()
        self.start_time = int(time.time())
        log.debug("start_time: %s", self.start_time)
        self.setup_collectors()

    def setup_collectors(self):
        log.debug("cluster: %s", self.cluster)
        hosts = [rem.shortname for rem in self.cluster.remotes.keys()]
        self.setup_grafana(hosts)
        self.setup_graphite(hosts)
        self.setup_archive(hosts)

    def setup_grafana(self, hosts):
        if self.use_grafana:
            self.grafana = GrafanaGrapher(
                hosts=hosts,
                time_from=self.start_time,
                time_until=self.stop_time,
                job_id=self.job_id,
            )

    def setup_graphite(self, hosts):
        if not getattr(self.ctx, 'archive', None):
            self.use_graphite = False
        if self.use_graphite:
            out_dir = os.path.join(
                self.ctx.archive,
                'pcp',
                'graphite',
            )
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            self.graphite = GraphiteGrapher(
                hosts=hosts,
                time_from=self.start_time,
                time_until=self.stop_time,
                dest_dir=out_dir,
                job_id=self.job_id,
            )

    def setup_archive(self, hosts):
        if not getattr(self.ctx, 'archive', None):
            self.fetch_archives = False
        if self.fetch_archives:
            self.archiver = PCPArchive(
                hosts=hosts,
                time_from=self.start_time,
                time_until=self.stop_time,
            )

    def begin(self):
        if not self.enabled:
            return
        if self.use_grafana:
            log.info(
                "PCP+Grafana dashboard: %s",
                self.grafana.build_graph_url(),
            )
        if self.use_graphite:
            self.graphite.write_html()

    def end(self):
        if not self.enabled:
            return
        self.stop_time = int(time.time())
        self.setup_collectors()
        log.debug("stop_time: %s", self.stop_time)
        if self.use_grafana:
            grafana_url = self.grafana.build_graph_url()
            log.info(
                "PCP+Grafana dashboard: %s",
                grafana_url,
            )
            if hasattr(self.ctx, 'summary'):
                self.ctx.summary['pcp_grafana_url'] = grafana_url
        if self.use_graphite:
            try:
                self.graphite.download_graphs()
                self.graphite.write_html(mode='static')
            except (requests.ConnectionError, requests.ReadTimeout):
                log.exception("Downloading graphs failed!")
                self.graphite.write_html()
        if self.fetch_archives:
            for remote in self.cluster.remotes.keys():
                log.info("Copying PCP data into archive...")
                cmd = self.archiver.get_pmlogextract_cmd(remote.shortname)
                archive_out_path = os.path.join(
                    misc.get_testdir(),
                    'pcp_archive_%s' % remote.shortname,
                )
                cmd.append(archive_out_path)
                remote.run(args=cmd)


task = PCP
