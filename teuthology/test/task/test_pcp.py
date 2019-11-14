import os
import requests

from teuthology.util.compat import parse_qs, urljoin

from mock import patch, DEFAULT, Mock, mock_open, call
from pytest import raises

from teuthology.config import config, FakeNamespace
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra.remote import Remote
from teuthology.orchestra.run import Raw
from teuthology.task.pcp import (PCPDataSource, PCPArchive, PCPGrapher,
                                 GrafanaGrapher, GraphiteGrapher, PCP)

from teuthology.test.task import TestTask

pcp_host = 'http://pcp.front.sepia.ceph.com:44323/'


class TestPCPDataSource(object):
    klass = PCPDataSource

    def setup(self):
        config.pcp_host = pcp_host

    def test_init(self):
        hosts = ['host1', 'host2']
        time_from = 'now-2h'
        time_until = 'now'
        obj = self.klass(
            hosts=hosts,
            time_from=time_from,
            time_until=time_until,
        )
        assert obj.hosts == hosts
        assert obj.time_from == time_from
        assert obj.time_until == time_until


class TestPCPArchive(TestPCPDataSource):
    klass = PCPArchive

    def test_get_archive_input_dir(self):
        hosts = ['host1', 'host2']
        time_from = 'now-1d'
        obj = self.klass(
            hosts=hosts,
            time_from=time_from,
        )
        assert obj.get_archive_input_dir('host1') == \
            '/var/log/pcp/pmlogger/host1'

    def test_get_pmlogextract_cmd(self):
        obj = self.klass(
            hosts=['host1'],
            time_from='now-3h',
            time_until='now-1h',
        )
        expected = [
            'pmlogextract',
            '-S', 'now-3h',
            '-T', 'now-1h',
            Raw('/var/log/pcp/pmlogger/host1/*.0'),
        ]
        assert obj.get_pmlogextract_cmd('host1') == expected

    def test_format_time(self):
        assert self.klass._format_time(1462893484) == \
            '@ Tue May 10 15:18:04 2016'

    def test_format_time_now(self):
        assert self.klass._format_time('now-1h') == 'now-1h'


class TestPCPGrapher(TestPCPDataSource):
    klass = PCPGrapher

    def test_init(self):
        hosts = ['host1', 'host2']
        time_from = 'now-2h'
        time_until = 'now'
        obj = self.klass(
            hosts=hosts,
            time_from=time_from,
            time_until=time_until,
        )
        assert obj.hosts == hosts
        assert obj.time_from == time_from
        assert obj.time_until == time_until
        expected_url = urljoin(config.pcp_host, self.klass._endpoint)
        assert obj.base_url == expected_url


class TestGrafanaGrapher(TestPCPGrapher):
    klass = GrafanaGrapher

    def test_build_graph_url(self):
        hosts = ['host1']
        time_from = 'now-3h'
        time_until = 'now-1h'
        obj = self.klass(
            hosts=hosts,
            time_from=time_from,
            time_until=time_until,
        )
        base_url = urljoin(
            config.pcp_host,
            'grafana/index.html#/dashboard/script/index.js',
        )
        assert obj.base_url == base_url
        got_url = obj.build_graph_url()
        parsed_query = parse_qs(got_url.split('?')[1])
        assert parsed_query['hosts'] == hosts
        assert len(parsed_query['time_from']) == 1
        assert parsed_query['time_from'][0] == time_from
        assert len(parsed_query['time_to']) == 1
        assert parsed_query['time_to'][0] == time_until

    def test_format_time(self):
        assert self.klass._format_time(1462893484) == \
            '2016-05-10T15:18:04'

    def test_format_time_now(self):
        assert self.klass._format_time('now-1h') == 'now-1h'


class TestGraphiteGrapher(TestPCPGrapher):
    klass = GraphiteGrapher

    def test_build_graph_urls(self):
        obj = self.klass(
            hosts=['host1', 'host2'],
            time_from='now-3h',
            time_until='now-1h',
        )
        expected_urls = [obj.get_graph_url(m) for m in obj.metrics]
        obj.build_graph_urls()
        built_urls = []
        for metric in obj.graphs.keys():
            built_urls.append(obj.graphs[metric]['url'])
        assert len(built_urls) == len(expected_urls)
        assert sorted(built_urls) == sorted(expected_urls)

    def test_check_dest_dir(self):
        obj = self.klass(
            hosts=['host1'],
            time_from='now-3h',
        )
        assert obj.dest_dir is None
        with raises(RuntimeError):
            obj._check_dest_dir()

    def test_generate_html_dynamic(self):
        obj = self.klass(
            hosts=['host1'],
            time_from='now-3h',
        )
        html = obj.generate_html()
        assert config.pcp_host in html

    def test_download_graphs(self):
        dest_dir = '/fake/path'
        obj = self.klass(
            hosts=['host1'],
            time_from='now-3h',
            dest_dir=dest_dir,
        )
        _format = obj.graph_defaults.get('format')
        with patch('teuthology.task.pcp.requests.get', create=True) as m_get:
            m_resp = Mock()
            m_resp.ok = True
            m_get.return_value = m_resp
            with patch('teuthology.task.pcp.open', mock_open(), create=True):
                obj.download_graphs()
        expected_filenames = []
        for metric in obj.metrics:
            expected_filenames.append(
                "{}.{}".format(
                    os.path.join(
                        dest_dir,
                        obj._sanitize_metric_name(metric),
                    ),
                    _format,
                )
            )
        graph_filenames = []
        for metric in obj.graphs.keys():
            graph_filenames.append(obj.graphs[metric]['file'])
        assert sorted(graph_filenames) == sorted(expected_filenames)

    def test_generate_html_static(self):
        obj = self.klass(
            hosts=['host1'],
            time_from='now-3h',
            dest_dir='/fake/path',
        )
        with patch('teuthology.task.pcp.requests.get', create=True) as m_get:
            m_resp = Mock()
            m_resp.ok = True
            m_get.return_value = m_resp
            with patch('teuthology.task.pcp.open', mock_open(), create=True):
                obj.download_graphs()
        html = obj.generate_html(mode='static')
        assert config.pcp_host not in html

    def test_sanitize_metric_name(self):
        sanitized_metrics = {
            'foo.bar': 'foo.bar',
            'foo.*': 'foo._all_',
            'foo.bar baz': 'foo.bar_baz',
            'foo.*.bar baz': 'foo._all_.bar_baz',
        }
        for in_, out in sanitized_metrics.items():
            assert self.klass._sanitize_metric_name(in_) == out

    def test_get_target_globs(self):
        obj = self.klass(
            hosts=['host1'],
            time_from='now-3h',
        )
        assert obj.get_target_globs() == ['*host1*']
        assert obj.get_target_globs('a.metric') == ['*host1*.a.metric']
        obj.hosts.append('host2')
        assert obj.get_target_globs() == ['*host1*', '*host2*']
        assert obj.get_target_globs('a.metric') == \
            ['*host1*.a.metric', '*host2*.a.metric']


class TestPCPTask(TestTask):
    klass = PCP
    task_name = 'pcp'

    def setup(self):
        self.ctx = FakeNamespace()
        self.ctx.cluster = Cluster()
        self.ctx.cluster.add(Remote('user@remote1'), ['role1'])
        self.ctx.cluster.add(Remote('user@remote2'), ['role2'])
        self.ctx.config = dict()
        self.task_config = dict()
        config.pcp_host = pcp_host

    def test_init(self):
        task = self.klass(self.ctx, self.task_config)
        assert task.stop_time == 'now'

    def test_disabled(self):
        config.pcp_host = None
        with self.klass(self.ctx, self.task_config) as task:
            assert task.enabled is False
            assert not hasattr(task, 'grafana')
            assert not hasattr(task, 'graphite')
            assert not hasattr(task, 'archiver')

    def test_setup(self):
        with patch.multiple(
            self.klass,
            setup_collectors=DEFAULT,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                task.setup_collectors.assert_called_once_with()
                assert isinstance(task.start_time, int)

    def test_setup_collectors(self):
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                assert hasattr(task, 'grafana')
                assert not hasattr(task, 'graphite')
                assert not hasattr(task, 'archiver')
            self.task_config['grafana'] = False
            with self.klass(self.ctx, self.task_config) as task:
                assert not hasattr(task, 'grafana')

    @patch('os.makedirs')
    def test_setup_grafana(self, m_makedirs):
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            self.ctx.archive = '/fake/path'
            with self.klass(self.ctx, self.task_config) as task:
                assert hasattr(task, 'grafana')
            self.task_config['grafana'] = False
            with self.klass(self.ctx, self.task_config) as task:
                assert not hasattr(task, 'grafana')

    @patch('os.makedirs')
    @patch('teuthology.task.pcp.GraphiteGrapher')
    def test_setup_graphite(self, m_graphite_grapher, m_makedirs):
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                assert not hasattr(task, 'graphite')
            self.task_config['graphite'] = False
            with self.klass(self.ctx, self.task_config) as task:
                assert not hasattr(task, 'graphite')
            self.ctx.archive = '/fake/path'
            self.task_config['graphite'] = True
            with self.klass(self.ctx, self.task_config) as task:
                assert hasattr(task, 'graphite')
            self.task_config['graphite'] = False
            with self.klass(self.ctx, self.task_config) as task:
                assert not hasattr(task, 'graphite')

    @patch('os.makedirs')
    @patch('teuthology.task.pcp.PCPArchive')
    def test_setup_archiver(self, m_archive, m_makedirs):
        with patch.multiple(
            self.klass,
            begin=DEFAULT,
            end=DEFAULT,
        ):
            self.task_config['fetch_archives'] = True
            with self.klass(self.ctx, self.task_config) as task:
                assert not hasattr(task, 'archiver')
            self.task_config['fetch_archives'] = False
            with self.klass(self.ctx, self.task_config) as task:
                assert not hasattr(task, 'archiver')
            self.ctx.archive = '/fake/path'
            self.task_config['fetch_archives'] = True
            with self.klass(self.ctx, self.task_config) as task:
                assert hasattr(task, 'archiver')
            self.task_config['fetch_archives'] = False
            with self.klass(self.ctx, self.task_config) as task:
                assert not hasattr(task, 'archiver')

    @patch('os.makedirs')
    @patch('teuthology.task.pcp.GrafanaGrapher')
    @patch('teuthology.task.pcp.GraphiteGrapher')
    def test_begin(self, m_grafana, m_graphite, m_makedirs):
        with patch.multiple(
            self.klass,
            end=DEFAULT,
        ):
            with self.klass(self.ctx, self.task_config) as task:
                task.grafana.build_graph_url.assert_called_once_with()
            self.task_config['graphite'] = True
            self.ctx.archive = '/fake/path'
            with self.klass(self.ctx, self.task_config) as task:
                task.graphite.write_html.assert_called_once_with()

    @patch('os.makedirs')
    @patch('teuthology.task.pcp.GrafanaGrapher')
    @patch('teuthology.task.pcp.GraphiteGrapher')
    def test_end(self, m_grafana, m_graphite, m_makedirs):
        self.ctx.archive = '/fake/path'
        with self.klass(self.ctx, self.task_config) as task:
            # begin() should have called write_html() once by now, with no args
            task.graphite.write_html.assert_called_once_with()
        # end() should have called write_html() a second time by now, with
        # mode=static
        second_call = task.graphite.write_html.call_args_list[1]
        assert second_call[1]['mode'] == 'static'
        assert isinstance(task.stop_time, int)

    @patch('os.makedirs')
    @patch('teuthology.task.pcp.GrafanaGrapher')
    @patch('teuthology.task.pcp.GraphiteGrapher')
    def test_end_16049(self, m_grafana, m_graphite, m_makedirs):
        # http://tracker.ceph.com/issues/16049
        # Jobs were failing if graph downloading failed. We don't want that.
        self.ctx.archive = '/fake/path'
        with self.klass(self.ctx, self.task_config) as task:
            task.graphite.download_graphs.side_effect = \
                requests.ConnectionError
        # Even though downloading graphs failed, we should have called
        # write_html() a second time, again with no args
        assert task.graphite.write_html.call_args_list == [call(), call()]
        assert isinstance(task.stop_time, int)
