# -*- coding: utf-8 -*-
import errno
import time
from unittest.mock import MagicMock, patch

import pytest

from ..services.nvmeof_top_cli import Counter, NvmeofTopCollector, NVMeoFTopCPU, NVMeoFTopIO
from ..tests import CLICommandTestMixin, CmdException


@pytest.fixture(name='cpu_collector')
def fixture_cpu_collector():
    collector = MagicMock()
    collector.get_reactor_data.return_value = []
    collector.delay = 1.5
    collector.timestamp = time.time()
    return collector


@pytest.fixture(name='io_collector')
def fixture_io_collector():
    collector = MagicMock()
    collector.get_sorted_namespaces.return_value = []
    collector.get_subsystem_summary_data.return_value = []
    collector.get_overall_summary_data.return_value = []
    collector.delay = 2.0
    collector.timestamp = time.time()
    return collector


class TestCounter:
    def test_initial_values(self):
        c = Counter()
        assert c.current == 0.0
        assert c.last == 0.0

    def test_update_tracks_last(self):
        c = Counter()
        c.update(10.0)
        assert c.current == 10.0
        assert c.last == 0.0
        c.update(20.0)
        assert c.current == 20.0
        assert c.last == 10.0

    def test_rate(self):
        c = Counter()
        c.update(100.0)
        c.update(150.0)
        assert c.rate(5.0) == 10.0

    def test_rate_zero_interval_returns_zero(self):
        c = Counter()
        c.update(100.0)
        assert c.rate(0) == 0.0


class TestNVMeoFTopCPUFormat:
    default_args = {
        'sort_by': 'Thread Name',
        'sort_descending': False,
        'with_timestamp': False,
        'no_header': False,
        'service': '',
        'server_addr': '',
        'group': '',
    }

    def test_headers(self, cpu_collector):
        tool = NVMeoFTopCPU(self.default_args, cpu_collector)
        output = tool.format_output()
        assert 'Gateway' in output
        assert 'Thread Name' in output
        assert 'Busy Rate%' in output
        assert 'Idle Rate%' in output

    def test_no_header(self, cpu_collector):
        tool = NVMeoFTopCPU({**self.default_args, 'no_header': True}, cpu_collector)
        output = tool.format_output()
        assert 'Gateway' not in output

    def test_with_timestamp(self, cpu_collector):
        tool = NVMeoFTopCPU({**self.default_args, 'with_timestamp': True}, cpu_collector)
        output = tool.format_output()
        assert 'delay:' in output
        assert '1.50s' in output

    def test_reactor_data(self, cpu_collector):
        cpu_collector.get_reactor_data.return_value = [
            ('192.168.1.1:5500', 'reactor_0', '72.50', '27.50'),
            ('192.168.1.1:5500', 'reactor_1', '45.00', '55.00'),
        ]
        tool = NVMeoFTopCPU(self.default_args, cpu_collector)
        output = tool.format_output()
        assert '192.168.1.1:5500' in output
        assert 'reactor_0' in output
        assert 'reactor_1' in output

    def test_invalid_sort_key(self, cpu_collector):
        tool = NVMeoFTopCPU({**self.default_args, 'sort_by': 'NonExistent'}, cpu_collector)
        with pytest.raises(ValueError, match="Invalid sort key"):
            tool.format_output()


class TestNVMeoFTopIOFormat:
    default_args = {
        'sort_by': 'NSID',
        'sort_descending': False,
        'with_timestamp': False,
        'no_header': False,
        'summary': False,
        'subsystem': 'nqn.2024-01.io.spdk:cnode1',
        'server_addr': '',
        'group': '',
    }

    def test_no_namespaces(self, io_collector):
        tool = NVMeoFTopIO(self.default_args, io_collector)
        output = tool.format_output()
        assert '<no namespaces defined>' in output

    def test_headers_present(self, io_collector):
        tool = NVMeoFTopIO(self.default_args, io_collector)
        output = tool.format_output()
        assert 'NSID' in output
        assert 'RBD Image' in output

    def test_no_header(self, io_collector):
        tool = NVMeoFTopIO({**self.default_args, 'no_header': True}, io_collector)
        output = tool.format_output()
        assert 'NSID' not in output

    def test_namespace_data(self, io_collector):
        io_collector.get_sorted_namespaces.return_value = [
            (1, 'pool/image1', 100, 50, '1.00', '0.50', '4.00',
             50, '1.00', '0.50', '4.00', '1', 'No'),
            (2, 'pool/image2', 200, 100, '2.00', '1.00', '8.00',
             100, '2.00', '1.00', '8.00', '2', 'Yes'),
        ]
        tool = NVMeoFTopIO(self.default_args, io_collector)
        output = tool.format_output()
        assert 'pool/image1' in output
        assert 'pool/image2' in output

    def test_with_timestamp(self, io_collector):
        tool = NVMeoFTopIO({**self.default_args, 'with_timestamp': True}, io_collector)
        output = tool.format_output()
        assert 'delay:' in output

    def test_invalid_sort_key(self, io_collector):
        tool = NVMeoFTopIO({**self.default_args, 'sort_by': 'BadKey'}, io_collector)
        with pytest.raises(ValueError, match="Invalid sort key"):
            tool.format_output()


class TestNvmeofTopCollector:
    @pytest.fixture
    def collector(self):
        c = NvmeofTopCollector()
        c.client = MagicMock()
        c.client.service_name = 'myservice'
        c.tool = MagicMock()
        c.tool.args = {'group': '', 'server_addr': ''}
        return c

    def test_grpc_call_failure(self, collector):
        collector.client.gateway_addr = '192.168.1.1:5500'
        collector.client.stub.get_gateway_info.side_effect = Exception('connection refused')
        collector._call_grpc(  # pylint: disable=protected-access
            'get_gateway_info', MagicMock(), collector.client)
        assert collector.health.rc == -errno.ECONNREFUSED
        assert collector.health.msg == 'RPC endpoint unavailable at 192.168.1.1:5500'

    def test_collect_cpu_data_service_not_found(self, collector):
        collector.tool.service_name = 'myservice'
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value={'gateways': {}}):
            collector.collect_cpu_data()
        assert collector.health.rc == -errno.ENOENT
        assert collector.health.msg == 'Service myservice not found'

    def test_collect_io_data_subsystems_unavailable(self, collector):
        collector.tool.subsystem_nqn = 'nqn.test'
        with patch.object(collector, '_fetch_subsystems', return_value=None):
            collector.collect_io_data()
        assert collector.health.rc == -errno.ECONNREFUSED
        assert collector.health.msg == 'Unable to retrieve a list of subsystems'

    def test_collect_io_data_no_subsystems(self, collector):
        collector.tool.subsystem_nqn = ''
        mock_subsystems = MagicMock()
        mock_subsystems.status = 0
        mock_subsystems.subsystems = []
        with patch.object(collector, '_fetch_subsystems', return_value=mock_subsystems):
            collector.collect_io_data()
        assert collector.health.rc == -errno.ENOENT
        assert collector.health.msg == 'No subsystems found'

    def test_collect_io_data_nqn_not_found(self, collector):
        collector.tool.subsystem_nqn = 'nqn.test'
        mock_subsystems = MagicMock()
        mock_subsystems.status = 0
        mock_sub = MagicMock()
        mock_sub.nqn = 'nqn.other'
        mock_subsystems.subsystems = [mock_sub]
        with patch.object(collector, '_fetch_subsystems', return_value=mock_subsystems):
            collector.collect_io_data()
        assert collector.health.rc == -errno.ENOENT
        assert collector.health.msg == 'Subsystem NQN provided not found'

    def test_collect_io_data_service_not_found(self, collector):
        collector.tool.subsystem_nqn = 'nqn.test'
        mock_subsystems = MagicMock()
        mock_subsystems.status = 0
        mock_sub = MagicMock()
        mock_sub.nqn = 'nqn.test'
        mock_subsystems.subsystems = [mock_sub]
        mock_namespace_info = MagicMock()
        mock_namespace_info.namespaces = []
        with patch.object(collector, '_fetch_subsystems', return_value=mock_subsystems), \
             patch.object(collector, '_fetch_namespaces', return_value=mock_namespace_info), \
             patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value={'gateways': {}}):
            collector.collect_io_data()
        assert collector.health.rc == -errno.ENOENT
        assert collector.health.msg == 'Service myservice not found'

    def test_collect_io_data_lbg_mapping_failed(self, collector):
        collector.tool.subsystem_nqn = 'nqn.test'
        mock_subsystems = MagicMock()
        mock_subsystems.status = 0
        mock_sub = MagicMock()
        mock_sub.nqn = 'nqn.test'
        mock_subsystems.subsystems = [mock_sub]
        mock_namespace_info = MagicMock()
        mock_namespace_info.namespaces = []
        with patch.object(collector, '_fetch_subsystems', return_value=mock_subsystems), \
             patch.object(collector, '_fetch_namespaces', return_value=mock_namespace_info), \
             patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value={'gateways': {'myservice': []}}), \
             patch('dashboard.services.nvmeof_top_cli.get_lbg_gws_map', return_value={}):
            collector.collect_io_data()
        assert collector.health.rc == -errno.ENOENT
        assert collector.health.msg == \
            'Failed to retrieve load balancing group mapping for service myservice'


class TestNvmeofTopCommands(CLICommandTestMixin):
    @classmethod
    def exec_nvmeof_cmd(cls, cmd, **kwargs):
        return cls.exec_cmd('', prefix=cmd, **kwargs)

    def test_top_io_missing_subsystem_returns_einval(self):
        with pytest.raises(CmdException) as exc_info:
            self.exec_nvmeof_cmd('nvmeof top io', subsystem='', session_id='sess1')
        assert exc_info.value.retcode == -errno.EINVAL
        assert str(exc_info.value) == "Required argument '--subsystem' missing"

    def test_top_cpu_success(self):
        with patch('dashboard.services.nvmeof_top_cli.get_collector') as mock_gc:
            mock_gc.return_value = MagicMock()
            with patch.object(NVMeoFTopCPU, 'run', return_value=(0, 'cpu output\n')):
                result = self.exec_nvmeof_cmd('nvmeof top cpu', session_id='sess1')
                assert 'cpu output' in result
                mock_gc.assert_called_once_with('sess1')

    def test_top_cpu_run_fail(self):
        with patch('dashboard.services.nvmeof_top_cli.get_collector') as mock_gc:
            mock_gc.return_value = MagicMock()
            with patch.object(NVMeoFTopCPU, 'run',
                              return_value=(-errno.ENOENT, 'error')):
                with pytest.raises(CmdException) as exc_info:
                    self.exec_nvmeof_cmd('nvmeof top cpu', session_id='sess1')
                assert exc_info.value.retcode == -errno.ENOENT
                assert str(exc_info.value) == 'error'

    def test_top_io_success(self):
        with patch('dashboard.services.nvmeof_top_cli.get_collector') as mock_gc:
            mock_gc.return_value = MagicMock()
            with patch.object(NVMeoFTopIO, 'run', return_value=(0, 'io output\n')):
                result = self.exec_nvmeof_cmd(
                    'nvmeof top io',
                    subsystem='nqn.test',
                    session_id='sess2'
                )
                assert 'io output' in result
                mock_gc.assert_called_once_with('sess2')

    def test_top_cpu_get_collector_fail(self):
        with patch('dashboard.services.nvmeof_top_cli.get_collector',
                   side_effect=RuntimeError("boom")):
            with pytest.raises(CmdException) as exc_info:
                self.exec_nvmeof_cmd('nvmeof top cpu', session_id='sess1')
            assert exc_info.value.retcode == -errno.EINVAL
            assert str(exc_info.value) == 'boom'

    def test_top_io_get_collector_fail(self):
        with patch('dashboard.services.nvmeof_top_cli.get_collector',
                   side_effect=RuntimeError("boom")):
            with pytest.raises(CmdException) as exc_info:
                self.exec_nvmeof_cmd(
                    'nvmeof top io',
                    subsystem='nqn.test',
                    session_id='sess2'
                )
            assert exc_info.value.retcode == -errno.EINVAL
            assert str(exc_info.value) == 'boom'
