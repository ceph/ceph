# -*- coding: utf-8 -*-
import errno
import time
from unittest.mock import MagicMock, patch

import pytest

from ..services.nvmeof_top_cli import MAX_SESSION_TTL, Counter, \
    NvmeofTopCollector, NVMeoFTopCPU, NVMeoFTopIO
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
    collector.get_gateway_summary_data.return_value = []
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
        'server_address': '',
        'server_port': None,
        'gw_group': '',
        'period': 1,
        'session_id': None,
    }

    def test_headers(self, cpu_collector):
        tool = NVMeoFTopCPU(self.default_args)
        tool.collector = cpu_collector
        output = tool.format_output()
        assert 'Gateway' in output
        assert 'Thread Name' in output
        assert 'Busy Rate%' in output
        assert 'Idle Rate%' in output

    def test_no_header(self, cpu_collector):
        tool = NVMeoFTopCPU({**self.default_args, 'no_header': True})
        tool.collector = cpu_collector
        output = tool.format_output()
        assert 'Gateway' not in output

    def test_with_timestamp(self, cpu_collector):
        tool = NVMeoFTopCPU({**self.default_args, 'with_timestamp': True})
        tool.collector = cpu_collector
        output = tool.format_output()
        assert 'delay:' in output
        assert '1.50s' in output

    def test_reactor_data(self, cpu_collector):
        cpu_collector.get_reactor_data.return_value = [
            ('192.168.1.1:5500', 'reactor_0', 72.50, 27.50),
            ('192.168.1.1:5500', 'reactor_1', 45.00, 55.00),
        ]
        tool = NVMeoFTopCPU(self.default_args)
        tool.collector = cpu_collector
        output = tool.format_output()
        assert '192.168.1.1:5500' in output
        assert 'reactor_0' in output
        assert 'reactor_1' in output

    def test_invalid_sort_key(self, cpu_collector):
        tool = NVMeoFTopCPU({**self.default_args, 'sort_by': 'NonExistent'})
        tool.collector = cpu_collector
        with pytest.raises(ValueError, match="Invalid sort key"):
            tool.format_output()


class TestNVMeoFTopIOFormat:
    default_args = {
        'sort_by': 'NSID',
        'sort_descending': False,
        'with_timestamp': False,
        'no_header': False,
        'summary': False,
        'nqn': 'nqn.2024-01.io.spdk:cnode1',
        'server_address': '',
        'server_port': None,
        'gw_group': '',
        'period': 1,
        'session_id': None,
    }

    def test_no_namespaces(self, io_collector):
        tool = NVMeoFTopIO(self.default_args)
        tool.collector = io_collector
        output = tool.format_output()
        assert '<no namespaces defined>' in output

    def test_headers_present(self, io_collector):
        tool = NVMeoFTopIO(self.default_args)
        tool.collector = io_collector
        output = tool.format_output()
        assert 'NSID' in output
        assert 'RBD Image' in output

    def test_no_header(self, io_collector):
        tool = NVMeoFTopIO({**self.default_args, 'no_header': True})
        tool.collector = io_collector
        output = tool.format_output()
        assert 'NSID' not in output

    def test_namespace_data(self, io_collector):
        io_collector.get_sorted_namespaces.return_value = [
            (1, 'pool/image1', 100, 50, '1.00', '0.50', '4.00',
             50, '1.00', '0.50', '4.00', '1', 'No'),
            (2, 'pool/image2', 200, 100, '2.00', '1.00', '8.00',
             100, '2.00', '1.00', '8.00', '2', 'Yes'),
        ]
        tool = NVMeoFTopIO(self.default_args)
        tool.collector = io_collector
        output = tool.format_output()
        assert 'pool/image1' in output
        assert 'pool/image2' in output

    def test_with_timestamp(self, io_collector):
        tool = NVMeoFTopIO({**self.default_args, 'with_timestamp': True})
        tool.collector = io_collector
        output = tool.format_output()
        assert 'delay:' in output

    def test_invalid_sort_key(self, io_collector):
        tool = NVMeoFTopIO({**self.default_args, 'sort_by': 'BadKey'})
        tool.collector = io_collector
        with pytest.raises(ValueError, match="Invalid sort key"):
            tool.format_output()


class TestNvmeofTopCollector:
    @pytest.fixture
    def collector(self):
        c = NvmeofTopCollector()
        c.client = MagicMock()
        c.service = 'myservice'
        c.group = 'mygroup'
        c.tool = MagicMock()
        c.tool.args = {'gw_group': '', 'server_address': ''}
        return c

    def test_grpc_call_failure(self, collector):
        collector.client.gateway_addr = '192.168.1.1:5500'
        collector.client.stub.get_gateway_info.side_effect = Exception('connection refused')
        collector._call_grpc(  # pylint: disable=protected-access
            'get_gateway_info', MagicMock(), collector.client)
        assert collector.health.rc == -errno.ECONNREFUSED
        assert collector.health.msg == 'RPC endpoint unavailable at 192.168.1.1:5500'

    def test_set_gateways_no_services(self, collector):
        collector.service = ''
        collector.group = ''
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value={'gateways': {}}):
            collector._set_gateways('', '')  # pylint: disable=protected-access
        assert collector.health.rc == -errno.ENOENT
        assert collector.health.msg == 'No NVMeoF gateways configured'

    def test_set_gateways_multiple_groups_no_filter(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '1.1.1.1:5500', 'group': 'group1'}],
            'nvmeof.pool.group2': [{'service_url': '2.2.2.2:5500', 'group': 'group2'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config):
            collector._set_gateways('', '')  # pylint: disable=protected-access
        assert collector.health.rc == -errno.EINVAL
        assert 'Multiple gateway groups found' in collector.health.msg

    def test_set_gateways_address_not_found(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '1.1.1.1:5500', 'group': 'group1'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config):
            collector._set_gateways('', '9.9.9.9')  # pylint: disable=protected-access
        assert collector.health.rc == -errno.ENOENT
        assert 'No gateway found matching address' in collector.health.msg

    def test_set_gateways_group_not_found(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '1.1.1.1:5500', 'group': 'group1'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config):
            collector._set_gateways('nonexistent', '')  # pylint: disable=protected-access
        assert collector.health.rc == -errno.ENOENT
        assert "Gateway group 'nonexistent' not found" in collector.health.msg

    def test_set_gateways_address_group_mismatch(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '1.1.1.1:5500', 'group': 'group1'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config):
            collector._set_gateways('group2', '1.1.1.1')  # pylint: disable=protected-access
        assert collector.health.rc == -errno.EINVAL
        assert "Address '1.1.1.1' belongs to group 'group1', not 'group2'" in collector.health.msg

    def test_set_gateways_single_service_auto_detect(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '1.1.1.1:5500', 'group': 'group1'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config), \
             patch.object(collector, '_get_client'):
            collector._set_gateways('', '')  # pylint: disable=protected-access
        assert collector.service == 'nvmeof.pool.group1'
        assert collector.group == 'group1'

    def test_set_gateways_by_group(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '1.1.1.1:5500', 'group': 'group1'}],
            'nvmeof.pool.group2': [{'service_url': '2.2.2.2:5500', 'group': 'group2'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config), \
             patch.object(collector, '_get_client'):
            collector._set_gateways('group1', '')  # pylint: disable=protected-access
        assert collector.service == 'nvmeof.pool.group1'
        assert collector.group == 'group1'

    def test_set_gateways_by_address(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '1.1.1.1:5500', 'group': 'group1'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config), \
             patch.object(collector, '_get_client'):
            collector._set_gateways('', '1.1.1.1')  # pylint: disable=protected-access
        assert collector.service == 'nvmeof.pool.group1'
        assert collector.group == 'group1'

    def test_set_gateways_by_address_no_substring_match(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '1.1.1.11:5500', 'group': 'group1'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config):
            collector._set_gateways('', '1.1.1.1')  # pylint: disable=protected-access
        assert collector.health.rc == -errno.ENOENT

    def test_set_gateways_by_address_and_port(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '1.1.1.1:5500', 'group': 'group1'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config), \
             patch.object(collector, '_get_client'):
            collector._set_gateways('', '1.1.1.1', 5500)  # pylint: disable=protected-access
        assert collector.service == 'nvmeof.pool.group1'

    def test_set_gateways_port_mismatch(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '1.1.1.1:5500', 'group': 'group1'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config):
            collector._set_gateways('', '1.1.1.1', 9999)  # pylint: disable=protected-access
        assert collector.health.rc == -errno.ENOENT

    def test_set_gateways_ipv6_address(self, collector):
        collector.service = ''
        collector.group = ''
        config = {'gateways': {
            'nvmeof.pool.group1': [{'service_url': '[::1]:5500', 'group': 'group1'}],
        }}
        with patch('dashboard.services.nvmeof_top_cli.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=config), \
             patch.object(collector, '_get_client'):
            collector._set_gateways('', '::1')  # pylint: disable=protected-access
        assert collector.service == 'nvmeof.pool.group1'

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
             patch('dashboard.services.nvmeof_top_cli.get_lbg_gws_map', return_value={}):
            collector.collect_io_data()
        assert collector.health.rc == -errno.ENOENT
        assert collector.health.msg == \
            'Failed to retrieve load balancing group mapping for service myservice'


class TestNvmeofTopValidateArgs:
    base_args = {
        'sort_by': 'Thread Name',
        'sort_descending': False,
        'with_timestamp': False,
        'no_header': False,
        'server_address': '',
        'server_port': None,
        'gw_group': '',
        'period': 1,
        'session_id': None,
    }

    def _validate(self, **overrides):
        args = {**self.base_args, **overrides}
        return NVMeoFTopCPU(args)._validate_args()  # pylint: disable=protected-access

    def test_valid_period(self):
        assert self._validate(period=5) is None

    def test_invalid_period_too_low(self):
        rc, msg = self._validate(period=0)
        assert rc == -errno.EINVAL
        assert msg == f"Invalid period '0': must be between 1 and {MAX_SESSION_TTL}"

    def test_invalid_period_too_high(self):
        rc, msg = self._validate(period=999999)
        assert rc == -errno.EINVAL
        assert msg == f"Invalid period '999999': must be between 1 and {MAX_SESSION_TTL}"

    def test_valid_server_address(self):
        assert self._validate(server_address='1.2.3.4') is None

    def test_invalid_server_address(self):
        rc, msg = self._validate(server_address='not-an-ip')
        assert rc == -errno.EINVAL
        assert msg == "Invalid server-address 'not-an-ip': must be a valid IP address"

    def test_valid_server_address_ipv6(self):
        assert self._validate(server_address='::1') is None

    def test_valid_server_port(self):
        assert self._validate(server_address='1.2.3.4', server_port=5500) is None

    def test_invalid_server_port_zero(self):
        rc, msg = self._validate(server_address='1.2.3.4', server_port=0)
        assert rc == -errno.EINVAL
        assert msg == "Invalid server-port '0': must be between 1 and 65535"

    def test_invalid_server_port_too_high(self):
        rc, msg = self._validate(server_address='1.2.3.4', server_port=65536)
        assert rc == -errno.EINVAL
        assert msg == "Invalid server-port '65536': must be between 1 and 65535"

    def test_port_without_address_is_valid(self):
        assert self._validate(server_port=5500) is None


class TestNvmeofTopCommands(CLICommandTestMixin):
    @classmethod
    def exec_nvmeof_cmd(cls, cmd, **kwargs):
        return cls.exec_cmd('', prefix=cmd, **kwargs)

    def test_top_io_missing_nqn_returns_einval(self):
        with pytest.raises(CmdException) as exc_info:
            self.exec_nvmeof_cmd('nvmeof top io', nqn='', session_id='sess1')
        assert exc_info.value.retcode == -errno.EINVAL
        assert str(exc_info.value) == "Required argument '--nqn' missing"

    def test_top_cpu_success(self):
        with patch.object(NVMeoFTopCPU, 'run', return_value=(0, 'cpu output\n')):
            result = self.exec_nvmeof_cmd('nvmeof top cpu', session_id='sess1')
            assert 'cpu output' in result

    def test_top_cpu_run_fail(self):
        with patch.object(NVMeoFTopCPU, 'run',
                          return_value=(-errno.ENOENT, 'error')):
            with pytest.raises(CmdException) as exc_info:
                self.exec_nvmeof_cmd('nvmeof top cpu', session_id='sess1')
            assert exc_info.value.retcode == -errno.ENOENT
            assert str(exc_info.value) == 'error'

    def test_top_io_success(self):
        with patch.object(NVMeoFTopIO, 'run', return_value=(0, 'io output\n')):
            result = self.exec_nvmeof_cmd(
                'nvmeof top io',
                nqn='nqn.test',
                session_id='sess2'
            )
            assert 'io output' in result

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
                    nqn='nqn.test',
                    session_id='sess2'
                )
            assert exc_info.value.retcode == -errno.EINVAL
            assert str(exc_info.value) == 'boom'
