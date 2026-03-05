
import pytest
from unittest.mock import patch

from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import RGWSpec
from cephadm.tests.fixtures import with_host, with_service, _run_cephadm


class TestRGWService:

    @pytest.mark.parametrize(
        "frontend, ssl, extra_args, expected",
        [
            ('beast', False, ['tcp_nodelay=1'],
             'beast endpoint=[fd00:fd00:fd00:3000::1]:80 tcp_nodelay=1'),
            ('beast', True, ['tcp_nodelay=0', 'max_header_size=65536'],
             'beast ssl_endpoint=[fd00:fd00:fd00:3000::1]:443 ssl_certificate=config://rgw/cert/rgw.foo tcp_nodelay=0 max_header_size=65536'),
            ('civetweb', False, [], 'civetweb port=[fd00:fd00:fd00:3000::1]:80'),
            ('civetweb', True, None,
             'civetweb port=[fd00:fd00:fd00:3000::1]:443s ssl_certificate=config://rgw/cert/rgw.foo'),
        ]
    )
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_rgw_update(self, frontend, ssl, extra_args, expected, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'host1'):
            cephadm_module.cache.update_host_networks('host1', {
                'fd00:fd00:fd00:3000::/64': {
                    'if0': ['fd00:fd00:fd00:3000::1']
                }
            })
            s = RGWSpec(service_id="foo",
                        networks=['fd00:fd00:fd00:3000::/64'],
                        ssl=ssl,
                        rgw_frontend_type=frontend,
                        rgw_frontend_extra_args=extra_args)
            with with_service(cephadm_module, s) as dds:
                _, f, _ = cephadm_module.check_mon_command({
                    'prefix': 'config get',
                    'who': f'client.{dds[0]}',
                    'key': 'rgw_frontends',
                })
                assert f == expected

    @pytest.mark.parametrize(
        "disable_sync_traffic",
        [
            (True),
            (False),
        ]
    )
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_rgw_disable_sync_traffic(self, disable_sync_traffic, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'host1'):
            s = RGWSpec(service_id="foo",
                        disable_multisite_sync_traffic=disable_sync_traffic)
            with with_service(cephadm_module, s) as dds:
                _, f, _ = cephadm_module.check_mon_command({
                    'prefix': 'config get',
                    'who': f'client.{dds[0]}',
                    'key': 'rgw_run_sync_thread',
                })
                assert f == ('false' if disable_sync_traffic else 'true')
