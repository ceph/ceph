import logging
import random
import string
from typing import List, cast, Tuple, Dict, Any

from ceph.deployment.service_spec import HaproxySpec

from .cephadmservice import CephadmDaemonDeploySpec, CephService
from ..utils import resolve_ip

logger = logging.getLogger(__name__)


class HaproxyService(CephService):
    TYPE = 'haproxy'

    def prepare_create(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> CephadmDaemonDeploySpec:
        assert daemon_spec.daemon_type == 'haproxy'

        daemon_id = daemon_spec.daemon_id
        host = daemon_spec.host
        spec = cast(HaproxySpec, self.mgr.spec_store[daemon_spec.service_name].spec)

        logger.info('Create daemon %s on host %s with spec %s' % (
            daemon_id, host, spec))

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec

    def generate_config(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> Tuple[Dict[str, Any], List[str]]:
        spec = cast(HaproxySpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        assert spec.backend_service
        daemons = self.mgr.cache.get_daemons_by_service(spec.backend_service)
        deps = [d.name() for d in daemons]

        # generate password?
        pw_key = f'{spec.service_name()}/monitor_password'
        password = self.mgr.get_store(pw_key)
        if password is None:
            password = ''.join(random.choice(string.ascii_lowercase) for _ in range(20))
            self.mgr.set_store(pw_key, password)
        else:
            if spec.monitor_password:
                self.mgr.set_store(pw_key, None)

        haproxy_conf = self.mgr.template.render(
            'services/haproxy/haproxy.cfg.j2',
            {
                'spec': spec,
                'servers': [
                    {
                        'name': d.name(),
                        'ip': d.ip or resolve_ip(str(d.hostname)),
                        'port': d.ports[0],
                    } for d in daemons if d.ports
                ],
                'user': spec.monitor_user or 'admin',
                'password': password,
                'ip': daemon_spec.ip or '*',
                'frontend_port': daemon_spec.ports[0] if daemon_spec.ports else spec.frontend_port,
                'monitor_port': daemon_spec.ports[1] if daemon_spec.ports else spec.monitor_port,
            }
        )
        config_files = {
            'files': {
                "haproxy.cfg": haproxy_conf,
            }
        }
        if spec.ssl_cert:
            ssl_cert = spec.ssl_cert
            if isinstance(ssl_cert, list):
                ssl_cert = '\n'.join(ssl_cert)
            config_files['files']['haproxy.pem'] = ssl_cert

        return config_files, sorted(deps)
