import json
import os

from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from ..constants import DEFAULT_SNMP_GATEWAY_IMAGE
from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer
from ..context import CephadmContext
from ..context_getters import fetch_configs, fetch_endpoints
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..data_utils import is_fsid
from ..deployment_utils import to_deployment_container
from ..exceptions import Error
from ..file_utils import write_new


@register_daemon_form
class SNMPGateway(ContainerDaemonForm):
    """Defines an SNMP gateway between Prometheus and SNMP monitoring Frameworks"""

    daemon_type = 'snmp-gateway'
    SUPPORTED_VERSIONS = ['V2c', 'V3']
    default_image = DEFAULT_SNMP_GATEWAY_IMAGE
    DEFAULT_PORT = 9464
    env_filename = 'snmp-gateway.conf'

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        fsid: str,
        daemon_id: Union[int, str],
        config_json: Dict[str, Any],
        image: Optional[str] = None,
    ) -> None:
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image or SNMPGateway.default_image

        self.uid = config_json.get('uid', 0)
        self.gid = config_json.get('gid', 0)

        self.destination = config_json.get('destination', '')
        self.snmp_version = config_json.get('snmp_version', 'V2c')
        self.snmp_community = config_json.get('snmp_community', 'public')
        self.log_level = config_json.get('log_level', 'info')
        self.snmp_v3_auth_username = config_json.get(
            'snmp_v3_auth_username', ''
        )
        self.snmp_v3_auth_password = config_json.get(
            'snmp_v3_auth_password', ''
        )
        self.snmp_v3_auth_protocol = config_json.get(
            'snmp_v3_auth_protocol', ''
        )
        self.snmp_v3_priv_protocol = config_json.get(
            'snmp_v3_priv_protocol', ''
        )
        self.snmp_v3_priv_password = config_json.get(
            'snmp_v3_priv_password', ''
        )
        self.snmp_v3_engine_id = config_json.get('snmp_v3_engine_id', '')

        self.validate()

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: Union[int, str]
    ) -> 'SNMPGateway':
        cfgs = fetch_configs(ctx)
        assert cfgs  # assert some config data was found
        return cls(ctx, fsid, daemon_id, cfgs, ctx.image)

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'SNMPGateway':
        return cls.init(ctx, ident.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

    @staticmethod
    def get_version(
        ctx: CephadmContext, fsid: str, daemon_id: str
    ) -> Optional[str]:
        """Return the version of the notifier from it's http endpoint"""
        path = os.path.join(
            ctx.data_dir, fsid, f'snmp-gateway.{daemon_id}', 'unit.meta'
        )
        try:
            with open(path, 'r') as env:
                metadata = json.loads(env.read())
        except (OSError, json.JSONDecodeError):
            return None

        ports = metadata.get('ports', [])
        if not ports:
            return None

        try:
            with urlopen(f'http://127.0.0.1:{ports[0]}/') as r:
                html = r.read().decode('utf-8').split('\n')
        except (HTTPError, URLError):
            return None

        for h in html:
            stripped = h.strip()
            if stripped.startswith(('<pre>', '<PRE>')) and stripped.endswith(
                ('</pre>', '</PRE>')
            ):
                # <pre>(version=1.2.1, branch=HEAD, revision=7...
                return stripped.split(',')[0].split('version=')[1]

        return None

    @property
    def port(self) -> int:
        endpoints = fetch_endpoints(self.ctx)
        if not endpoints:
            return self.DEFAULT_PORT
        return endpoints[0].port

    def get_daemon_args(self) -> List[str]:
        v3_args = []
        base_args = [
            f'--web.listen-address=:{self.port}',
            f'--snmp.destination={self.destination}',
            f'--snmp.version={self.snmp_version}',
            f'--log.level={self.log_level}',
            '--snmp.trap-description-template=/etc/snmp_notifier/description-template.tpl',
        ]

        if self.snmp_version == 'V3':
            # common auth settings
            v3_args.extend(
                [
                    '--snmp.authentication-enabled',
                    f'--snmp.authentication-protocol={self.snmp_v3_auth_protocol}',
                    f'--snmp.security-engine-id={self.snmp_v3_engine_id}',
                ]
            )
            # authPriv setting is applied if we have a privacy protocol setting
            if self.snmp_v3_priv_protocol:
                v3_args.extend(
                    [
                        '--snmp.private-enabled',
                        f'--snmp.private-protocol={self.snmp_v3_priv_protocol}',
                    ]
                )

        return base_args + v3_args

    @property
    def data_dir(self) -> str:
        return os.path.join(
            self.ctx.data_dir,
            self.ctx.fsid,
            f'{self.daemon_type}.{self.daemon_id}',
        )

    @property
    def conf_file_path(self) -> str:
        return os.path.join(self.data_dir, self.env_filename)

    def create_daemon_conf(self) -> None:
        """Creates the environment file holding 'secrets' passed to the snmp-notifier daemon"""
        with write_new(self.conf_file_path) as f:
            if self.snmp_version == 'V2c':
                f.write(f'SNMP_NOTIFIER_COMMUNITY={self.snmp_community}\n')
            else:
                f.write(
                    f'SNMP_NOTIFIER_AUTH_USERNAME={self.snmp_v3_auth_username}\n'
                )
                f.write(
                    f'SNMP_NOTIFIER_AUTH_PASSWORD={self.snmp_v3_auth_password}\n'
                )
                if self.snmp_v3_priv_password:
                    f.write(
                        f'SNMP_NOTIFIER_PRIV_PASSWORD={self.snmp_v3_priv_password}\n'
                    )

    def validate(self) -> None:
        """Validate the settings

        Raises:
            Error: if the fsid doesn't look like an fsid
            Error: if the snmp version is not supported
            Error: destination IP and port address missing
        """
        if not is_fsid(self.fsid):
            raise Error(f'not a valid fsid: {self.fsid}')

        if self.snmp_version not in SNMPGateway.SUPPORTED_VERSIONS:
            raise Error(f'not a valid snmp version: {self.snmp_version}')

        if not self.destination:
            raise Error(
                'config is missing destination attribute(<ip>:<port>) of the target SNMP listener'
            )

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return self.uid, self.gid

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.append(f'--env-file={self.conf_file_path}')

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(self.get_daemon_args())
