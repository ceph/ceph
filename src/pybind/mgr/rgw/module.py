import json
import threading
import yaml
import errno
import base64
import functools
import sys

from mgr_module import MgrModule, CLICommand, HandleCommandResult, Option
import orchestrator

from ceph.deployment.service_spec import RGWSpec, PlacementSpec, SpecValidationError
from typing import Any, Optional, Sequence, Iterator, List, Callable, TypeVar, cast, Dict, Tuple, Union, TYPE_CHECKING

from ceph.rgw.types import RGWAMException, RGWAMEnvMgr, RealmToken
from ceph.rgw.rgwam_core import EnvArgs, RGWAM
from orchestrator import OrchestratorClientMixin, OrchestratorError, DaemonDescription, OrchResult


FuncT = TypeVar('FuncT', bound=Callable[..., Any])

if TYPE_CHECKING:
    # this uses a version check as opposed to a try/except because this
    # form makes mypy happy and try/except doesn't.
    if sys.version_info >= (3, 8):
        from typing import Protocol
    else:
        from typing_extensions import Protocol

    class MgrModuleProtocol(Protocol):
        def tool_exec(self, args: List[str], timeout: int = 10, stdin: Optional[bytes] = None) -> Tuple[int, str, str]:
            ...

        def apply_rgw(self, spec: RGWSpec) -> OrchResult[str]:
            ...

        def list_daemons(self, service_name: Optional[str] = None,
                         daemon_type: Optional[str] = None,
                         daemon_id: Optional[str] = None,
                         host: Optional[str] = None,
                         refresh: bool = False) -> OrchResult[List['DaemonDescription']]:
            ...
else:
    class MgrModuleProtocol:
        pass


class RGWSpecParsingError(Exception):
    pass


class OrchestratorAPI(OrchestratorClientMixin):
    def __init__(self, mgr: MgrModule):
        super(OrchestratorAPI, self).__init__()
        self.set_mgr(mgr)

    def status(self) -> Dict[str, Union[str, bool]]:
        try:
            status, message, _module_details = super().available()
            return dict(available=status, message=message)
        except (RuntimeError, OrchestratorError, ImportError) as e:
            return dict(available=False, message=f'Orchestrator is unavailable: {e}')


class RGWAMOrchMgr(RGWAMEnvMgr):
    def __init__(self, mgr: MgrModuleProtocol):
        self.mgr = mgr

    def tool_exec(self, prog: str, args: List[str], stdin: Optional[bytes] = None) -> Tuple[List[str], int, str, str]:
        cmd = [prog] + args
        rc, stdout, stderr = self.mgr.tool_exec(args=cmd, stdin=stdin)
        return cmd, rc, stdout, stderr

    def apply_rgw(self, spec: RGWSpec) -> None:
        completion = self.mgr.apply_rgw(spec)
        orchestrator.raise_if_exception(completion)

    def list_daemons(self, service_name: Optional[str] = None,
                     daemon_type: Optional[str] = None,
                     daemon_id: Optional[str] = None,
                     host: Optional[str] = None,
                     refresh: bool = True) -> List['DaemonDescription']:
        completion = self.mgr.list_daemons(service_name,
                                           daemon_type,
                                           daemon_id=daemon_id,
                                           host=host,
                                           refresh=refresh)
        return orchestrator.raise_if_exception(completion)


def check_orchestrator(func: FuncT) -> FuncT:
    @functools.wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> HandleCommandResult:
        available = self.api.status()['available']
        if available:
            return func(self, *args, **kwargs)
        else:
            err_msg = "Cephadm is not available. Please enable cephadm by 'ceph mgr module enable cephadm'."
            return HandleCommandResult(retval=-errno.EINVAL, stdout='', stderr=err_msg)
    return cast(FuncT, wrapper)


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS: List[Option] = []

    # These are "native" Ceph options that this module cares about.
    NATIVE_OPTIONS: List[Option] = []

    def __init__(self, *args: Any, **kwargs: Any):
        self.inited = False
        self.lock = threading.Lock()
        super(Module, self).__init__(*args, **kwargs)
        self.api = OrchestratorAPI(self)

        # ensure config options members are initialized; see config_notify()
        self.config_notify()

        with self.lock:
            self.inited = True
            self.env = EnvArgs(RGWAMOrchMgr(self))

        # set up some members to enable the serve() method and shutdown()
        self.run = True
        self.event = threading.Event()

    def config_notify(self) -> None:
        """
        This method is called whenever one of our config options is changed.
        """
        # This is some boilerplate that stores MODULE_OPTIONS in a class
        # member, so that, for instance, the 'emphatic' option is always
        # available as 'self.emphatic'.
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))
        # Do the same for the native options.
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,  # type: ignore
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))  # type: ignore

    @CLICommand('rgw admin', perm='rw')
    def _cmd_rgw_admin(self, params: Sequence[str]) -> HandleCommandResult:
        """rgw admin"""
        cmd, returncode, out, err = self.env.mgr.tool_exec('radosgw-admin', params or [])

        self.log.error('retcode=%d' % returncode)
        self.log.error('out=%s' % out)
        self.log.error('err=%s' % err)

        return HandleCommandResult(retval=returncode, stdout=out, stderr=err)

    @CLICommand('rgw realm bootstrap', perm='rw')
    @check_orchestrator
    def _cmd_rgw_realm_bootstrap(self,
                                 realm_name: Optional[str] = None,
                                 zonegroup_name: Optional[str] = None,
                                 zone_name: Optional[str] = None,
                                 port: Optional[int] = None,
                                 placement: Optional[str] = None,
                                 zone_endpoints: Optional[str] = None,
                                 start_radosgw: Optional[bool] = True,
                                 inbuf: Optional[str] = None) -> HandleCommandResult:
        """Bootstrap new rgw realm, zonegroup, and zone"""

        if inbuf:
            try:
                rgw_specs = self._parse_rgw_specs(inbuf)
            except RGWSpecParsingError as e:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'{e}')
        elif (realm_name and zonegroup_name and zone_name):
            placement_spec = PlacementSpec.from_string(placement) if placement else None
            rgw_specs = [RGWSpec(rgw_realm=realm_name,
                                 rgw_zonegroup=zonegroup_name,
                                 rgw_zone=zone_name,
                                 rgw_frontend_port=port,
                                 placement=placement_spec,
                                 zone_endpoints=zone_endpoints)]
        else:
            err_msg = 'Invalid arguments: either pass a spec with -i or provide the realm, zonegroup and zone.'
            return HandleCommandResult(retval=-errno.EINVAL, stdout='', stderr=err_msg)

        try:
            for spec in rgw_specs:
                RGWAM(self.env).realm_bootstrap(spec, start_radosgw)
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return HandleCommandResult(retval=e.retcode, stdout=e.stdout, stderr=e.stderr)

        return HandleCommandResult(retval=0, stdout="Realm(s) created correctly. Please, use 'ceph rgw realm tokens' to get the token.", stderr='')

    def _parse_rgw_specs(self, inbuf: str) -> List[RGWSpec]:
        """Parse RGW specs from a YAML file."""
        # YAML '---' document separator with no content generates
        # None entries in the output. Let's skip them silently.
        yaml_objs: Iterator = yaml.safe_load_all(inbuf)
        specs = [o for o in yaml_objs if o is not None]
        rgw_specs = []
        for spec in specs:
            # A secondary zone spec normally contains only the zone and the reaml token
            # since no rgw_realm is specified in this case we extract it from the token
            if 'rgw_realm_token' in spec:
                realm_token = RealmToken.from_base64_str(spec['rgw_realm_token'])
                if realm_token is None:
                    raise RGWSpecParsingError(f"Invalid realm token: {spec['rgw_realm_token']}")
                spec['rgw_realm'] = realm_token.realm_name

            try:
                rgw_spec = RGWSpec.from_json(spec)
                rgw_spec.validate()
            except SpecValidationError as e:
                raise RGWSpecParsingError(f'RGW Spec parsing/validation error: {e}')
            else:
                rgw_specs.append(rgw_spec)

        return rgw_specs

    @CLICommand('rgw realm zone-creds create', perm='rw')
    def _cmd_rgw_realm_new_zone_creds(self,
                                      realm_name: Optional[str] = None,
                                      endpoints: Optional[str] = None,
                                      sys_uid: Optional[str] = None) -> HandleCommandResult:
        """Create credentials for new zone creation"""

        try:
            retval, out, err = RGWAM(self.env).realm_new_zone_creds(realm_name, endpoints, sys_uid)
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return HandleCommandResult(retval=e.retcode, stdout=e.stdout, stderr=e.stderr)

        return HandleCommandResult(retval=retval, stdout=out, stderr=err)

    @CLICommand('rgw realm zone-creds remove', perm='rw')
    def _cmd_rgw_realm_rm_zone_creds(self, realm_token: Optional[str] = None) -> HandleCommandResult:
        """Create credentials for new zone creation"""

        try:
            retval, out, err = RGWAM(self.env).realm_rm_zone_creds(realm_token)
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return HandleCommandResult(retval=e.retcode, stdout=e.stdout, stderr=e.stderr)

        return HandleCommandResult(retval=retval, stdout=out, stderr=err)

    @CLICommand('rgw realm tokens', perm='r')
    def list_realm_tokens(self) -> HandleCommandResult:
        try:
            realms_info = self.get_realm_tokens()
        except RGWAMException as e:
            self.log.error(f'cmd run exception: ({e.retcode}) {e.message}')
            return HandleCommandResult(retval=e.retcode, stdout=e.stdout, stderr=e.stderr)

        return HandleCommandResult(retval=0, stdout=json.dumps(realms_info, indent=4), stderr='')

    def get_realm_tokens(self) -> List[Dict]:
        realms_info = []
        for realm_info in RGWAM(self.env).get_realms_info():
            if not realm_info['master_zone_id']:
                realms_info.append({'realm': realm_info['realm_name'], 'token': 'realm has no master zone'})
            elif not realm_info['endpoint']:
                realms_info.append({'realm': realm_info['realm_name'], 'token': 'master zone has no endpoint'})
            elif not (realm_info['access_key'] and realm_info['secret']):
                realms_info.append({'realm': realm_info['realm_name'], 'token': 'master zone has no access/secret keys'})
            else:
                keys = ['realm_name', 'realm_id', 'endpoint', 'access_key', 'secret']
                realm_token = RealmToken(**{k: realm_info[k] for k in keys})
                realm_token_b = realm_token.to_json().encode('utf-8')
                realm_token_s = base64.b64encode(realm_token_b).decode('utf-8')
                realms_info.append({'realm': realm_info['realm_name'], 'token': realm_token_s})
        return realms_info

    @CLICommand('rgw zone modify', perm='rw')
    def update_zone_info(self, realm_name: str, zonegroup_name: str, zone_name: str, realm_token: str, zone_endpoints: List[str]) -> HandleCommandResult:
        try:
            retval, out, err = RGWAM(self.env).zone_modify(realm_name,
                                                           zonegroup_name,
                                                           zone_name,
                                                           zone_endpoints,
                                                           realm_token)
            return HandleCommandResult(retval, 'Zone updated successfully', '')
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return HandleCommandResult(retval=e.retcode, stdout=e.stdout, stderr=e.stderr)

    @CLICommand('rgw zonegroup modify', perm='rw')
    def update_zonegroup_info(self, realm_name: str, zonegroup_name: str, zone_name: str, hostnames: List[str]) -> HandleCommandResult:
        try:
            retval, out, err = RGWAM(self.env).zonegroup_modify(realm_name,
                                                                zonegroup_name,
                                                                zone_name,
                                                                hostnames)
            return HandleCommandResult(retval, 'Zonegroup updated successfully', '')
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return HandleCommandResult(retval=e.retcode, stdout=e.stdout, stderr=e.stderr)

    @CLICommand('rgw zone create', perm='rw')
    @check_orchestrator
    def _cmd_rgw_zone_create(self,
                             zone_name: Optional[str] = None,
                             realm_token: Optional[str] = None,
                             port: Optional[int] = None,
                             placement: Optional[str] = None,
                             start_radosgw: Optional[bool] = True,
                             zone_endpoints: Optional[str] = None,
                             inbuf: Optional[str] = None) -> HandleCommandResult:
        """Bootstrap new rgw zone that syncs with zone on another cluster in the same realm"""

        try:
            created_zones = self.rgw_zone_create(zone_name, realm_token, port, placement,
                                                 start_radosgw, zone_endpoints, inbuf)
            return HandleCommandResult(retval=0, stdout=f"Zones {', '.join(created_zones)} created successfully")
        except RGWAMException as e:
            return HandleCommandResult(retval=e.retcode, stderr=f'Failed to create zone: {str(e)}')

    def rgw_zone_create(self,
                        zone_name: Optional[str] = None,
                        realm_token: Optional[str] = None,
                        port: Optional[int] = None,
                        placement: Optional[Union[str, Dict[str, Any]]] = None,
                        start_radosgw: Optional[bool] = True,
                        zone_endpoints: Optional[str] = None,
                        inbuf: Optional[str] = None) -> List[str]:

        if inbuf:
            try:
                rgw_specs = self._parse_rgw_specs(inbuf)
            except RGWSpecParsingError as e:
                raise RGWAMException(str(e))
        elif (zone_name and realm_token):
            token = RealmToken.from_base64_str(realm_token)
            if isinstance(placement, dict):
                placement_spec = PlacementSpec.from_json(placement) if placement else None
            elif isinstance(placement, str):
                placement_spec = PlacementSpec.from_string(placement) if placement else None
            rgw_specs = [RGWSpec(rgw_realm=token.realm_name,
                                 rgw_zone=zone_name,
                                 rgw_realm_token=realm_token,
                                 rgw_frontend_port=port,
                                 placement=placement_spec,
                                 zone_endpoints=zone_endpoints)]
        else:
            err_msg = 'Invalid arguments: either pass a spec with -i or provide the zone_name and realm_token.'
            raise RGWAMException(err_msg)

        try:
            created_zones = []
            for rgw_spec in rgw_specs:
                RGWAM(self.env).zone_create(rgw_spec, start_radosgw)
                if rgw_spec.rgw_zone is not None:
                    created_zones.append(rgw_spec.rgw_zone)
                    return created_zones
        except RGWAMException as e:
            err_msg = 'cmd run exception: (%d) %s' % (e.retcode, e.message)
            self.log.error(err_msg)
            raise e
        return created_zones

    @CLICommand('rgw realm reconcile', perm='rw')
    def _cmd_rgw_realm_reconcile(self,
                                 realm_name: Optional[str] = None,
                                 zonegroup_name: Optional[str] = None,
                                 zone_name: Optional[str] = None,
                                 update: Optional[bool] = False) -> HandleCommandResult:
        """Bootstrap new rgw zone that syncs with existing zone"""

        try:
            retval, out, err = RGWAM(self.env).realm_reconcile(realm_name, zonegroup_name,
                                                               zone_name, update)
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return HandleCommandResult(retval=e.retcode, stdout=e.stdout, stderr=e.stderr)

        return HandleCommandResult(retval=retval, stdout=out, stderr=err)

    def shutdown(self) -> None:
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def import_realm_token(self,
                           zone_name: Optional[str] = None,
                           realm_token: Optional[str] = None,
                           port: Optional[int] = None,
                           placement: Optional[dict] = None,
                           start_radosgw: Optional[bool] = True,
                           zone_endpoints: Optional[str] = None) -> None:
        placement_spec = placement.get('placement') if placement else None
        self.rgw_zone_create(zone_name, realm_token, port, placement_spec, start_radosgw,
                             zone_endpoints)
