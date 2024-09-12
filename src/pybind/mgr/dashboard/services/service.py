import json
import logging
import time
from subprocess import SubprocessError

try:
    from typing import Optional, Tuple
except ImportError:
    pass  # For typing only

from .. import mgr
from ..exceptions import DashboardException
from ..settings import Settings
from .orchestrator import OrchClient

logger = logging.getLogger('service')


class NoCredentialsException(Exception):
    def __init__(self):
        super(NoCredentialsException, self).__init__(
            'No RGW credentials found, '
            'please consult the documentation on how to enable RGW for '
            'the dashboard.')


def verify_service_restart(service_type: str, service_id: str):
    orch = OrchClient.instance()
    service_name = f'{service_type}.{service_id}'

    logger.info("Getting initial service info for: %s", service_name)
    info = orch.services.get(service_name)[0].to_dict()
    logger.info("Initial service info: %s", info)
    last_refreshed = info['status']['last_refresh']

    logger.info("Reloading service: %s", service_name)
    orch.services.reload(service_type, service_id)

    logger.info("Waiting for service refresh: %s", service_name)
    wait_for_refresh(orch, service_name, last_refreshed)

    logger.info("Checking daemon status for: %s", service_name)
    daemon_status = wait_for_daemon_to_start(service_name)
    return daemon_status


def wait_for_refresh(orch, service_name, last_refreshed):
    orch = OrchClient.instance()
    logger.info("Waiting for service %s to refresh", service_name)

    while True:
        updated_info = orch.services.get(service_name)[0].to_dict()
        if updated_info['status']['last_refresh'] != last_refreshed:
            logger.info("Service %s refreshed", service_name)
            break


def wait_for_daemon_to_start(service_name, timeout=20):
    orch = OrchClient.instance()
    start_time = time.time()
    logger.info("Waiting for daemon %s to start", service_name)

    while True:
        daemons = [d.to_dict() for d in orch.services.list_daemons(service_name=service_name)]
        logger.info("Daemon list for service %s: %s", service_name, daemons)

        if not daemons:
            logger.info("No daemons found for service %s. Retrying...", service_name)
            # Check if timeout has been reached
            daemon_start_time = time.time()
            if time.time() - daemon_start_time > timeout:
                logger.error("Timeout reached while waiting for daemon list for service %s", service_name)
                raise DashboardException(
                    code='daemon_list_timeout',
                    msg="Timeout reached while waiting for daemon list for service %s." % service_name
                )
            time.sleep(1)
            continue  # Retry getting daemon list

        all_running = True

        for daemon in daemons:
            logger.info("Checking daemon %s state", daemon['daemon_id'])
            daemon_state = daemon['status_desc']
            logger.info("Daemon %s state: %s", daemon['daemon_id'], daemon_state)

            if daemon_state in ('unknown', 'error', 'stopped'):
                logger.error("Failed to restart daemon %s for service %s. State is %s", daemon['daemon_id'], service_name, daemon_state)
                raise DashboardException(
                    code='daemon_restart_failed',
                    msg="Failed to restart the daemon %s. Daemon state is %s." % (daemon['daemon_id'], daemon_state)
                )

            if daemon_state == 'starting':
                all_running = False

            elif daemon_state != 'running':
                all_running = False

        if all_running:
            logger.info("All daemons for service %s are running", service_name)
            return True

        if time.time() - start_time > timeout:
            logger.error("Timeout reached while waiting for daemon %s to start", service_name)
            raise DashboardException(
                code='daemon_restart_timeout',
                msg="Timeout reached while waiting for daemon %s to start." % service_name
            )

        time.sleep(1)  # Adding a short delay before retrying


class RgwServiceManager:
    def create_rgw_instance(self, service_spec):
        # Create RGW instance.
        logger.info("Creating RGW instance")
        orch = OrchClient.instance()
        result = orch.services.apply(service_spec)
        return result
    
    def find_available_port(self, starting_port=80):
        orch = OrchClient.instance()
        daemons = [d.to_dict() for d in orch.services.list_daemons(daemon_type='rgw')]
        used_ports = set()
        for daemon in daemons:
            ports = daemon.get('ports', [])
            if ports:
                used_ports.update(ports)
        port = starting_port
        while port in used_ports:
            port += 1
        return port

    def restart_rgw_daemons_and_set_credentials(self):
        # Restart RGW daemons and set credentials.
        logger.info("Restarting RGW daemons and setting credentials")
        orch = OrchClient.instance()
        services, _ = orch.services.list(service_type='rgw', offset=0)

        all_daemons_up = True
        for service in services:
            logger.info("Verifying service restart for: %s", service['service_id'])
            daemons_up = verify_service_restart('rgw', service['service_id'])
            if not daemons_up:
                logger.error("Service %s restart verification failed", service['service_id'])
                all_daemons_up = False

        if all_daemons_up:
            logger.info("All daemons are up, configuring RGW credentials")
            self.configure_rgw_credentials()
        else:
            logger.error("Not all daemons are up, skipping RGW credentials configuration")

    def _parse_secrets(self, user: str, data: dict) -> Tuple[str, str]:
        for key in data.get('keys', []):
            if key.get('user') == user and data.get('system') in ['true', True]:
                access_key = key.get('access_key')
                secret_key = key.get('secret_key')
                return access_key, secret_key
        return '', ''

    def _get_user_keys(self, user: str, realm: Optional[str] = None) -> Tuple[str, str]:
        access_key = ''
        secret_key = ''
        rgw_user_info_cmd = ['user', 'info', '--uid', user]
        cmd_realm_option = ['--rgw-realm', realm] if realm else []
        if realm:
            rgw_user_info_cmd += cmd_realm_option
        try:
            _, out, err = mgr.send_rgwadmin_command(rgw_user_info_cmd)
            if out:
                access_key, secret_key = self._parse_secrets(user, out)
            if not access_key:
                rgw_create_user_cmd = [
                    'user', 'create',
                    '--uid', user,
                    '--display-name', 'Ceph Dashboard',
                    '--system',
                ] + cmd_realm_option
                _, out, err = mgr.send_rgwadmin_command(rgw_create_user_cmd)
                if out:
                    access_key, secret_key = self._parse_secrets(user, out)
            if not access_key:
                logger.error('Unable to create rgw user "%s": %s', user, err)
        except SubprocessError as error:
            logger.exception(error)

        return access_key, secret_key

    def configure_rgw_credentials(self):
        logger.info('Configuring dashboard RGW credentials')
        user = 'dashboard'
        realms = []
        access_key = ''
        secret_key = ''
        try:
            # Fetch the list of realms
            _, out, err = mgr.send_rgwadmin_command(['realm', 'list'])
            if out:
                realms = out.get('realms', [])
            if err:
                logger.error('Unable to list RGW realms: %s', err)

            realm_access_keys = {}
            realm_secret_keys = {}

            # If realms exist, fetch credentials for each realm and store them with keys
            if realms:
                for realm in realms:
                    realm_access_key, realm_secret_key = self._get_user_keys(user, realm)
                    if realm_access_key:
                        realm_access_keys[realm] = realm_access_key
                        realm_secret_keys[realm] = realm_secret_key

                # Store default dashboard user credentials with empty key when realms exist
                default_access_key, default_secret_key = self._get_user_keys(user)
                realm_access_keys[''] = default_access_key or ''
                realm_secret_keys[''] = default_secret_key or ''

                # Convert the credentials dictionary to JSON
                access_key = json.dumps(realm_access_keys)
                secret_key = json.dumps(realm_secret_keys)
            else:
                # When no realms exist, store credentials as plain text
                access_key, secret_key = self._get_user_keys(user)

            assert access_key and secret_key
            Settings.RGW_API_ACCESS_KEY = access_key
            Settings.RGW_API_SECRET_KEY = secret_key

        except (AssertionError, SubprocessError) as error:
            logger.exception(error)
            raise NoCredentialsException
