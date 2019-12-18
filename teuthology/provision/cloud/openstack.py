import logging
import re
import requests
import socket
import time
import yaml

from teuthology.util.compat import urlencode

from copy import deepcopy
from libcloud.common.exceptions import RateLimitReachedError, BaseHTTPError

from paramiko import AuthenticationException
from paramiko.ssh_exception import NoValidConnectionsError

from teuthology.config import config
from teuthology.contextutil import safe_while

from teuthology.provision.cloud import base
from teuthology.provision.cloud import util
from teuthology.provision.cloud.base import Provider


log = logging.getLogger(__name__)


RETRY_EXCEPTIONS = (RateLimitReachedError, BaseHTTPError)


def retry(function, *args, **kwargs):
    """
    Call a function (returning its results), retrying if any of the exceptions
    in RETRY_EXCEPTIONS are raised
    """
    with safe_while(sleep=1, tries=24, increment=1) as proceed:
        tries = 0
        while proceed():
            tries += 1
            try:
                result = function(*args, **kwargs)
                if tries > 1:
                    log.debug(
                        "'%s' succeeded after %s tries",
                        function.__name__,
                        tries,
                    )
                return result
            except RETRY_EXCEPTIONS:
                pass


class OpenStackProvider(Provider):
    _driver_posargs = ['username', 'password']

    def _get_driver(self):
        self._auth_token = util.AuthToken(name='teuthology_%s' % self.name)
        with self._auth_token as token:
            driver = super(OpenStackProvider, self)._get_driver()
            # We must apparently call get_service_catalog() so that
            # get_endpoint() works.
            driver.connection.get_service_catalog()
            if not token.value:
                token.write(
                    driver.connection.auth_token,
                    driver.connection.auth_token_expires,
                    driver.connection.get_endpoint(),
                )
        return driver
    driver = property(fget=_get_driver)

    def _get_driver_args(self):
        driver_args = super(OpenStackProvider, self)._get_driver_args()
        if self._auth_token.value:
            driver_args['ex_force_auth_token'] = self._auth_token.value
            driver_args['ex_force_base_url'] = self._auth_token.endpoint
        return driver_args

    @property
    def ssh_interface(self):
        if not hasattr(self, '_ssh_interface'):
            self._ssh_interface = self.conf.get('ssh_interface', 'public_ips')
        return self._ssh_interface

    @property
    def images(self):
        if not hasattr(self, '_images'):
            self._images = retry(self.driver.list_images)
        return self._images

    @property
    def sizes(self):
        if not hasattr(self, '_sizes'):
            allow_sizes = self.conf.get('allow_sizes', '.*')
            if not isinstance(allow_sizes, list):
                allow_sizes = [allow_sizes]
            allow_re = [re.compile(x) for x in allow_sizes]
            # By default, exclude instance types meant for Windows
            exclude_sizes = self.conf.get('exclude_sizes', 'win-.*')
            if not isinstance(exclude_sizes, list):
                exclude_sizes = [exclude_sizes]
            exclude_re = [re.compile(x) for x in exclude_sizes]
            sizes = retry(self.driver.list_sizes)
            self._sizes = list(filter(
                lambda s:
                    any(x.match(s.name) for x in allow_re)
                    and not
                    all(x.match(s.name) for x in exclude_re),
                sizes
            ))
        return self._sizes

    @property
    def networks(self):
        if not hasattr(self, '_networks'):
            allow_networks = self.conf.get('allow_networks', '.*')
            if not isinstance(allow_networks, list):
                allow_networks=[allow_networks]
            networks_re = [re.compile(x) for x in allow_networks]
            try:
                networks = retry(self.driver.ex_list_networks)
                if networks:
                    self._networks = filter(
                        lambda s: any(x.match(s.name) for x in networks_re),
                        networks
                    )
                else:
                    self._networks = list()
            except AttributeError:
                log.warn("Unable to list networks for %s", self.driver)
                self._networks = list()
        return self._networks

    @property
    def default_userdata(self):
        if not hasattr(self, '_default_userdata'):
            self._default_userdata = self.conf.get('userdata', dict())
        return self._default_userdata

    @property
    def security_groups(self):
        if not hasattr(self, '_security_groups'):
            try:
                self._security_groups = retry(
                    self.driver.ex_list_security_groups
                )
            except AttributeError:
                log.warn("Unable to list security groups for %s", self.driver)
                self._security_groups = list()
        return self._security_groups


class OpenStackProvisioner(base.Provisioner):
    _sentinel_path = '/.teuth_provisioned'

    defaults = dict(
        openstack=dict(
            machine=dict(
                disk=20,
                ram=8000,
                cpus=1,
            ),
            volumes=dict(
                count=0,
                size=0,
            ),
        )
    )

    def __init__(
        self,
        provider, name, os_type=None, os_version=None,
        conf=None,
        user='ubuntu',
    ):
        super(OpenStackProvisioner, self).__init__(
            provider, name, os_type, os_version, conf=conf, user=user,
        )
        self._read_conf(conf)

    def _read_conf(self, conf=None):
        """
        Looks through the following in order:

            the 'conf' arg
            conf[DRIVER_NAME]
            teuthology.config.config.DRIVER_NAME
            self.defaults[DRIVER_NAME]

        It will use the highest value for each of the following: disk, RAM,
        cpu, volume size and count

        The resulting configuration becomes the new instance configuration
        and is stored as self.conf

        :param conf: The instance configuration

        :return: None
        """
        driver_name = self.provider.driver_name.lower()
        full_conf = conf or dict()
        driver_conf = full_conf.get(driver_name, dict())
        legacy_conf = getattr(config, driver_name) or dict()
        defaults = self.defaults.get(driver_name, dict())
        confs = list()
        for obj in (full_conf, driver_conf, legacy_conf, defaults):
            obj = deepcopy(obj)
            if isinstance(obj, list):
                confs.extend(obj)
            else:
                confs.append(obj)
        self.conf = util.combine_dicts(confs, lambda x, y: x > y)

    def _create(self):
        userdata = self.userdata
        log.debug("Creating node: %s", self)
        log.debug("Selected size: %s", self.size)
        log.debug("Selected image: %s", self.image)
        log.debug("Using userdata: %s", userdata)
        create_args = dict(
            name=self.name,
            size=self.size,
            image=self.image,
            ex_userdata=userdata,
        )
        networks = self.provider.networks
        if networks:
            create_args['networks'] = networks
        security_groups = self.security_groups
        if security_groups:
            create_args['ex_security_groups'] = security_groups
        self._node = retry(
            self.provider.driver.create_node,
            **create_args
        )
        log.debug("Created node: %s", self.node)
        results = retry(
            self.provider.driver.wait_until_running,
            nodes=[self.node],
            ssh_interface=self.provider.ssh_interface,
        )
        self._node, self.ips = results[0]
        log.debug("Node started: %s", self.node)
        if not self._create_volumes():
            self._destroy_volumes()
            return False
        self._update_dns()
        # Give cloud-init a few seconds to bring up the network, start sshd,
        # and install the public key
        time.sleep(20)
        self._wait_for_ready()
        return self.node

    def _create_volumes(self):
        vol_count = self.conf['volumes']['count']
        vol_size = self.conf['volumes']['size']
        name_templ = "%s_%0{0}d".format(len(str(vol_count - 1)))
        vol_names = [name_templ % (self.name, i)
                     for i in range(vol_count)]
        try:
            for name in vol_names:
                volume = retry(
                    self.provider.driver.create_volume,
                    vol_size,
                    name,
                )
                log.info("Created volume %s", volume)
                retry(
                    self.provider.driver.attach_volume,
                    self.node,
                    volume,
                    device=None,
                )
        except Exception:
            log.exception("Failed to create or attach volume!")
            return False
        return True

    def _destroy_volumes(self):
        all_volumes = retry(self.provider.driver.list_volumes)
        our_volumes = [vol for vol in all_volumes
                       if vol.name.startswith("%s_" % self.name)]
        for vol in our_volumes:
            try:
                retry(self.provider.driver.detach_volume, vol)
            except Exception:
                log.exception("Could not detach volume %s", vol)
            try:
                retry(self.provider.driver.destroy_volume, vol)
            except Exception:
                log.exception("Could not destroy volume %s", vol)

    def _update_dns(self):
        query = urlencode(dict(
            name=self.name,
            ip=self.ips[0],
        ))
        nsupdate_url = "%s?%s" % (
            config.nsupdate_url,
            query,
        )
        resp = requests.get(nsupdate_url)
        resp.raise_for_status()

    def _wait_for_ready(self):
        with safe_while(sleep=6, tries=20) as proceed:
            while proceed():
                try:
                    self.remote.connect()
                    break
                except (
                    socket.error,
                    NoValidConnectionsError,
                    AuthenticationException,
                ):
                    pass
        cmd = "while [ ! -e '%s' ]; do sleep 5; done" % self._sentinel_path
        self.remote.run(args=cmd, timeout=600)
        log.info("Node is ready: %s", self.node)

    @property
    def image(self):
        os_specs = [
            '{os_type} {os_version}',
            '{os_type}-{os_version}',
        ]
        for spec in os_specs:
            matches = [image for image in self.provider.images
                       if spec.format(
                           os_type=self.os_type,
                           os_version=self.os_version,
                       ) in image.name.lower()]
            if matches:
                break
        if not matches:
            raise RuntimeError(
                "Could not find an image for %s %s" %
                (self.os_type, self.os_version))
        return matches[0]

    @property
    def size(self):
        ram = self.conf['machine']['ram']
        disk = self.conf['machine']['disk']
        cpu = self.conf['machine']['cpus']

        def good_size(size):
            if (size.ram < ram or size.disk < disk or size.vcpus < cpu):
                return False
            return True

        all_sizes = self.provider.sizes
        good_sizes = filter(good_size, all_sizes)
        smallest_match = sorted(
            good_sizes,
            key=lambda s: (s.ram, s.disk, s.vcpus)
        )[0]
        return smallest_match

    @property
    def security_groups(self):
        group_names = self.provider.conf.get('security_groups')
        if group_names is None:
            return
        result = list()
        groups = self.provider.security_groups
        for name in group_names:
            matches = [group for group in groups if group.name == name]
            if not matches:
                msg = "No security groups found with name '%s'"
            elif len(matches) > 1:
                msg = "More than one security group found with name '%s'"
            elif len(matches) == 1:
                result.append(matches[0])
                continue
            raise RuntimeError(msg % name)
        return result

    @property
    def userdata(self):
        spec="{t}-{v}".format(t=self.os_type,
                              v=self.os_version)
        base_config = dict(
            packages=[
                'git',
                'wget',
                'python',
                'ntp',
            ],
        )
        runcmd=[
            # Remove the user's password so that console logins are
            # possible
            ['passwd', '-d', self.user],
            ['touch', self._sentinel_path]
        ]
        if spec in self.provider.default_userdata:
            base_config = deepcopy(
                    self.provider.default_userdata.get(spec, dict()))
        base_config.update(user=self.user)
        if 'manage_etc_hosts' not in base_config:
            base_config.update(
                manage_etc_hosts=True,
                hostname=self.hostname,
            )
        base_config['runcmd'] = base_config.get('runcmd', list())
        base_config['runcmd'].extend(runcmd)
        ssh_pubkey = util.get_user_ssh_pubkey()
        if ssh_pubkey:
            authorized_keys = base_config.get('ssh_authorized_keys', list())
            authorized_keys.append(ssh_pubkey)
            base_config['ssh_authorized_keys'] = authorized_keys
        user_str = "#cloud-config\n" + yaml.safe_dump(base_config)
        return user_str

    @property
    def node(self):
        if hasattr(self, '_node'):
            return self._node
        matches = self._find_nodes()
        msg = "Unknown error locating %s"
        if not matches:
            msg = "No nodes found with name '%s'" % self.name
            log.warn(msg)
            return
        elif len(matches) > 1:
            msg = "More than one node found with name '%s'"
        elif len(matches) == 1:
            self._node = matches[0]
            return self._node
        raise RuntimeError(msg % self.name)

    def _find_nodes(self):
        nodes = retry(self.provider.driver.list_nodes)
        matches = [node for node in nodes if node.name == self.name]
        return matches

    def _destroy(self):
        self._destroy_volumes()
        nodes = self._find_nodes()
        if not nodes:
            log.warn("Didn't find any nodes named '%s' to destroy!", self.name)
            return True
        if len(nodes) > 1:
            log.warn("Found multiple nodes named '%s' to destroy!", self.name)
        log.info("Destroying nodes: %s", nodes)
        return all([node.destroy() for node in nodes])
