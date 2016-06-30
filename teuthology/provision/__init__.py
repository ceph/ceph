import json
import logging
import os
import random
import re
import subprocess
import time
import tempfile

from subprocess import CalledProcessError

from .. import misc

from ..openstack import OpenStack, OpenStackInstance
from ..config import config
from ..contextutil import safe_while
from ..exceptions import QuotaExceededError
from ..misc import decanonicalize_hostname, get_distro, get_distro_version
from ..lockstatus import get_status

from .downburst import Downburst


log = logging.getLogger(__name__)


class ProvisionOpenStack(OpenStack):
    """
    A class that provides methods for creating and destroying virtual machine
    instances using OpenStack
    """
    def __init__(self):
        super(ProvisionOpenStack, self).__init__()
        self.user_data = tempfile.mktemp()
        log.debug("ProvisionOpenStack: " + str(config.openstack))
        self.basename = 'target'
        self.up_string = 'The system is finally up'
        self.property = "%16x" % random.getrandbits(128)

    def __del__(self):
        if os.path.exists(self.user_data):
            os.unlink(self.user_data)

    def init_user_data(self, os_type, os_version):
        """
        Get the user-data file that is fit for os_type and os_version.
        It is responsible for setting up enough for ansible to take
        over.
        """
        template_path = config['openstack']['user-data'].format(
            os_type=os_type,
            os_version=os_version)
        nameserver = config['openstack'].get('nameserver', '8.8.8.8')
        user_data_template = open(template_path).read()
        user_data = user_data_template.format(
            up=self.up_string,
            nameserver=nameserver,
            username=self.username,
            lab_domain=config.lab_domain)
        open(self.user_data, 'w').write(user_data)

    def attach_volumes(self, name, volumes):
        """
        Create and attach volumes to the named OpenStack instance.
        """
        for i in range(volumes['count']):
            volume_name = name + '-' + str(i)
            try:
                misc.sh("openstack volume show -f json " +
                         volume_name)
            except subprocess.CalledProcessError as e:
                if 'No volume with a name or ID' not in e.output:
                    raise e
                misc.sh("openstack volume create -f json " +
                        config['openstack'].get('volume-create', '') + " " +
                        " --property ownedby=" + config.openstack['ip'] +
                        " --size " + str(volumes['size']) + " " +
                        volume_name)
            with safe_while(sleep=2, tries=100,
                            action="volume " + volume_name) as proceed:
                while proceed():
                    r = misc.sh("openstack volume show  -f json " +
                                volume_name)
                    status = self.get_value(json.loads(r), 'status')
                    if status == 'available':
                        break
                    else:
                        log.info("volume " + volume_name +
                                 " not available yet")
            misc.sh("openstack server add volume " +
                    name + " " + volume_name)

    @staticmethod
    def ip2name(prefix, ip):
        """
        return the instance name suffixed with the /16 part of the IP.
        """
        digits = map(int, re.findall('.*\.(\d+)\.(\d+)', ip)[0])
        return prefix + "%03d%03d" % tuple(digits)

    def create(self, num, os_type, os_version, arch, resources_hint):
        """
        Create num OpenStack instances running os_type os_version and
        return their names. Each instance has at least the resources
        described in resources_hint.
        """
        log.debug('ProvisionOpenStack:create')
        resources_hint = self.interpret_hints({
            'machine': config['openstack']['machine'],
            'volumes': config['openstack']['volumes'],
        }, resources_hint)
        self.init_user_data(os_type, os_version)
        image = self.image(os_type, os_version)
        if 'network' in config['openstack']:
            net = "--nic net-id=" + str(self.net_id(config['openstack']['network']))
        else:
            net = ''
        flavor = self.flavor(resources_hint['machine'],
                             config['openstack'].get('flavor-select-regexp'))
        cmd = ("flock --close --timeout 28800 /tmp/teuthology-server-create.lock" +
               " openstack server create" +
               " " + config['openstack'].get('server-create', '') +
               " -f json " +
               " --image '" + str(image) + "'" +
               " --flavor '" + str(flavor) + "'" +
               " --key-name teuthology " +
               " --user-data " + str(self.user_data) +
               " " + net +
               " --min " + str(num) +
               " --max " + str(num) +
               " --security-group teuthology" +
               " --property teuthology=" + self.property +
               " --property ownedby=" + config.openstack['ip'] +
               " --wait " +
               " " + self.basename)
        try:
            misc.sh(cmd)
        except CalledProcessError as exc:
            if "quota exceeded" in exc.output.lower():
                raise QuotaExceededError(message=exc.output)
            raise
        instances = filter(
            lambda instance: self.property in instance['Properties'],
            self.list_instances())
        instances = [OpenStackInstance(i['ID']) for i in instances]
        fqdns = []
        try:
            network = config['openstack'].get('network', '')
            for instance in instances:
                ip = instance.get_ip(network)
                name = self.ip2name(self.basename, ip)
                misc.sh("openstack server set " +
                        "--name " + name + " " +
                        instance['ID'])
                fqdn = name + '.' + config.lab_domain
                if not misc.ssh_keyscan_wait(fqdn):
                    raise ValueError('ssh_keyscan_wait failed for ' + fqdn)
                time.sleep(15)
                if not self.cloud_init_wait(instance):
                    raise ValueError('cloud_init_wait failed for ' + fqdn)
                self.attach_volumes(name, resources_hint['volumes'])
                fqdns.append(fqdn)
        except Exception as e:
            log.exception(str(e))
            for id in [instance['ID'] for instance in instances]:
                self.destroy(id)
            raise e
        return fqdns

    def destroy(self, name_or_id):
        log.debug('ProvisionOpenStack:destroy ' + name_or_id)
        return OpenStackInstance(name_or_id).destroy()


def create_if_vm(ctx, machine_name, _downburst=None):
    """
    Use downburst to create a virtual machine

    :param _downburst: Only used for unit testing.
    """
    if _downburst:
        status_info = _downburst.status
    else:
        status_info = get_status(machine_name)
    if not status_info.get('is_vm', False):
        return False
    os_type = get_distro(ctx)
    os_version = get_distro_version(ctx)

    has_config = hasattr(ctx, 'config') and ctx.config is not None
    if has_config and 'downburst' in ctx.config:
        log.warning(
            'Usage of a custom downburst config has been deprecated.'
        )

    dbrst = _downburst or Downburst(name=machine_name, os_type=os_type,
                                    os_version=os_version, status=status_info)
    return dbrst.create()


def destroy_if_vm(ctx, machine_name, user=None, description=None,
                  _downburst=None):
    """
    Use downburst to destroy a virtual machine

    Return False only on vm downburst failures.

    :param _downburst: Only used for unit testing.
    """
    if _downburst:
        status_info = _downburst.status
    else:
        status_info = get_status(machine_name)
    if not status_info or not status_info.get('is_vm', False):
        return True
    if user is not None and user != status_info['locked_by']:
        msg = "Tried to destroy {node} as {as_user} but it is locked " + \
            "by {locked_by}"
        log.error(msg.format(node=machine_name, as_user=user,
                             locked_by=status_info['locked_by']))
        return False
    if (description is not None and description !=
            status_info['description']):
        msg = "Tried to destroy {node} with description {desc_arg} " + \
            "but it is locked with description {desc_lock}"
        log.error(msg.format(node=machine_name, desc_arg=description,
                             desc_lock=status_info['description']))
        return False
    if status_info.get('machine_type') == 'openstack':
        return ProvisionOpenStack().destroy(
            decanonicalize_hostname(machine_name))

    dbrst = _downburst or Downburst(name=machine_name, os_type=None,
                                    os_version=None, status=status_info)
    return dbrst.destroy()
