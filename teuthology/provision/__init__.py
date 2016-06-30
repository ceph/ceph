import json
import logging
import os
import random
import re
import subprocess
import time
import tempfile
import yaml

from subprocess import CalledProcessError

from .. import misc

from ..openstack import OpenStack, OpenStackInstance
from ..config import config
from ..contextutil import safe_while
from ..exceptions import QuotaExceededError
from ..misc import decanonicalize_hostname, get_distro, get_distro_version
from ..lockstatus import get_status


log = logging.getLogger(__name__)


def downburst_executable():
    """
    First check for downburst in the user's path.
    Then check in ~/src, ~ubuntu/src, and ~teuthology/src.
    Return '' if no executable downburst is found.
    """
    if config.downburst:
        return config.downburst
    path = os.environ.get('PATH', None)
    if path:
        for p in os.environ.get('PATH', '').split(os.pathsep):
            pth = os.path.join(p, 'downburst')
            if os.access(pth, os.X_OK):
                return pth
    import pwd
    little_old_me = pwd.getpwuid(os.getuid()).pw_name
    for user in [little_old_me, 'ubuntu', 'teuthology']:
        pth = os.path.expanduser(
            "~%s/src/downburst/virtualenv/bin/downburst" % user)
        if os.access(pth, os.X_OK):
            return pth
    return ''


class Downburst(object):
    """
    A class that provides methods for creating and destroying virtual machine
    instances using downburst: https://github.com/ceph/downburst
    """
    def __init__(self, name, os_type, os_version, status=None, user='ubuntu'):
        self.name = name
        self.os_type = os_type
        self.os_version = os_version
        self.status = status or get_status(self.name)
        self.config_path = None
        self.user_path = None
        self.user = user
        self.host = decanonicalize_hostname(self.status['vm_host']['name'])
        self.executable = downburst_executable()

    def create(self):
        """
        Launch a virtual machine instance.

        If creation fails because an instance with the specified name is
        already running, first destroy it, then try again. This process will
        repeat two more times, waiting 60s between tries, before giving up.
        """
        if not self.executable:
            log.error("No downburst executable found.")
            return False
        self.build_config()
        success = None
        with safe_while(sleep=60, tries=3,
                        action="downburst create") as proceed:
            while proceed():
                (returncode, stdout, stderr) = self._run_create()
                if returncode == 0:
                    log.info("Downburst created %s: %s" % (self.name,
                                                           stdout.strip()))
                    success = True
                    break
                elif stderr:
                    # If the guest already exists first destroy then re-create:
                    if 'exists' in stderr:
                        success = False
                        log.info("Guest files exist. Re-creating guest: %s" %
                                 (self.name))
                        self.destroy()
                    else:
                        success = False
                        log.info("Downburst failed on %s: %s" % (
                            self.name, stderr.strip()))
                        break
            return success

    def _run_create(self):
        """
        Used by create(), this method is what actually calls downburst when
        creating a virtual machine instance.
        """
        if not self.config_path:
            raise ValueError("I need a config_path!")
        if not self.user_path:
            raise ValueError("I need a user_path!")
        shortname = decanonicalize_hostname(self.name)

        args = [
            self.executable,
            '-c', self.host,
            'create',
            '--wait',
            '--meta-data=%s' % self.config_path,
            '--user-data=%s' % self.user_path,
            shortname,
        ]
        log.info("Provisioning a {distro} {distroversion} vps".format(
            distro=self.os_type,
            distroversion=self.os_version
        ))
        proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        return (proc.returncode, out, err)

    def destroy(self):
        """
        Destroy (shutdown and delete) a virtual machine instance.
        """
        executable = self.executable
        if not executable:
            log.error("No downburst executable found.")
            return False
        shortname = decanonicalize_hostname(self.name)
        args = [executable, '-c', self.host, 'destroy', shortname]
        proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,)
        out, err = proc.communicate()
        if err:
            log.error("Error destroying {machine}: {msg}".format(
                machine=self.name, msg=err))
            return False
        elif proc.returncode == 0:
            out_str = ': %s' % out if out else ''
            log.info("Destroyed %s%s" % (self.name, out_str))
            return True
        else:
            log.error("I don't know if the destroy of {node} succeded!".format(
                node=self.name))
            return False

    def build_config(self):
        """
        Assemble a configuration to pass to downburst, and write it to a file.
        """
        config_fd = tempfile.NamedTemporaryFile(delete=False)

        os_type = self.os_type.lower()
        mac_address = self.status['mac_address']

        file_info = {
            'disk-size': '100G',
            'ram': '3.8G',
            'cpus': 1,
            'networks': [
                {'source': 'front', 'mac': mac_address}],
            'distro': os_type,
            'distroversion': self.os_version,
            'additional-disks': 3,
            'additional-disks-size': '200G',
            'arch': 'x86_64',
        }
        fqdn = self.name.split('@')[1]
        file_out = {
            'downburst': file_info,
            'local-hostname': fqdn,
        }
        yaml.safe_dump(file_out, config_fd)
        self.config_path = config_fd.name

        user_info = {
            'user': self.user,
            # Remove the user's password so console logins are possible
            'runcmd': [
                ['passwd', '-d', self.user],
            ]
        }
        # On CentOS/RHEL/Fedora, write the correct mac address
        if os_type in ['centos', 'rhel', 'fedora']:
            user_info['runcmd'].extend([
                ['sed', '-ie', 's/HWADDR=".*"/HWADDR="%s"/' % mac_address,
                 '/etc/sysconfig/network-scripts/ifcfg-eth0'],
            ])
        # On Ubuntu, starting with 16.04, we need to install 'python' to get
        # python2.7, which ansible needs
        elif os_type == 'ubuntu':
            if not 'packages' in user_info:
                user_info['packages'] = list()
            user_info['packages'].extend([
                'python',
            ])
        user_fd = tempfile.NamedTemporaryFile(delete=False)
        yaml.safe_dump(user_info, user_fd)
        self.user_path = user_fd.name
        return True

    def remove_config(self):
        """
        Remove the downburst configuration file created by build_config()
        """
        if self.config_path and os.path.exists(self.config_path):
            os.remove(self.config_path)
            self.config_path = None
            return True
        if self.user_path and os.path.exists(self.user_path):
            os.remove(self.user_path)
            self.user_path = None
            return True
        return False

    def __del__(self):
        self.remove_config()


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
