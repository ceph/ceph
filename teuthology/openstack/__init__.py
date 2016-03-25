#
# Copyright (c) 2015 Red Hat, Inc.
#
# Author: Loic Dachary <loic@dachary.org>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
import copy
import datetime
import json
import logging
import os
import paramiko
import re
import socket
import subprocess
import tempfile
import teuthology
import types

from subprocess import CalledProcessError

from teuthology.contextutil import safe_while
from teuthology.config import config as teuth_config
from teuthology.config import set_config_attr
from teuthology.orchestra import connection
from teuthology import misc

log = logging.getLogger(__name__)

def enforce_json_dictionary(something):
    if type(something) is not types.DictType:
        raise Exception(
            'Please pip uninstall --yes cliff-tablib and try again.'
            ' Details about this error can be found at'
            ' https://bugs.launchpad.net/python-openstackclient/+bug/1510546'
            ' you are encouraged to add a comment if you want it to be'
            ' fixed.')

class OpenStackInstance(object):

    def __init__(self, name_or_id, info=None):
        self.name_or_id = name_or_id
        self.ip = None
        if info is None:
            self.set_info()
        else:
            self.info = dict(map(lambda (k,v): (k.lower(), v), info.iteritems()))

    def set_info(self):
        try:
            self.info = json.loads(
                misc.sh("openstack server show -f json " + self.name_or_id))
            enforce_json_dictionary(self.info)
        except CalledProcessError:
            self.info = None

    def __getitem__(self, name):
        return self.info[name.lower()]

    def get_created(self):
        now = datetime.datetime.now()
        created = datetime.datetime.strptime(
            self['created'], '%Y-%m-%dT%H:%M:%SZ')
        return (now - created).total_seconds()

    def exists(self):
        return self.info is not None

    def get_volumes(self):
        """
        Return the uuid of the volumes attached to the name_or_id
        OpenStack instance.
        """
        volumes = self['os-extended-volumes:volumes_attached']
        return [volume['id'] for volume in volumes ]

    def get_addresses(self):
        """
        Return the list of IPs associated with instance_id in OpenStack.
        """
        with safe_while(sleep=2, tries=30,
                        action="get ip " + self['id']) as proceed:
            while proceed():
                found = re.match('.*\d+', self['addresses'])
                if found:
                    return self['addresses']
                self.set_info()

    def get_ip_neutron(self):
        subnets = json.loads(misc.sh("neutron subnet-list -f json -c id -c ip_version"))
        subnet_id = None
        for subnet in subnets:
            if subnet['ip_version'] == 4:
                subnet_id = subnet['id']
                break
        if not subnet_id:
            raise Exception("no subnet with ip_version == 4")
        ports = json.loads(misc.sh("neutron port-list -f json -c fixed_ips -c device_id"))
        fixed_ips = None
        for port in ports:
            if port['device_id'] == self['id']:
                fixed_ips = port['fixed_ips'].split("\n")
                break
        if not fixed_ips:
            raise Exception("no fixed ip record found")
        ip = None
        for fixed_ip in fixed_ips:
            record = json.loads(fixed_ip)
            if record['subnet_id'] == subnet_id:
                ip = record['ip_address']
                break
        if not ip:
            raise Exception("no ip")
        return ip

    def get_ip(self, network):
        """
        Return the private IP of the OpenStack instance_id.
        """
        try:
            return self.get_ip_neutron()
        except Exception as e:
            log.debug("ignoring get_ip_neutron exception " + str(e))
            return re.findall(network + '=([\d.]+)',
                              self.get_addresses())[0]

    def get_floating_ip(self):
        ips = json.loads(misc.sh("openstack ip floating list -f json"))
        for ip in ips:
            if ip['Instance ID'] == self['id']:
                return ip['IP']
        return None

    def get_floating_ip_or_ip(self):
        if not self.ip:
            self.ip = self.get_floating_ip()
            if not self.ip:
                self.ip = self.get_ip('')
        return self.ip

    def destroy(self):
        """
        Delete the name_or_id OpenStack instance.
        """
        if not self.exists():
            return True
        volumes = self.get_volumes()
        misc.sh("openstack server set --name REMOVE-ME-" + self.name_or_id +
                " " + self['id'])
        misc.sh("openstack server delete --wait " + self['id'] +
                " || true")
        for volume in volumes:
            misc.sh("openstack volume set --name REMOVE-ME " + volume + " || true")
            misc.sh("openstack volume delete " + volume + " || true")
        return True


class OpenStack(object):

    # wget -O debian-8.0.qcow2  http://cdimage.debian.org/cdimage/openstack/current/debian-8.1.0-openstack-amd64.qcow2
    # wget -O ubuntu-12.04.qcow2 https://cloud-images.ubuntu.com/precise/current/precise-server-cloudimg-amd64-disk1.img
    # wget -O ubuntu-12.04-i386.qcow2 https://cloud-images.ubuntu.com/precise/current/precise-server-cloudimg-i386-disk1.img
    # wget -O ubuntu-14.04.qcow2 https://cloud-images.ubuntu.com/trusty/current/trusty-server-cloudimg-amd64-disk1.img
    # wget -O ubuntu-14.04-i386.qcow2 https://cloud-images.ubuntu.com/trusty/current/trusty-server-cloudimg-i386-disk1.img
    # wget -O ubuntu-15.04.qcow2 https://cloud-images.ubuntu.com/vivid/current/vivid-server-cloudimg-arm64-disk1.img
    # wget -O ubuntu-15.04-i386.qcow2 https://cloud-images.ubuntu.com/vivid/current/vivid-server-cloudimg-i386-disk1.img
    # wget -O opensuse-13.2 http://download.opensuse.org/repositories/Cloud:/Images:/openSUSE_13.2/images/openSUSE-13.2-OpenStack-Guest.x86_64.qcow2
    # wget -O opensuse-42.1 http://download.opensuse.org/repositories/Cloud:/Images:/Leap_42.1/images/openSUSE-Leap-42.1-OpenStack.x86_64.qcow2
    # wget -O centos-7.0.qcow2 http://cloud.centos.org/centos/7/images/CentOS-7-x86_64-GenericCloud.qcow2
    # wget -O centos-6.6.qcow2 http://cloud.centos.org/centos/6/images/CentOS-6-x86_64-GenericCloud.qcow2
    # wget -O fedora-22.qcow2 https://download.fedoraproject.org/pub/fedora/linux/releases/22/Cloud/x86_64/Images/Fedora-Cloud-Base-22-20150521.x86_64.qcow2
    # wget -O fedora-21.qcow2 http://fedora.mirrors.ovh.net/linux/releases/21/Cloud/Images/x86_64/Fedora-Cloud-Base-20141203-21.x86_64.qcow2
    # wget -O fedora-20.qcow2 http://fedora.mirrors.ovh.net/linux/releases/20/Images/x86_64/Fedora-x86_64-20-20131211.1-sda.qcow2
    image2url = {
        'centos-6.5': 'http://cloud.centos.org/centos/6/images/CentOS-6-x86_64-GenericCloud-1508.qcow2',
        'centos-7.0': 'http://cloud.centos.org/centos/7/images/CentOS-7-x86_64-GenericCloud-1503.qcow2',
        'centos-7.1': 'http://cloud.centos.org/centos/7/images/CentOS-7-x86_64-GenericCloud-1503.qcow2',
        'centos-7.2': 'http://cloud.centos.org/centos/7/images/CentOS-7-x86_64-GenericCloud-1511.qcow2',
        'ubuntu-12.04': 'https://cloud-images.ubuntu.com/precise/current/precise-server-cloudimg-amd64-disk1.img',
        'ubuntu-14.04': 'https://cloud-images.ubuntu.com/trusty/current/trusty-server-cloudimg-amd64-disk1.img',
        'debian-8.0': 'http://cdimage.debian.org/cdimage/openstack/current/debian-8.2.0-openstack-amd64.qcow2',
    }

    def __init__(self):
        self.key_filename = None
        self.username = 'ubuntu'
        self.up_string = "UNKNOWN"
        self.teuthology_suite = 'teuthology-suite'

    def set_provider(self):
        if 'OS_AUTH_URL' not in os.environ:
            raise Exception('no OS_AUTH_URL environment variable')
        providers = (('cloud.ovh.net', 'ovh'),
                     ('entercloudsuite.com', 'entercloudsuite'),
                     ('rackspacecloud.com', 'rackspace'),
                     ('dream.io', 'dreamhost'))
        self.provider = None
        for (pattern, provider) in providers:
            if pattern in os.environ['OS_AUTH_URL']:
                self.provider = provider
                break
        return self.provider

    @staticmethod
    def get_value(result, field):
        """
        Get the value of a field from a result returned by the openstack
        command in json format.

        :param result:  A dictionary similar to the output of
                        'openstack server show'
        :param field:   The name of the field whose value to retrieve. Case is
                        ignored.
        """
        enforce_json_dictionary(result)
        return result[field.lower()]

    def image_exists(self, image):
        """
        Return true if the image exists in OpenStack.
        """
        found = misc.sh("openstack image list -f json --property name='" +
                        self.image_name(image) + "'")
        return len(json.loads(found)) > 0

    def net_id(self, network):
        """
        Return the uuid of the network in OpenStack.
        """
        r = json.loads(misc.sh("openstack network show -f json " +
                               network))
        return self.get_value(r, 'id')

    def type_version(self, os_type, os_version):
        """
        Return the string used to differentiate os_type and os_version in names.
        """
        return os_type + '-' + os_version

    def image_name(self, name):
        """
        Return the image name used by teuthology in OpenStack to avoid
        conflicts with existing names.
        """
        return "teuthology-" + name

    def image_create(self, name):
        """
        Upload an image into OpenStack with glance.
        """
        misc.sh("wget -c -O " + name + ".qcow2 " + self.image2url[name])
        self.set_provider()
        if self.provider == 'dreamhost':
            image = name + ".raw"
            disk_format = 'raw'
            misc.sh("qemu-img convert " + name + ".qcow2 " + image)
        else:
            image = name + ".qcow2"
            disk_format = 'qcow2'
        misc.sh("glance image-create --property ownedby=teuthology " +
                " --disk-format=" + disk_format + " --container-format=bare " +
                " --visibility private" +
                " --file " + image + " --name " + self.image_name(name))

    def image(self, os_type, os_version):
        """
        Return the image name for the given os_type and os_version. If the image
        does not exist it will be created.
        """
        name = self.type_version(os_type, os_version)
        if not self.image_exists(name):
            self.image_create(name)
        return self.image_name(name)

    def flavor(self, hint, select):
        """
        Return the smallest flavor that satisfies the desired size.
        """
        flavors_string = misc.sh("openstack flavor list -f json")
        flavors = json.loads(flavors_string)
        found = []
        for flavor in flavors:
            if select and not re.match(select, flavor['Name']):
                continue
            if (flavor['RAM'] >= hint['ram'] and
                    flavor['VCPUs'] >= hint['cpus'] and
                    flavor['Disk'] >= hint['disk']):
                found.append(flavor)
        if not found:
            raise Exception("openstack flavor list: " + flavors_string +
                            " does not contain a flavor in which" +
                            " the desired " + str(hint) + " can fit")

        def sort_flavor(a, b):
            return (a['VCPUs'] - b['VCPUs'] or
                    a['RAM'] - b['RAM'] or
                    a['Disk'] - b['Disk'])
        sorted_flavor = sorted(found, cmp=sort_flavor)
        log.debug("sorted flavor = " + str(sorted_flavor))
        return sorted_flavor[0]['Name']

    def interpret_hints(self, defaults, hints):
        """
        Return a hint hash which is the interpretation of a list of hints
        """
        result = copy.deepcopy(defaults)
        if not hints:
            return result
        if type(hints) is types.DictType:
            raise TypeError("openstack: " + str(hints) +
                            " must be an array, not a dict")
        for hint in hints:
            for resource in ('machine', 'volumes'):
                if resource in hint:
                    new = hint[resource]
                    current = result[resource]
                    for key, value in hint[resource].iteritems():
                        current[key] = max(current[key], new[key])
        return result

    @staticmethod
    def list_instances():
        ownedby = "ownedby='" + teuth_config.openstack['ip'] + "'"
        all = json.loads(misc.sh(
            "openstack server list -f json --long --name 'target'"))
        return filter(lambda instance: ownedby in instance['Properties'], all)

    @staticmethod
    def list_volumes():
        ownedby = "ownedby='" + teuth_config.openstack['ip'] + "'"
        all = json.loads(misc.sh(
            "openstack volume list -f json --long"))
        def select(volume):
            return (ownedby in volume['Properties'] and
                    volume['Display Name'].startswith('target'))
        return filter(select, all)

    def cloud_init_wait(self, instance):
        """
        Wait for cloud-init to complete on the name_or_ip OpenStack instance.
        """
        ip = instance.get_floating_ip_or_ip()
        log.debug('cloud_init_wait ' + ip)
        client_args = {
            'user_at_host': '@'.join((self.username, ip)),
            'timeout': 240,
            'retry': False,
        }
        if self.key_filename:
            log.debug("using key " + self.key_filename)
            client_args['key_filename'] = self.key_filename
        with safe_while(sleep=30, tries=30,
                        action="cloud_init_wait " + ip) as proceed:
            success = False
            # CentOS 6.6 logs in /var/log/clout-init-output.log
            # CentOS 7.0 logs in /var/log/clout-init.log
            tail = ("tail --follow=name --retry"
                        " /var/log/cloud-init*.log /tmp/init.out")
            while proceed():
                try:
                    client = connection.connect(**client_args)
                except paramiko.PasswordRequiredException as e:
                    raise Exception(
                        "The private key requires a passphrase.\n"
                        "Create a new key with:"
                        "  openstack keypair create myself > myself.pem\n"
                        "  chmod 600 myself.pem\n"
                        "and call teuthology-openstack with the options\n"
                        " --key-name myself --key-filename myself.pem\n")
                except paramiko.AuthenticationException as e:
                    log.debug('cloud_init_wait AuthenticationException ' + str(e))
                    continue
                except socket.timeout as e:
                    log.debug('cloud_init_wait connect socket.timeout ' + str(e))
                    continue
                except socket.error as e:
                    log.debug('cloud_init_wait connect socket.error ' + str(e))
                    continue
                except Exception as e:
                    transients = ('Incompatible ssh peer', 'Unknown server')
                    for transient in transients:
                        if transient in str(e):
                            continue
                    log.exception('cloud_init_wait ' + ip)
                    raise
                log.debug('cloud_init_wait ' + tail)
                try:
                    # get the I/O channel to iterate line by line
                    transport = client.get_transport()
                    channel = transport.open_session()
                    channel.get_pty()
                    channel.settimeout(240)
                    output = channel.makefile('r', 1)
                    channel.exec_command(tail)
                    for line in iter(output.readline, b''):
                        log.info(line.strip())
                        if self.up_string in line:
                            success = True
                            break
                except socket.timeout as e:
                    client.close()
                    log.debug('cloud_init_wait socket.timeout ' + tail)
                    continue
                except socket.error as e:
                    client.close()
                    log.debug('cloud_init_wait socket.error ' + str(e) + ' ' + tail)
                    continue
                client.close()
                if success:
                    break
            return success

    def get_ip(self, instance_id, network):
        return OpenStackInstance(instance_id).get_ip(network)


class TeuthologyOpenStack(OpenStack):

    def __init__(self, args, config, argv):
        """
        args is of type argparse.Namespace as returned
        when parsing argv and config is the job
        configuration. The argv argument can be re-used
        to build the arguments list of teuthology-suite.
        """
        super(TeuthologyOpenStack, self).__init__()
        self.argv = argv
        self.args = args
        self.config = config
        self.up_string = 'teuthology is up and running'
        self.user_data = 'teuthology/openstack/openstack-user-data.txt'

    def main(self):
        """
        Entry point implementing the teuthology-openstack command.
        """
        self.setup_logs()
        set_config_attr(self.args)
        self.key_filename = self.args.key_filename
        self.verify_openstack()
        self.setup()
        exit_code = 0
        if self.args.suite:
            if self.args.wait:
                self.reminders()
            exit_code = self.run_suite()
            self.reminders()
        if self.args.teardown:
            if self.args.suite and not self.args.wait:
                log.error("it does not make sense to teardown a cluster"
                          " right after a suite is scheduled")
            else:
                self.teardown()
        return exit_code

    def run_suite(self):
        """
        Delegate running teuthology-suite to the OpenStack instance
        running the teuthology cluster.
        """
        original_argv = self.argv[:]
        argv = []
        while len(original_argv) > 0:
            if original_argv[0] in ('--name',
                                    '--teuthology-branch',
                                    '--teuthology-git-url',
                                    '--archive-upload',
                                    '--archive-upload-url',
                                    '--key-name',
                                    '--key-filename',
                                    '--simultaneous-jobs',
                                    '--ceph-git-url',
                                    '--ceph-qa-suite-git-url'):
                del original_argv[0:2]
            elif original_argv[0] in ('--teardown',
                                      '--upload'):
                del original_argv[0]
            else:
                argv.append(original_argv.pop(0))
        #
        # If --upload, provide --archive-upload{,-url} regardless of
        # what was originally provided on the command line because the
        # teuthology-openstack defaults are different from the
        # teuthology-suite defaults.
        #
        if self.args.upload:
            argv.extend(['--archive-upload', self.args.archive_upload,
                         '--archive-upload-url', self.args.archive_upload_url])
        for arg in ('ceph_git_url', 'ceph_qa_suite_git_url'):
            if getattr(self.args, arg):
                command = (
                    "perl -pi -e 's|.*{arg}.*|{arg}: {value}|'"
                    " ~/.teuthology.yaml"
                ).format(arg=arg, value=getattr(self.args, arg))
                self.ssh(command)
        argv.append('/home/' + self.username +
                    '/teuthology/teuthology/openstack/openstack.yaml')
        command = (
            "source ~/.bashrc_teuthology ; " + self.teuthology_suite + " " +
            " --machine-type openstack " +
            " ".join(map(lambda x: "'" + x + "'", argv))
        )
        return self.ssh(command)

    def reminders(self):
        if self.args.key_filename:
            identity = '-i ' + self.args.key_filename + ' '
        else:
            identity = ''
        if self.args.upload:
            upload = 'upload to            : ' + self.args.archive_upload
        else:
            upload = ''
        log.info("""
pulpito web interface: http://{ip}:8081/
ssh access           : ssh {identity}{username}@{ip} # logs in /usr/share/nginx/html
{upload}""".format(ip=self.instance.get_floating_ip_or_ip(),
                   username=self.username,
                   identity=identity,
                   upload=upload))

    def setup(self):
        self.instance = OpenStackInstance(self.args.name)
        if not self.instance.exists():
            if self.provider != 'rackspace':
                self.create_security_group()
            self.create_cluster()

    def setup_logs(self):
        """
        Setup the log level according to --verbose
        """
        loglevel = logging.INFO
        if self.args.verbose:
            loglevel = logging.DEBUG
            logging.getLogger("paramiko.transport").setLevel(logging.DEBUG)
        teuthology.log.setLevel(loglevel)

    def ssh(self, command):
        """
        Run a command in the OpenStack instance of the teuthology cluster.
        Return the stdout / stderr of the command.
        """
        ip = self.instance.get_floating_ip_or_ip()
        client_args = {
            'user_at_host': '@'.join((self.username, ip)),
            'retry': False,
            'timeout': 240,
        }
        if self.key_filename:
            log.debug("ssh overriding key with " + self.key_filename)
            client_args['key_filename'] = self.key_filename
        client = connection.connect(**client_args)
        # get the I/O channel to iterate line by line
        transport = client.get_transport()
        channel = transport.open_session()
        channel.get_pty()
        channel.settimeout(900)
        output = channel.makefile('r', 1)
        log.debug(":ssh@" + ip + ":" + command)
        channel.exec_command(command)
        for line in iter(output.readline, b''):
            log.info(line.strip())
        return channel.recv_exit_status()

    def verify_openstack(self):
        """
        Check there is a working connection to an OpenStack cluster
        and set the provider data member if it is among those we
        know already.
        """
        try:
            misc.sh("openstack flavor list | tail -2")
        except subprocess.CalledProcessError:
            log.exception("openstack flavor list")
            raise Exception("verify openrc.sh has been sourced")
        self.set_provider()

    def flavor(self):
        """
        Return an OpenStack flavor fit to run the teuthology cluster.
        The RAM size depends on the maximum number of workers that
        will run simultaneously.
        """
        hint = {
            'disk': 10, # GB
            'ram': 1024, # MB
            'cpus': 1,
        }
        if self.args.simultaneous_jobs >= 100:
            hint['ram'] = 30000 # MB
        elif self.args.simultaneous_jobs >= 25:
            hint['ram'] = 8000 # MB
        elif self.args.simultaneous_jobs >= 10:
            hint['ram'] = 4000 # MB

        select = None
        if self.provider == 'ovh':
            select = '^(vps|eg)-'
        return super(TeuthologyOpenStack, self).flavor(hint, select)

    def net(self):
        """
        Return the network to be used when creating an OpenStack instance.
        By default it should not be set. But some providers such as
        entercloudsuite require it is.
        """
        if self.provider == 'entercloudsuite':
            return "--nic net-id=default"
        else:
            return ""

    def get_user_data(self):
        """
        Create a user-data.txt file to be used to spawn the teuthology
        cluster, based on a template where the OpenStack credentials
        and a few other values are substituted.
        """
        path = tempfile.mktemp()
        if self.user_data.startswith('/'):
            user_data = self.user_data
        else:
            user_data = os.path.join(os.path.dirname(__file__),
                                     '../..', self.user_data)
        template = open(user_data).read()
        openrc = ''
        for (var, value) in os.environ.iteritems():
            if var.startswith('OS_'):
                openrc += ' ' + var + '=' + value
        if self.args.upload:
            upload = '--archive-upload ' + self.args.archive_upload
        else:
            upload = ''
        clone = teuth_config.openstack['clone']
        if self.args.teuthology_git_url:
            clone = ("git clone -b {branch} {url}".format(
                branch=self.args.teuthology_branch,
                url=self.args.teuthology_git_url))
        log.debug("OPENRC = " + openrc + " " +
                  "TEUTHOLOGY_USERNAME = " + self.username + " " +
                  "CLONE_OPENSTACK = " + clone + " " +
                  "UPLOAD = " + upload + " " +
                  "NWORKERS = " + str(self.args.simultaneous_jobs))
        content = (template.
                   replace('OPENRC', openrc).
                   replace('TEUTHOLOGY_USERNAME', self.username).
                   replace('CLONE_OPENSTACK', clone).
                   replace('UPLOAD', upload).
                   replace('NWORKERS', str(self.args.simultaneous_jobs)))
        open(path, 'w').write(content)
        log.debug("get_user_data: " + content + " written to " + path)
        return path

    def create_security_group(self):
        """
        Create a security group that will be used by all teuthology
        created instances. This should not be necessary in most cases
        but some OpenStack providers enforce firewall restrictions even
        among instances created within the same tenant.
        """
        try:
            misc.sh("openstack security group show teuthology")
            return
        except subprocess.CalledProcessError:
            pass
        # TODO(loic): this leaves the teuthology vm very exposed
        # it would be better to be very liberal for 192.168.0.0/16
        # and 172.16.0.0/12 and 10.0.0.0/8 and only allow 80/8081/22
        # for the rest.
        misc.sh("""
openstack security group create teuthology
openstack security group rule create --dst-port 1:65535 teuthology
openstack security group rule create --proto udp --dst-port 53 teuthology # dns
        """)

    @staticmethod
    def get_unassociated_floating_ip():
        """
        Return a floating IP address not associated with an instance or None.
        """
        ips = json.loads(misc.sh("openstack ip floating list -f json"))
        for ip in ips:
            if not ip['Instance ID']:
                return ip['IP']
        return None

    @staticmethod
    def create_floating_ip():
        pools = json.loads(misc.sh("openstack ip floating pool list -f json"))
        if not pools:
            return None
        pool = pools[0]['Name']
        try:
            ip = json.loads(misc.sh(
                "openstack ip floating create -f json '" + pool + "'"))
            return TeuthologyOpenStack.get_value(ip, 'ip')
        except subprocess.CalledProcessError:
            log.debug("create_floating_ip: not creating a floating ip")
            pass
        return None

    @staticmethod
    def associate_floating_ip(name_or_id):
        """
        Associate a floating IP to the OpenStack instance
        or do nothing if no floating ip can be created.
        """
        ip = TeuthologyOpenStack.get_unassociated_floating_ip()
        if not ip:
            ip = TeuthologyOpenStack.create_floating_ip()
        if ip:
            misc.sh("openstack ip floating add " + ip + " " + name_or_id)

    @staticmethod
    def get_floating_ip_id(ip):
        """
        Return the id of a floating IP
        """
        results = json.loads(misc.sh("openstack ip floating list -f json"))
        for result in results:
            if result['IP'] == ip:
                return str(result['ID'])
        return None

    @staticmethod
    def get_instance_id(name):
        return OpenStackInstance(name)['id']

    @staticmethod
    def delete_floating_ip(instance_id):
        """
        Remove the floating ip from instance_id and delete it.
        """
        ip = OpenStackInstance(instance_id).get_floating_ip()
        if not ip:
            return
        misc.sh("openstack ip floating remove " + ip + " " + instance_id)
        ip_id = TeuthologyOpenStack.get_floating_ip_id(ip)
        misc.sh("openstack ip floating delete " + ip_id)

    def create_cluster(self):
        user_data = self.get_user_data()
        if self.provider == 'rackspace':
            security_group = ''
        else:
            security_group = " --security-group teuthology"
        misc.sh(
            "openstack server create " +
            " --image '" + self.image('ubuntu', '14.04') + "' " +
            " --flavor '" + self.flavor() + "' " +
            " " + self.net() +
            " --key-name " + self.args.key_name +
            " --user-data " + user_data +
            security_group +
            " --wait " + self.args.name +
            " -f json")
        os.unlink(user_data)
        self.instance = OpenStackInstance(self.args.name)
        self.associate_floating_ip(self.instance['id'])
        return self.cloud_init_wait(self.instance)

    def teardown(self):
        """
        Delete all instances run by the teuthology cluster and delete the
        instance running the teuthology cluster.
        """
        self.ssh("sudo /etc/init.d/teuthology stop || true")
        instance_id = self.get_instance_id(self.args.name)
        self.delete_floating_ip(instance_id)
        misc.sh("openstack server delete packages-repository || true")
        misc.sh("openstack server delete --wait " + self.args.name)

def main(ctx, argv):
    return TeuthologyOpenStack(ctx, teuth_config, argv).main()
