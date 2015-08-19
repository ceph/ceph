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
import json
import logging
import os
import paramiko
import re
import socket
import subprocess
import tempfile
import teuthology

from teuthology.contextutil import safe_while
from teuthology.config import config as teuth_config
from teuthology import misc

log = logging.getLogger(__name__)

class OpenStack(object):

    # wget -O debian-8.0.qcow2  http://cdimage.debian.org/cdimage/openstack/current/debian-8.1.0-openstack-amd64.qcow2
    # wget -O ubuntu-12.04.qcow2 https://cloud-images.ubuntu.com/precise/current/precise-server-cloudimg-amd64-disk1.img
    # wget -O ubuntu-12.04-i386.qcow2 https://cloud-images.ubuntu.com/precise/current/precise-server-cloudimg-i386-disk1.img
    # wget -O ubuntu-14.04.qcow2 https://cloud-images.ubuntu.com/trusty/current/trusty-server-cloudimg-amd64-disk1.img
    # wget -O ubuntu-14.04-i386.qcow2 https://cloud-images.ubuntu.com/trusty/current/trusty-server-cloudimg-i386-disk1.img
    # wget -O ubuntu-15.04.qcow2 https://cloud-images.ubuntu.com/vivid/current/vivid-server-cloudimg-arm64-disk1.img
    # wget -O ubuntu-15.04-i386.qcow2 https://cloud-images.ubuntu.com/vivid/current/vivid-server-cloudimg-i386-disk1.img
    # wget -O opensuse-13.2 http://download.opensuse.org/repositories/Cloud:/Images:/openSUSE_13.2/images/openSUSE-13.2-OpenStack-Guest.x86_64.qcow2
    # wget -O centos-7.0.qcow2 http://cloud.centos.org/centos/7/images/CentOS-7-x86_64-GenericCloud.qcow2
    # wget -O centos-6.6.qcow2 http://cloud.centos.org/centos/6/images/CentOS-6-x86_64-GenericCloud.qcow2
    # wget -O fedora-22.qcow2 https://download.fedoraproject.org/pub/fedora/linux/releases/22/Cloud/x86_64/Images/Fedora-Cloud-Base-22-20150521.x86_64.qcow2
    # wget -O fedora-21.qcow2 http://fedora.mirrors.ovh.net/linux/releases/21/Cloud/Images/x86_64/Fedora-Cloud-Base-20141203-21.x86_64.qcow2
    # wget -O fedora-20.qcow2 http://fedora.mirrors.ovh.net/linux/releases/20/Images/x86_64/Fedora-x86_64-20-20131211.1-sda.qcow2
    image2url = {
        'centos-6.5': 'http://cloud.centos.org/centos/6/images/CentOS-6-x86_64-GenericCloud.qcow2',
        'centos-7.0': 'http://cloud.centos.org/centos/7/images/CentOS-7-x86_64-GenericCloud-20150628_01.qcow2',
        'ubuntu-14.04': 'https://cloud-images.ubuntu.com/trusty/current/trusty-server-cloudimg-amd64-disk1.img',
    }

    def __init__(self):
        self.key_filename = None
        self.username = 'ubuntu'
        self.up_string = "UNKNOWN"
        self.teuthology_suite = 'teuthology-suite'

    @staticmethod
    def get_value(result, field):
        """
        Get the value of a field from a result returned by the openstack command
        in json format.
        """
        return filter(lambda v: v['Field'] == field, result)[0]['Value']

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
        Upload an image into OpenStack with glance. The image has to be qcow2.
        """
        misc.sh("wget -c -O " + name + ".qcow2 " + self.image2url[name])
        misc.sh("glance image-create --property ownedby=teuthology " +
                " --disk-format=qcow2 --container-format=bare " +
                " --file " + name + ".qcow2 --name " + self.image_name(name))

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

    def cloud_init_wait(self, name_or_ip):
        """
        Wait for cloud-init to complete on the name_or_ip OpenStack instance.
        """
        log.debug('cloud_init_wait ' + name_or_ip)
        client_args = {
            'timeout': 10,
            'username': self.username,
        }
        if self.key_filename:
            log.debug("using key " + self.key_filename)
            client_args['key_filename'] = self.key_filename
        with safe_while(sleep=2, tries=600,
                        action="cloud_init_wait " + name_or_ip) as proceed:
            success = False
            # CentOS 6.6 logs in /var/log/clout-init-output.log
            # CentOS 7.0 logs in /var/log/clout-init.log
            all_done = ("tail /var/log/cloud-init*.log ; " +
                        " test -f /tmp/init.out && tail /tmp/init.out ; " +
                        " grep '" + self.up_string + "' " +
                        "/var/log/cloud-init*.log")
            while proceed():
                client = paramiko.SSHClient()
                try:
                    client.set_missing_host_key_policy(
                        paramiko.AutoAddPolicy())
                    client.connect(name_or_ip, **client_args)
                except paramiko.PasswordRequiredException as e:
                    client.close()
                    raise Exception(
                        "The private key requires a passphrase.\n"
                        "Create a new key with:"
                        "  openstack keypair create myself > myself.pem\n"
                        "  chmod 600 myself.pem\n"
                        "and call teuthology-openstack with the options\n"
                        " --key-name myself --key-filename myself.pem\n")
                except paramiko.AuthenticationException as e:
                    client.close()
                    log.debug('cloud_init_wait AuthenticationException ' + str(e))
                    continue
                except socket.timeout as e:
                    client.close()
                    log.debug('cloud_init_wait connect socket.timeout ' + str(e))
                    continue
                except socket.error as e:
                    client.close()
                    log.debug('cloud_init_wait connect socket.error ' + str(e))
                    continue
                except Exception as e:
                    if 'Unknown server' not in str(e):
                        log.exception('cloud_init_wait ' + name_or_ip)
                    client.close()
                    if 'Unknown server' in str(e):
                        continue
                    else:
                        raise e
                log.debug('cloud_init_wait ' + all_done)
                try:
                    stdin, stdout, stderr = client.exec_command(all_done)
                    stdout.channel.settimeout(5)
                    out = stdout.read()
                    log.debug('cloud_init_wait stdout ' + all_done + ' ' + out)
                except socket.timeout as e:
                    client.close()
                    log.debug('cloud_init_wait socket.timeout ' + all_done)
                    continue
                except socket.error as e:
                    client.close()
                    log.debug('cloud_init_wait socket.error ' + str(e) + ' ' + all_done)
                    continue
                log.debug('cloud_init_wait stderr ' + all_done +
                          ' ' + stderr.read())
                if stdout.channel.recv_exit_status() == 0:
                    success = True
                client.close()
                if success:
                    break
            return success

    def exists(self, name_or_id):
        """
        Return true if the OpenStack name_or_id instance exists,
        false otherwise.
        """
        servers = json.loads(misc.sh("openstack server list -f json"))
        for server in servers:
            if (server['ID'] == name_or_id or server['Name'] == name_or_id):
                return True
        return False

    @staticmethod
    def get_addresses(instance_id):
        """
        Return the list of IPs associated with instance_id in OpenStack.
        """
        with safe_while(sleep=2, tries=30,
                        action="get ip " + instance_id) as proceed:
            while proceed():
                instance = misc.sh("openstack server show -f json " +
                                   instance_id)
                addresses = OpenStack.get_value(json.loads(instance),
                                                'addresses')
                found = re.match('.*\d+', addresses)
                if found:
                    return addresses

    def get_ip(self, instance_id, network):
        """
        Return the private IP of the OpenStack instance_id. The network,
        if not the empty string, disambiguate multiple networks attached
        to the instance.
        """
        return re.findall(network + '=([\d.]+)',
                          self.get_addresses(instance_id))[0]

class TeuthologyOpenStack(OpenStack):

    def __init__(self, args, config, argv):
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
        misc.read_config(self.args)
        self.key_filename = self.args.key_filename
        self.verify_openstack()
        ip = self.setup()
        if self.args.suite:
            self.run_suite()
        if self.args.key_filename:
            identity = '-i ' + self.args.key_filename + ' '
        else:
            identity = ''
        if self.args.upload:
            upload = 'upload to    : ' + self.args.archive_upload
        else:
            upload = ''
        log.info("""
web interface: http://{ip}:8081/
ssh access   : ssh {identity}{username}@{ip} # logs in /usr/share/nginx/html
{upload}""".format(ip=ip,
                   username=self.username,
                   identity=identity,
                   upload=upload))
        if self.args.teardown:
            self.teardown()

    def run_suite(self):
        """
        Delegate running teuthology-suite to the OpenStack instance
        running the teuthology cluster.
        """
        original_argv = self.argv[:]
        argv = []
        while len(original_argv) > 0:
            if original_argv[0] in ('--name',
                                    '--archive-upload',
                                    '--key-name',
                                    '--key-filename',
                                    '--simultaneous-jobs'):
                del original_argv[0:2]
            elif original_argv[0] in ('--teardown',
                                      '--upload'):
                del original_argv[0]
            else:
                argv.append(original_argv.pop(0))
        argv.append('/home/' + self.username +
                    '/teuthology/teuthology/openstack/test/openstack.yaml')
        command = (
            "source ~/.bashrc_teuthology ; " + self.teuthology_suite + " " +
            " --machine-type openstack " +
            " ".join(map(lambda x: "'" + x + "'", argv))
        )
        print self.ssh(command)

    def setup(self):
        """
        Create the teuthology cluster if it does not already exists
        and return its IP address.
        """
        if not self.cluster_exists():
            self.create_security_group()
            self.create_cluster()
        instance_id = self.get_instance_id(self.args.name)
        return self.get_floating_ip_or_ip(instance_id)

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
        client_args = {
            'username': self.username,
        }
        if self.key_filename:
            log.debug("ssh using key " + self.key_filename)
            client_args['key_filename'] = self.key_filename
        instance_id = self.get_instance_id(self.args.name)
        ip = self.get_floating_ip_or_ip(instance_id)
        log.debug("ssh " + self.username + "@" + str(ip) + " " + command)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(
            paramiko.AutoAddPolicy())
        client.connect(ip, **client_args)
        stdin, stdout, stderr = client.exec_command(command)
        stdout.channel.settimeout(300)
        out = ''
        try:
            out = stdout.read()
            log.debug('teardown stdout ' + command + ' ' + out)
        except Exception:
            log.exception('teardown ' + command + ' failed')
        err = stderr.read()
        log.debug('teardown stderr ' + command + ' ' + err)
        return out + ' ' + err

    def verify_openstack(self):
        """
        Check there is a working connection to an OpenStack cluster
        and set the provider data member if it is among those we
        know already.
        """
        try:
            misc.sh("openstack server list")
        except subprocess.CalledProcessError:
            log.exception("openstack server list")
            raise Exception("verify openrc.sh has been sourced")
        if 'OS_AUTH_URL' not in os.environ:
            raise Exception('no OS_AUTH_URL environment variable')
        providers = (('cloud.ovh.net', 'ovh'),
                     ('entercloudsuite.com', 'entercloudsuite'))
        self.provider = None
        for (pattern, provider) in providers:
            if pattern in os.environ['OS_AUTH_URL']:
                self.provider = provider
                break

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
        if self.args.simultaneous_jobs > 25:
            hint['ram'] = 30000 # MB
        elif self.args.simultaneous_jobs > 10:
            hint['ram'] = 7000 # MB
        elif self.args.simultaneous_jobs > 3:
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
        template = open(self.user_data).read()
        openrc = ''
        for (var, value) in os.environ.iteritems():
            if var.startswith('OS_'):
                openrc += ' ' + var + '=' + value
        if self.args.upload:
            upload = '--archive-upload ' + self.args.archive_upload
        else:
            upload = ''
        log.debug("OPENRC = " + openrc + " " +
                  "TEUTHOLOGY_USERNAME = " + self.username + " " +
                  "UPLOAD = " + upload + " " +
                  "NWORKERS = " + str(self.args.simultaneous_jobs))
        content = (template.
                   replace('OPENRC', openrc).
                   replace('TEUTHOLOGY_USERNAME', self.username).
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
openstack security group rule create --dst-port 1:10000 teuthology
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
    def get_floating_ip(instance_id):
        """
        Return the floating IP of the OpenStack instance_id.
        """
        ips = json.loads(misc.sh("openstack ip floating list -f json"))
        for ip in ips:
            if ip['Instance ID'] == instance_id:
                return ip['IP']
        return None

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
    def get_floating_ip_or_ip(instance_id):
        """
        Return the floating ip, if any, otherwise return the last
        IP displayed with openstack server list.
        """
        ip = TeuthologyOpenStack.get_floating_ip(instance_id)
        if not ip:
            ip = re.findall('([\d.]+)$',
                            TeuthologyOpenStack.get_addresses(instance_id))[0]
        return ip

    @staticmethod
    def get_instance_id(name):
        instance = json.loads(misc.sh("openstack server show -f json " + name))
        return TeuthologyOpenStack.get_value(instance, 'id')

    @staticmethod
    def delete_floating_ip(instance_id):
        """
        Remove the floating ip from instance_id and delete it.
        """
        ip = TeuthologyOpenStack.get_floating_ip(instance_id)
        if not ip:
            return
        misc.sh("openstack ip floating remove " + ip + " " + instance_id)
        ip_id = TeuthologyOpenStack.get_floating_ip_id(ip)
        misc.sh("openstack ip floating delete " + ip_id)

    def create_cluster(self):
        """
        Create an OpenStack instance that runs the teuthology cluster
        and wait for it to come up.
        """
        user_data = self.get_user_data()
        instance = misc.sh(
            "openstack server create " +
            " --image '" + self.image('ubuntu', '14.04') + "' " +
            " --flavor '" + self.flavor() + "' " +
            " " + self.net() +
            " --key-name " + self.args.key_name +
            " --user-data " + user_data +
            " --security-group teuthology" +
            " --wait " + self.args.name +
            " -f json")
        instance_id = self.get_value(json.loads(instance), 'id')
        os.unlink(user_data)
        self.associate_floating_ip(instance_id)
        ip = self.get_floating_ip_or_ip(instance_id)
        return self.cloud_init_wait(ip)

    def cluster_exists(self):
        """
        Return true if there exists an instance running the teuthology cluster.
        """
        if not self.exists(self.args.name):
            return False
        instance_id = self.get_instance_id(self.args.name)
        ip = self.get_floating_ip_or_ip(instance_id)
        return self.cloud_init_wait(ip)

    def teardown(self):
        """
        Delete all instances run by the teuthology cluster and delete the
        instance running the teuthology cluster.
        """
        self.ssh("sudo /etc/init.d/teuthology stop || true")
        instance_id = self.get_instance_id(self.args.name)
        self.delete_floating_ip(instance_id)
        misc.sh("openstack server delete --wait " + self.args.name)

def main(ctx, argv):
    return TeuthologyOpenStack(ctx, teuth_config, argv).main()
