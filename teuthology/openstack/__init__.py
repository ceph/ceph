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
import functools
import json
import logging
import operator
import os
import paramiko
import re
import socket
import subprocess
import tempfile
import teuthology
import time
import types
import yaml
import base64

from subprocess import CalledProcessError

from teuthology.contextutil import safe_while
from teuthology.config import config as teuth_config
from teuthology.config import set_config_attr
from teuthology.orchestra import connection
from teuthology import misc

from yaml.representer import SafeRepresenter

class cmd_str(str): pass

def cmd_repr(dumper, data):
    scalar = SafeRepresenter.represent_str(dumper, data)
    scalar.style ='|'
    return scalar

yaml.add_representer(cmd_str, cmd_repr)

log = logging.getLogger(__name__)

class NoFlavorException(Exception):
    pass

def enforce_json_dictionary(something):
    if not isinstance(something, dict):
        raise Exception(
            'Please pip uninstall --yes cliff-tablib and try again.'
            ' Details about this error can be found at'
            ' https://bugs.launchpad.net/python-openstackclient/+bug/1510546'
            ' you are encouraged to add a comment if you want it to be'
            ' fixed.')

class OpenStackInstance(object):

    def __init__(self, name_or_id, info=None):
        self.name_or_id = name_or_id
        self.private_or_floating_ip = None
        self.private_ip = None
        if info is None:
            self.set_info()
        else:
            self.info = {k.lower(): v for k, v in info.items()}
        if isinstance(self.info, dict) and self.info.get('status', '') == 'ERROR':
            errmsg = 'VM creation failed'
            if 'message' in self.info:
                errmsg = '{}: {}'.format(errmsg, self.info['message'])
            raise Exception(errmsg)

    def set_info(self):
        try:
            self.info = json.loads(
                OpenStack().run("server show -f json " + self.name_or_id))
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
        subnets = json.loads(misc.sh("unset OS_AUTH_TYPE OS_TOKEN ; "
                                     "neutron subnet-list -f json -c id -c ip_version"))
        subnet_ids = []
        for subnet in subnets:
            if subnet['ip_version'] == 4:
                subnet_ids.append(subnet['id'])
        if not subnet_ids:
            raise Exception("no subnet with ip_version == 4")
        ports = json.loads(misc.sh("unset OS_AUTH_TYPE OS_TOKEN ; "
                                   "neutron port-list -f json -c fixed_ips -c device_id"))
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
            if record['subnet_id'] in subnet_ids:
                ip = record['ip_address']
                break
        if not ip:
            raise Exception("no ip")
        return ip

    def get_ip(self, network):
        """
        Return the private IP of the OpenStack instance_id.
        """
        if self.private_ip is None:
            try:
                self.private_ip = self.get_ip_neutron()
            except Exception as e:
                log.debug("ignoring get_ip_neutron exception " + str(e))
                self.private_ip = re.findall(network + '=([\d.]+)',
                                             self.get_addresses())[0]
        return self.private_ip

    def get_floating_ip(self):
        ips = TeuthologyOpenStack.get_os_floating_ips()
        for ip in ips:
            if ip['Fixed IP Address'] == self.get_ip(''):
                return ip['Floating IP Address']
        return None

    def get_floating_ip_or_ip(self):
        if not self.private_or_floating_ip:
            self.private_or_floating_ip = self.get_floating_ip()
            if not self.private_or_floating_ip:
                self.private_or_floating_ip = self.get_ip('')
        return self.private_or_floating_ip

    def destroy(self):
        """
        Delete the name_or_id OpenStack instance.
        """
        if not self.exists():
            return True
        volumes = self.get_volumes()
        OpenStack().run("server set --name REMOVE-ME-" + self.name_or_id +
                        " " + self['id'])
        OpenStack().run("server delete --wait " + self['id'] +
                        " || true")
        for volume in volumes:
            OpenStack().volume_delete(volume)
        return True


class OpenStack(object):

    # http://cdimage.debian.org/cdimage/openstack/current/
    # https://cloud-images.ubuntu.com/precise/current/precise-server-cloudimg-amd64-disk1.img etc.
    # http://download.opensuse.org/repositories/Cloud:/Images:/openSUSE_13.2/images/openSUSE-13.2-OpenStack-Guest.x86_64.qcow2
    # http://cloud.centos.org/centos/7/images/CentOS-7-x86_64-GenericCloud.qcow2 etc.
    # http://cloud.centos.org/centos/6/images/CentOS-6-x86_64-GenericCloud.qcow2 etc.
    # https://download.fedoraproject.org/pub/fedora/linux/releases/22/Cloud/x86_64/Images/Fedora-Cloud-Base-22-20150521.x86_64.qcow2
    # http://fedora.mirrors.ovh.net/linux/releases/21/Cloud/Images/x86_64/Fedora-Cloud-Base-20141203-21.x86_64.qcow2
    # http://fedora.mirrors.ovh.net/linux/releases/20/Images/x86_64/Fedora-x86_64-20-20131211.1-sda.qcow2
    image2url = {
        'centos-7.2-x86_64': 'http://cloud.centos.org/centos/7/images/CentOS-7-x86_64-GenericCloud-1511.qcow2',
        'centos-7.3-x86_64': 'http://cloud.centos.org/centos/7/images/CentOS-7-x86_64-GenericCloud-1701.qcow2',
        'opensuse-42.1-x86_64': 'http://download.opensuse.org/repositories/Cloud:/Images:/Leap_42.1/images/openSUSE-Leap-42.1-OpenStack.x86_64.qcow2',
        'opensuse-42.2-x86_64': 'http://download.opensuse.org/repositories/Cloud:/Images:/Leap_42.2/images/openSUSE-Leap-42.2-OpenStack.x86_64.qcow2',
        'opensuse-42.3-x86_64': 'http://download.opensuse.org/repositories/Cloud:/Images:/Leap_42.3/images/openSUSE-Leap-42.3-OpenStack.x86_64.qcow2',
        'ubuntu-14.04-x86_64': 'https://cloud-images.ubuntu.com/trusty/current/trusty-server-cloudimg-amd64-disk1.img',
        'ubuntu-14.04-aarch64': 'https://cloud-images.ubuntu.com/trusty/current/trusty-server-cloudimg-arm64-disk1.img',
        'ubuntu-14.04-i686': 'https://cloud-images.ubuntu.com/trusty/current/trusty-server-cloudimg-i386-disk1.img',
        'ubuntu-16.04-x86_64': 'https://cloud-images.ubuntu.com/xenial/current/xenial-server-cloudimg-amd64-disk1.img',
        'ubuntu-16.04-aarch64': 'https://cloud-images.ubuntu.com/xenial/current/xenial-server-cloudimg-arm64-disk1.img',
        'ubuntu-16.04-i686': 'https://cloud-images.ubuntu.com/xenial/current/xenial-server-cloudimg-i386-disk1.img',
        'debian-8.0-x86_64': 'http://cdimage.debian.org/cdimage/openstack/current/debian-8.7.1-20170215-openstack-amd64.qcow2',
    }

    def __init__(self):
        self.provider = None
        self.key_filename = None
        self.username = 'ubuntu'
        self.up_string = "UNKNOWN"
        self.teuthology_suite = 'teuthology-suite'

    token = None
    token_expires = None
    token_cache_duration = 3600

    def cache_token(self):
        if self.provider != 'ovh':
            return False
        if (OpenStack.token is None and
            'OS_TOKEN_VALUE' in os.environ and
            'OS_TOKEN_EXPIRES' in os.environ):
            log.debug("get token from the environment of the parent process")
            OpenStack.token = os.environ['OS_TOKEN_VALUE']
            OpenStack.token_expires = int(os.environ['OS_TOKEN_EXPIRES'])
        if (OpenStack.token_expires is not None and
            OpenStack.token_expires < time.time()):
            log.debug("token discarded because it has expired")
            OpenStack.token = None
        if OpenStack.token is None:
            if 'OS_TOKEN_VALUE' in os.environ:
                del os.environ['OS_TOKEN_VALUE']
            OpenStack.token = misc.sh("openstack -q token issue -c id -f value").strip()
            os.environ['OS_TOKEN_VALUE'] = OpenStack.token
            OpenStack.token_expires = int(time.time() + OpenStack.token_cache_duration)
            os.environ['OS_TOKEN_EXPIRES'] = str(OpenStack.token_expires)
            log.debug("caching OS_TOKEN_VALUE "
                      "during %s seconds" % OpenStack.token_cache_duration)
        return True

    def get_os_url(self, cmd, type=None):
        if self.provider != 'ovh':
            return ""
        url = ""
        if (type == 'compute' or
            cmd.startswith("server ") or
            cmd.startswith("flavor ")):
            url = "https://compute.{reg}.cloud.ovh.net/v2/{tenant}"
        elif (type == 'network' or
              cmd.startswith("ip ") or
              cmd.startswith("security ") or
              cmd.startswith("network ")):
            url = "https://network.compute.{reg}.cloud.ovh.net/"
        elif (type == 'image' or
              cmd.startswith("image ")):
            url = "https://image.compute.{reg}.cloud.ovh.net/"
        elif (type == 'volume' or
              cmd.startswith("volume ")):
            url = "https://volume.compute.{reg}.cloud.ovh.net/v2/{tenant}"
        if url != "":
            url = url.format(reg=os.environ['OS_REGION_NAME'],
                             tenant=os.environ['OS_TENANT_ID'])
        return url
        
    def run(self, cmd, *args, **kwargs):
        url = self.get_os_url(cmd, kwargs.get('type'))
        if url != "":
            if self.cache_token():
                os.environ['OS_TOKEN'] = os.environ['OS_TOKEN_VALUE']
                os.environ['OS_URL'] = url
        if re.match('(server|flavor|ip|security|network|image|volume)', cmd):
            cmd = "openstack --quiet " + cmd
        try:
            status = misc.sh(cmd)
        finally:
            if 'OS_TOKEN' in os.environ:
                del os.environ['OS_TOKEN']
            if 'OS_URL' in os.environ:
                del os.environ['OS_URL']
        return status
    
    def set_provider(self):
        if 'OS_AUTH_URL' not in os.environ:
            raise Exception('no OS_AUTH_URL environment variable')
        providers = (('runabove.io', 'runabove'),
                     ('cloud.ovh.net', 'ovh'),
                     ('engcloud.prv.suse.net', 'ecp'),
                     ('cloudlab.us', 'cloudlab'),
                     ('entercloudsuite.com', 'entercloudsuite'),
                     ('rackspacecloud.com', 'rackspace'),
                     ('dream.io', 'dreamhost'))
        self.provider = 'any'
        for (pattern, provider) in providers:
            if pattern in os.environ['OS_AUTH_URL']:
                self.provider = provider
                break
        return self.provider

    def get_provider(self):
        if self.provider is None:
            self.set_provider()
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
        found = self.run("image list -f json --limit 2000 --private --property name='" +
                        self.image_name(image) + "'")
        return len(json.loads(found)) > 0

    def net_id(self, network):
        """
        Return the uuid of the network in OpenStack.
        """
        r = json.loads(self.run("network show -f json " +
                               network))
        return self.get_value(r, 'id')

    def type_version_arch(self, os_type, os_version, arch):
        """
        Return the string used to differentiate os_type and os_version in names.
        """
        return os_type + '-' + os_version + '-' + arch

    def image_name(self, name):
        """
        Return the image name used by teuthology in OpenStack to avoid
        conflicts with existing names.
        """
        return "teuthology-" + name

    def image_create(self, name, arch):
        """
        Upload an image into OpenStack
        """
        misc.sh("wget -c -O " + name + ".qcow2 " + self.image2url[name])
        if self.get_provider() == 'dreamhost':
            image = name + ".raw"
            disk_format = 'raw'
            misc.sh("qemu-img convert " + name + ".qcow2 " + image)
        else:
            image = name + ".qcow2"
            disk_format = 'qcow2'
        if self.get_provider() == 'runabove':
            properties = [
                "--property architecture_restrict=" + arch,
                "--property architecture=" + arch
            ]
        elif self.get_provider() == 'cloudlab':
            # if not, nova-compute fails on the compute node with
            # Error: Cirrus VGA not available
            properties = [
                "--property hw_video_model=vga",
            ]
        else:
            properties = []

        misc.sh("openstack image create --property ownedby=teuthology " +
                " ".join(properties) +
                " --disk-format=" + disk_format + " --container-format=bare " +
                " --private" +
                " --file " + image + " " + self.image_name(name))

    def image(self, os_type, os_version, arch):
        """
        Return the image name for the given os_type and os_version. If the image
        does not exist it will be created.
        """
        name = self.type_version_arch(os_type, os_version, arch)
        if not self.image_exists(name):
            self.image_create(name, arch)
        return self.image_name(name)

    @staticmethod
    def sort_flavors(flavors):
        def sort_flavor(a, b):
            return (a['VCPUs'] - b['VCPUs'] or
                    a['RAM'] - b['RAM'] or
                    a['Disk'] - b['Disk'])
        return sorted(flavors, cmp=sort_flavor)

    def get_os_flavors(self):
        flavors = json.loads(self.run("flavor list -f json"))
        return flavors

    def get_sorted_flavors(self, arch, select, flavor_list = None):
        log.debug("flavor selection regex: " + select)
        flavors = flavor_list or self.get_os_flavors()
        found = []
        for flavor in flavors:
            if select and not re.match(select, flavor['Name']):
                continue
            found.append(flavor)
        sorted_flavors = OpenStack.sort_flavors(found)
        log.debug("sorted flavors = " + str(sorted_flavors))
        return sorted_flavors

    def __flavor(self, hint, flavors):
        """
        Return the smallest flavor that satisfies the desired size.
        """
        flavors = OpenStack.sort_flavors(flavors)
        for flavor in flavors:
            if (flavor['RAM'] >= hint['ram'] and
                    flavor['VCPUs'] >= hint['cpus'] and
                    flavor['Disk'] >= hint['disk']):
                return flavor['Name']
        raise NoFlavorException("openstack flavor list: " + str(flavors) +
                                " does not contain a flavor in which" +
                                " the desired " + str(hint) + " can fit")

    def __flavor_range(self, min, good, flavors):
        """
        Return the smallest flavor that satisfies the good hint.
        If no such flavor, get the largest flavor smaller than good
        and larger than min.
        """
        flavors = OpenStack.sort_flavors(flavors)
        low_range = []
        for flavor in flavors:
            if (flavor['RAM'] >= good['ram'] and
                    flavor['VCPUs'] >= good['cpus'] and
                    flavor['Disk'] >= good['disk']):
                return flavor['Name']
            else:
                low_range.append(flavor)
        low_range.reverse()
        for flavor in low_range:
            if (flavor['RAM'] >= min['ram'] and
                    flavor['VCPUs'] >= min['cpus'] and
                    flavor['Disk'] >= min['disk']):
                return flavor['Name']
        raise NoFlavorException("openstack flavor list: " + str(flavors) +
                                " does not contain a flavor which" +
                                " is larger than " + str(min))

    def __flavor_wrapper(self, min, good, hint, arch):
        """
        Wrapper for __flavor_range() and __flavor(), to hide the messiness of
        the real world.

        This is the one, single place for coding OpenStack-provider-specific
        heuristics for selecting flavors.
        """
        select_dict = {
            #'ovh': ['^(s1|vps-ssd)-', '^(c2-[0-9]+|(hg|sg)-.*ssd)$', '^(hg|sg|c2)-.*ssd'],
            'ovh': [
                '^s1-', '^c2-[0-9]+$',          # new ovh flavors at first
                '^vps-ssd-', '^(hg|sg)-.*ssd$'  # old ovh flavors
            ],
            'ecp': ['^(m1|m2).'],
        }
        if 'flavor' in teuth_config.openstack:
            flavor_select = teuth_config.openstack['flavor'] or [None]
        else:
            flavor_select = select_dict[self.get_provider()] \
                if self.get_provider() in select_dict else [None]
        all_flavors = self.get_os_flavors()
        for select in flavor_select:
            try:
                flavors = self.get_sorted_flavors(arch, select, all_flavors)
                if hint:
                    flavor = self.__flavor(hint, flavors)
                else:
                    flavor = self.__flavor_range(min, good, flavors)
                if flavor:
                    return flavor
            except NoFlavorException:
                log.debug('No flavor found for select [%s]' % select)
                pass
        raise NoFlavorException('No flavors found for filters: %s' % flavor_select)

    def flavor(self, hint, arch):
        return self.__flavor_wrapper(None, None, hint, arch)

    def flavor_range(self, min, good, arch):
        return self.__flavor_wrapper(min, good, None, arch)

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
                    for key, value in hint[resource].items():
                        current[key] = max(current[key], new[key])
        return result

    @staticmethod
    def list_instances():
        ownedby = "ownedby='" + teuth_config.openstack['ip'] + "'"
        all = json.loads(OpenStack().run(
            "server list -f json --long --name 'target'"))
        return filter(lambda instance: ownedby in instance['Properties'], all)

    @staticmethod
    def list_volumes():
        ownedby = "ownedby='" + teuth_config.openstack['ip'] + "'"
        all = json.loads(OpenStack().run("volume list -f json --long"))
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
                except paramiko.PasswordRequiredException:
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
                except socket.timeout:
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

    def get_network(self):
        nets = {
            'entercloudsuite'  : 'default',
            'cloudlab'         : 'flat-lan-1-net',
            'ecp'              : 'sesci',
        }
        if 'network' in teuth_config.openstack:
            return teuth_config.openstack['network']
        elif self.get_provider() in nets:
            return nets[self.get_provider()]
        else:
            return None

    def net(self):
        """
        Return the network to be used when creating an OpenStack instance.
        By default it should not be set. But some providers such as
        entercloudsuite require it is.
        """
        log.debug('Using config: %s', teuth_config)
        network = self.get_network()
        return "--nic net-id=" + network if network else ""

    def get_available_archs(self):
        if (self.get_provider() == 'cloudlab' or
            (self.get_provider() == 'runabove' and
             'HZ1' in os.environ.get('OS_REGION_NAME', ''))):
            return ('aarch64',)
        else:
            return ('x86_64', 'i686')

    def get_default_arch(self):
        return self.get_available_archs()[0]

    def volume_delete(self, name_or_id):
        self.run("volume set --name REMOVE-ME " + name_or_id + " || true")
        self.run("volume delete " + name_or_id + " || true")


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

    def get_instance(self):
        if not hasattr(self, 'instance'):
            self.instance = OpenStackInstance(self.server_name())
        return self.instance

    def main(self):
        """
        Entry point implementing the teuthology-openstack command.
        """
        self.setup_logs()
        set_config_attr(self.args)
        log.debug('Teuthology config: %s' % self.config.openstack)
        key_filenames = (lambda x: x if isinstance(x, list) else [x]) \
            (self.args.key_filename)
        for keyfile in key_filenames:
            if os.path.isfile(keyfile):
                self.key_filename = keyfile
                break
        if not self.key_filename:
            raise Exception('No key file provided, please, use --key-filename option')
        self.verify_openstack()
        if self.args.teardown:
            self.teardown()
            return 0
        if self.args.setup:
            self.setup()
        exit_code = 0
        if self.args.suite:
            self.get_instance()
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

    def _upload_yaml_file(self, fp):
        """
        Given an absolute path fp, assume it is a YAML file existing
        on the local machine and upload it to the remote teuthology machine
        (see https://github.com/SUSE/teuthology/issues/56 for details)
        """
        f = open(fp, 'r') # will throw exception on failure
        f.close()
        log.info("Detected local YAML file {}".format(fp))
        machine = self.username + "@" + self.instance.get_floating_ip_or_ip()

        sshopts=('-o ConnectTimeout=3 -o UserKnownHostsFile=/dev/null '
                 '-o StrictHostKeyChecking=no')

        def ssh_command(s):
            return "ssh {o} -i {k} {m} sh -c \\\"{s}\\\"".format(
                o=sshopts,
                k=self.key_filename,
                m=machine,
                s=s,
            )

        log.info("Uploading local file {} to teuthology machine".format(fp))
        remote_fp=os.path.normpath(
            '/home/{un}/yaml/{fp}'.format(
                un=self.username,
                fp=fp,
            )
        )
        command = ssh_command("stat {aug_fp}".format(
            aug_fp=remote_fp,
        ))
        try:
            misc.sh(command)
        except:
            pass
        else:
            log.warning(
                ('{fp} probably already exists remotely as {aug_fp}; '
                 'the remote one will be clobbered').format(
                fp=fp,
                aug_fp=remote_fp,
            ))
        remote_dn=os.path.dirname(remote_fp)
        command = ssh_command("mkdir -p {aug_dn}".format(
            aug_dn=remote_dn,
        ))
        misc.sh(command) # will throw exception on failure
        command = "scp {o} -i {k} {yamlfile} {m}:{dn}".format(
            o=sshopts,
            k=self.key_filename,
            yamlfile=fp,
            m=machine,
            dn=remote_dn,
        )
        misc.sh(command) # will throw exception on failure
        return remote_fp

    def _repos_from_file(self, path):
        def __check_repo_dict(obj):
            if not isinstance(obj, dict):
                raise Exception(
                    'repo item must be a dict, %s instead' % type(obj))
            required = ['name', 'url']
            if not all(x in obj.keys() for x in required):
                raise Exception(
                    'repo spec must have at least %s elements' % required)

        def __check_repo_list(obj):
            if not isinstance(obj, list):
                raise Exception(
                    'repo data must be a list, %s instead' % type(obj))
            for i in obj:
                __check_repo_dict(i)

        with open(path) as f:
            if path.endswith('.yaml') or path.endswith('.yml'):
                data = yaml.safe_load(f)
            elif path.endswith('.json') or path.endswith('.jsn'):
                data = json.load(f)
            else:
                raise Exception(
                    'Cannot detect file type from name {name}. '
                    'Supported: .yaml, .yml, .json, .jsn'
                        .format(name=f.name))
        __check_repo_list(data)
        return data

    def _repo_from_arg(self, value):
        (name, url) = value.split(':', 1)
        if '!' in name:
            n, p = name.split('!', 1)
            return {'name': n, 'priority': int(p), 'url': url}
        else:
            return {'name': name, 'url': url}

    def run_suite(self):
        """
        Delegate running teuthology-suite to the OpenStack instance
        running the teuthology cluster.
        """
        original_argv = self.argv[:]
        argv = ['--ceph', self.args.ceph,
                '--ceph-repo', self.args.ceph_repo,
                '--suite-repo', self.args.suite_repo,
                '--suite-branch', self.args.suite_branch,
                ]
        while len(original_argv) > 0:
            if original_argv[0] in ('--name',
                                    '--nameserver',
                                    '--conf',
                                    '--teuthology-branch',
                                    '--teuthology-git-url',
                                    '--test-repo',
                                    '--suite-repo',
                                    '--suite-branch',
                                    '--ceph-repo',
                                    '--ceph',
                                    '--ceph-workbench-branch',
                                    '--ceph-workbench-git-url',
                                    '--archive-upload',
                                    '--archive-upload-url',
                                    '--key-name',
                                    '--key-filename',
                                    '--simultaneous-jobs',
                                    '--controller-cpus',
                                    '--controller-ram',
                                    '--controller-disk'):
                del original_argv[0:2]
            elif original_argv[0] in ('--teardown',
                                      '--setup',
                                      '--upload',
                                      '--no-canonical-tags'):
                del original_argv[0]
            elif os.path.isabs(original_argv[0]):
                remote_path = self._upload_yaml_file(original_argv[0])
                argv.append(remote_path)
                original_argv.pop(0)
            else:
                argv.append(original_argv.pop(0))
        if self.args.test_repo:
            log.info("Using repos: %s" % self.args.test_repo)
            repos = functools.reduce(operator.concat, (
                self._repos_from_file(it.lstrip('@'))
                    if it.startswith('@') else
                        [self._repo_from_arg(it)]
                            for it in self.args.test_repo))

            overrides = {
                'overrides': {
                    'install': {
                        'repos' : repos
                    }
                }
            }
            with tempfile.NamedTemporaryFile(mode='w+b',
                                             suffix='-artifact.yaml',
                                             delete=False) as f:
                yaml_file = f.name
                log.debug("Using file " + yaml_file)
                yaml.safe_dump(overrides, stream=f, default_flow_style=False)

            path = self._upload_yaml_file(yaml_file)
            argv.append(path)

        #
        # If --upload, provide --archive-upload{,-url} regardless of
        # what was originally provided on the command line because the
        # teuthology-openstack defaults are different from the
        # teuthology-suite defaults.
        #
        if self.args.upload:
            argv.extend(['--archive-upload', self.args.archive_upload,
                         '--archive-upload-url', self.args.archive_upload_url])
        ceph_repo = getattr(self.args, 'ceph_repo')
        if ceph_repo:
            command = (
                "perl -pi -e 's|.*{opt}.*|{opt}: {value}|'"
                " ~/.teuthology.yaml"
            ).format(opt='ceph_git_url', value=ceph_repo)
            self.ssh(command)
        user_home = '/home/' + self.username
        openstack_home = user_home + '/teuthology/teuthology/openstack'
        if self.args.test_repo:
            argv.append(openstack_home + '/openstack-basic.yaml')
        else:
            argv.append(openstack_home + '/openstack-basic.yaml')
            argv.append(openstack_home + '/openstack-buildpackages.yaml')
        command = (
            "source ~/.bashrc_teuthology ; " + self.teuthology_suite + " " +
            " --machine-type openstack " +
            " ".join(map(lambda x: "'" + x + "'", argv))
        )
        return self.ssh(command)

    def reminders(self):
        if self.key_filename:
            identity = '-i ' + self.key_filename + ' '
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
        instance = self.get_instance()
        if not instance.exists():
            if self.get_provider() != 'rackspace':
                self.create_security_group()
            self.create_cluster()
            self.reminders()

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
            self.run("flavor list | tail -2")
        except subprocess.CalledProcessError:
            log.exception("flavor list")
            raise Exception("verify openrc.sh has been sourced")

    def teuthology_openstack_flavor(self, arch):
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
            hint['ram'] = 60000 # MB
        elif self.args.simultaneous_jobs >= 50:
            hint['ram'] = 30000 # MB
        elif self.args.simultaneous_jobs >= 25:
            hint['ram'] = 15000 # MB
        elif self.args.simultaneous_jobs >= 10:
            hint['ram'] = 8000 # MB
        elif self.args.simultaneous_jobs >= 2:
            hint['ram'] = 4000 # MB
        if self.args.controller_cpus > 0:
            hint['cpus'] = self.args.controller_cpus
        if self.args.controller_ram > 0:
            hint['ram'] = self.args.controller_ram
        if self.args.controller_disk > 0:
            hint['disk'] = self.args.controller_disk

        return self.flavor(hint, arch)

    def get_user_data(self):
        """
        Create a user-data.txt file to be used to spawn the teuthology
        cluster, based on a template where the OpenStack credentials
        and a few other values are substituted.
        """
        path = tempfile.mktemp()

        with open(os.path.dirname(__file__) + '/bootstrap-teuthology.sh', 'rb') as f:
            b64_bootstrap = base64.b64encode(f.read())
            bootstrap_content = str(b64_bootstrap.decode())

        openrc_sh = ''
        cacert_cmd = None
        for (var, value) in os.environ.items():
            if var in ('OS_TOKEN_VALUE', 'OS_TOKEN_EXPIRES'):
                continue
            if var == 'OS_CACERT':
                cacert_path = '/home/%s/.openstack.crt' % self.username
                cacert_file = value
                openrc_sh += 'export %s=%s\n' % (var, cacert_path)
                cacert_cmd = (
                    "su - -c 'cat > {path}' {user} <<EOF\n"
                    "{data}\n"
                    "EOF\n").format(
                        path=cacert_path,
                        user=self.username,
                        data=open(cacert_file).read())
            elif var.startswith('OS_'):
                openrc_sh += 'export %s=%s\n' % (var, value)
        b64_openrc_sh = base64.b64encode(openrc_sh.encode())
        openrc_sh_content = str(b64_openrc_sh.decode())

        network = OpenStack().get_network()
        ceph_workbench = ''
        if self.args.ceph_workbench_git_url:
            ceph_workbench += (" --ceph-workbench-branch " +
                               self.args.ceph_workbench_branch)
            ceph_workbench += (" --ceph-workbench-git-url " +
                               self.args.ceph_workbench_git_url)

        setup_options = [
            '--keypair %s' % self.key_pair(),
            '--selfname %s' % self.args.name,
            '--server-name %s' % self.server_name(),
            '--server-group %s' % self.server_group(),
            '--worker-group %s' % self.worker_group(),
            '--package-repo %s' % self.packages_repository(),
            #'--setup-all',
        ]
        all_options = [
            '--install',                #do_install_packages=true
            #'--setup-ceph-workbench',   #do_ceph_workbench=true
            '--config',                 #do_create_config=true
            '--setup-keypair',          #do_setup_keypair=true
            #'',           #do_apt_get_update=true
            '--setup-docker',           #do_setup_docker=true
            '--setup-salt-master',      #do_setup_salt_master=true
            '--setup-dnsmasq',          #do_setup_dnsmasq=true
            '--setup-fail2ban',         #do_setup_fail2ban=true
            '--setup-paddles',          #do_setup_paddles=true
            '--setup-pulpito',          #do_setup_pulpito=true
            '--populate-paddles',       #do_populate_paddles=true
        ]

        if self.args.ceph_workbench_git_url:
            all_options += [
                '--setup-ceph-workbench',
                '--ceph-workbench-branch %s' % self.args.ceph_workbench_branch,
                '--ceph-workbench-git-url %s' % self.args.ceph_workbench_git_url,
            ]
        if self.args.no_canonical_tags:
            all_options += [ '--no-canonical-tags' ]
        if self.args.upload:
            all_options += [ '--archive-upload ' + self.args.archive_upload ]
        if network:
            all_options += [ '--network ' + network ]
        if self.args.simultaneous_jobs:
            all_options += [ '--nworkers ' + str(self.args.simultaneous_jobs) ]
        if self.args.nameserver:
            all_options += [ '--nameserver %s' % self.args.nameserver]


        cmds = [
            cmd_str(
                "su - -c 'bash /tmp/bootstrap-teuthology.sh "
                "teuthology {url} {branch}' {user} >> "
                "/tmp/init.out 2>&1".format(
                    url=self.args.teuthology_git_url,
                    branch=self.args.teuthology_branch,
                    user=self.username)),
            cmd_str(
                "su - -c 'cp /tmp/openrc.sh $HOME/openrc.sh' {user}"
                    .format(user=self.username)),
            cmd_str(
                "su - -c '(set +x ; source openrc.sh ; set -x ; cd teuthology ; "
                "source virtualenv/bin/activate ; "
                "teuthology/openstack/setup-openstack.sh {opts})' "
                "{user} >> /tmp/init.out "
                "2>&1".format(user=self.username,
                              opts=' '.join(setup_options + all_options))),
            # wa: we want to stop paddles and pulpito started by
            # setup-openstack before starting teuthology service
            "pkill -f 'pecan serve'",
            "pkill -f 'python run.py'",
            "systemctl enable teuthology",
            "systemctl start teuthology",
        ]
        if cacert_cmd:
            cmds.insert(0,cmd_str(cacert_cmd))
        #cloud-config
        cloud_config = {
            'bootcmd': [
                'touch /tmp/init.out',
                'echo nameserver 8.8.8.8 | tee -a /etc/resolv.conf',
            ],
            'manage_etc_hosts': True,
            'system_info': {
                'default_user': {
                    'name': self.username
                }
            },
            'packages': [
                'python-virtualenv',
                'git',
                'rsync',
            ],
            'write_files': [
                {
                    'path': '/tmp/bootstrap-teuthology.sh',
                    'content': cmd_str(bootstrap_content),
                    'encoding': 'b64',
                    'permissions': '0755',
                },
                {
                    'path': '/tmp/openrc.sh',
                    'owner': self.username,
                    'content': cmd_str(openrc_sh_content),
                    'encoding': 'b64',
                    'permissions': '0644',
                }
            ],
            'runcmd': cmds,
            'final_message': 'teuthology is up and running after $UPTIME seconds'
        }
        user_data = "#cloud-config\n%s" % \
              yaml.dump(cloud_config, default_flow_style = False)
        open(path, 'w').write(user_data)
        log.debug("user_data: %s" % user_data)

        return path

    def key_pair(self):
        return "teuth-%s" % self.args.name

    def server_name(self):
        return "teuth-%s" % self.args.name

    def server_group(self):
        return "teuth-%s" % self.args.name

    def worker_group(self):
        return "teuth-%s-worker" % self.args.name

    def create_security_group(self):
        """
        Create a security group that will be used by all teuthology
        created instances. This should not be necessary in most cases
        but some OpenStack providers enforce firewall restrictions even
        among instances created within the same tenant.
        """
        groups = misc.sh('openstack security group list -c Name -f value').split('\n')
        if all(g in groups for g in [self.server_group(), self.worker_group()]):
            return
        misc.sh("""
openstack security group delete {server} || true
openstack security group delete {worker} || true
openstack security group create {server}
openstack security group create {worker}
# access to teuthology VM from the outside
openstack security group rule create --proto tcp --dst-port 22 {server} # ssh
openstack security group rule create --proto tcp --dst-port 80 {server} # for log access
openstack security group rule create --proto tcp --dst-port 8080 {server} # pulpito
openstack security group rule create --proto tcp --dst-port 8081 {server} # paddles
# access between teuthology and workers
openstack security group rule create --src-group {worker} --dst-port 1:65535 {server}
openstack security group rule create --protocol udp --src-group {worker} --dst-port 1:65535 {server}
openstack security group rule create --src-group {server} --dst-port 1:65535 {worker}
openstack security group rule create --protocol udp --src-group {server} --dst-port 1:65535 {worker}
# access between members of one group
openstack security group rule create --src-group {worker} --dst-port 1:65535 {worker}
openstack security group rule create --protocol udp --src-group {worker} --dst-port 1:65535 {worker}
openstack security group rule create --src-group {server} --dst-port 1:65535 {server}
openstack security group rule create --protocol udp --src-group {server} --dst-port 1:65535 {server}
        """.format(server=self.server_group(), worker=self.worker_group()))

    @staticmethod
    def get_unassociated_floating_ip():
        """
        Return a floating IP address not associated with an instance or None.
        """
        ips = TeuthologyOpenStack.get_os_floating_ips()
        for ip in ips:
            if not ip['Port']:
                return ip['Floating IP Address']
        return None

    @staticmethod
    def create_floating_ip():
        try:
            pools = json.loads(OpenStack().run("ip floating pool list -f json"))
        except subprocess.CalledProcessError as e:
            if 'Floating ip pool operations are only available for Compute v2 network.' \
                    in e.output:
                log.debug(e.output)
                log.debug('Trying newer API than Compute v2')
                try:
                    network = 'floating'
                    ip = json.loads(misc.sh("openstack --quiet floating ip create -f json '%s'" % network))
                    return ip['floating_ip_address']
                except subprocess.CalledProcessError:
                    log.debug("Can't create floating ip for network '%s'" % network)

            log.debug("create_floating_ip: ip floating pool list failed")
            return None
        if not pools:
            return None
        pool = pools[0]['Name']
        try:
            ip = json.loads(OpenStack().run(
                "ip floating create -f json '" + pool + "'"))
            return ip['ip']
        except subprocess.CalledProcessError:
            log.debug("create_floating_ip: not creating a floating ip")
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
            OpenStack().run("ip floating add " + ip + " " + name_or_id)

    @staticmethod
    def get_os_floating_ips():
        try:
            ips = json.loads(OpenStack().run("ip floating list -f json"))
        except subprocess.CalledProcessError as e:
            log.warning(e)
            if e.returncode == 1:
                return []
            else:
                raise e
        return ips

    @staticmethod
    def get_floating_ip_id(ip):
        """
        Return the id of a floating IP
        """
        results = TeuthologyOpenStack.get_os_floating_ips()
        for result in results:
            for k in ['IP', 'Floating IP Address']:
                if k in result:
                    if result[k] == ip:
                        return str(result['ID'])

        return None

    def get_instance_id(self):
        instance = self.get_instance()
        if instance.info:
            return instance['id']
        else:
            return None

    @staticmethod
    def delete_floating_ip(instance_id):
        """
        Remove the floating ip from instance_id and delete it.
        """
        ip = OpenStackInstance(instance_id).get_floating_ip()
        if not ip:
            return
        OpenStack().run("ip floating remove " + ip + " " + instance_id)
        ip_id = TeuthologyOpenStack.get_floating_ip_id(ip)
        OpenStack().run("ip floating delete " + ip_id)

    def create_cluster(self):
        user_data = self.get_user_data()
        security_group = \
            " --security-group {teuthology}".format(teuthology=self.server_group())
        if self.get_provider() == 'rackspace':
            security_group = ''
        arch = self.get_default_arch()
        flavor = self.teuthology_openstack_flavor(arch)
        log.debug('Create server: %s' % self.server_name())
        log.debug('Using config: %s' % self.config.openstack)
        log.debug('Using flavor: %s' % flavor)
        key_name = self.args.key_name
        if not key_name:
            raise Exception('No key name provided, use --key-name option')
        log.debug('Using key name: %s' % self.args.key_name)
        self.run(
            "server create " +
            " --image '" + self.image('ubuntu', '16.04', arch) + "' " +
            " --flavor '" + flavor + "' " +
            " " + self.net() +
            " --key-name " + key_name +
            " --user-data " + user_data +
            security_group +
            " --wait " + self.server_name() +
            " -f json")
        os.unlink(user_data)
        self.instance = OpenStackInstance(self.server_name())
        self.associate_floating_ip(self.instance['id'])
        return self.cloud_init_wait(self.instance)

    def packages_repository(self):
        return 'teuth-%s-repo' % self.args.name #packages-repository

    def teardown(self):
        """
        Delete all instances run by the teuthology cluster and delete the
        instance running the teuthology cluster.
        """
        instance_id = self.get_instance_id()

        if instance_id:
            self.ssh("sudo /etc/init.d/teuthology stop || true")
            self.delete_floating_ip(instance_id)
        self.run("server delete %s || true" % self.packages_repository())
        self.run("server delete --wait %s || true" % self.server_name())
        self.run("keypair delete %s || true" % self.key_pair())
        self.run("security group delete %s || true" % self.worker_group())
        self.run("security group delete %s || true" % self.server_group())

def main(ctx, argv):
    return TeuthologyOpenStack(ctx, teuth_config, argv).main()
