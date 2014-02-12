#!/usr/bin/env python
import contextlib
import logging
from cStringIO import StringIO
import textwrap
from configparser import ConfigParser
import time

from ..orchestra import run
from teuthology import misc

"""
https://github.com/openstack-dev/devstack/blob/master/README.md
http://ceph.com/docs/master/rbd/rbd-openstack/
"""
log = logging.getLogger(__name__)

DEVSTACK_GIT_REPO = 'https://github.com/openstack-dev/devstack.git'


@contextlib.contextmanager
def task(ctx, config):
    """
    Install OpenStack DevStack and configure it to use a Ceph cluster for
    Glance and Cinder.

    Requires one node with a role 'devstack'

    Since devstack runs rampant on the system it's used on, typically you will
    want to reprovision that machine after using devstack on it.

    Also, the default 2GB of RAM that is given to vps nodes is insufficient. I
    recommend 4GB. Downburst can be instructed to give 4GB to a vps node by
    adding this to the yaml:

    downburst:
        ram: 4G
    """
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise TypeError("config must be a dict")

    # SETUP
    is_devstack_node = lambda role: role.startswith('devstack')
    is_osd_node = lambda role: role.startswith('osd')
    devstack_node = ctx.cluster.only(is_devstack_node).remotes.keys()[0]
    an_osd_node = ctx.cluster.only(is_osd_node).remotes.keys()[0]
    install_devstack(devstack_node)
    try:
        configure_devstack_and_ceph(ctx, config, devstack_node, an_osd_node)
        yield
    #except Exception as e:
        # FAIL
        #pass
    finally:
        # CLEANUP
        pass


def install_devstack(devstack_node):
    log.info("Cloning DevStack repo...")

    args = ['git', 'clone', DEVSTACK_GIT_REPO]
    devstack_node.run(args=args)

    log.info("Installing DevStack...")
    args = ['cd', 'devstack', run.Raw('&&'), './stack.sh']
    devstack_node.run(args=args)


def configure_devstack_and_ceph(ctx, config, devstack_node, ceph_node):
    pool_size = config.get('pool_size', '128')
    create_pools(ceph_node, pool_size)
    distribute_ceph_conf(devstack_node, ceph_node)
    # This is where we would install python-ceph and ceph-common but it appears
    # the ceph task does that for us.
    generate_ceph_keys(ceph_node)
    distribute_ceph_keys(devstack_node, ceph_node)
    secret_uuid = set_libvirt_secret(devstack_node, ceph_node)
    update_devstack_config_files(devstack_node, secret_uuid)
    set_apache_servername(devstack_node)
    # Rebooting is the most-often-used method of restarting devstack services
    reboot(devstack_node)
    start_devstack(devstack_node)
    restart_apache(devstack_node)


def create_pools(ceph_node, pool_size):
    log.info("Creating pools on Ceph cluster...")

    for pool_name in ['volumes', 'images', 'backups']:
        args = ['ceph', 'osd', 'pool', 'create', pool_name, pool_size]
        ceph_node.run(args=args)


def distribute_ceph_conf(devstack_node, ceph_node):
    log.info("Copying ceph.conf to DevStack node...")

    ceph_conf_path = '/etc/ceph/ceph.conf'
    ceph_conf = misc.get_file(ceph_node, ceph_conf_path, sudo=True)
    misc.sudo_write_file(devstack_node, ceph_conf_path, ceph_conf)


def generate_ceph_keys(ceph_node):
    log.info("Generating Ceph keys...")

    ceph_auth_cmds = [
        ['ceph', 'auth', 'get-or-create', 'client.cinder', 'mon',
            'allow r', 'osd', 'allow class-read object_prefix rbd_children, allow rwx pool=volumes, allow rx pool=images'],  # noqa
        ['ceph', 'auth', 'get-or-create', 'client.glance', 'mon',
            'allow r', 'osd', 'allow class-read object_prefix rbd_children, allow rwx pool=images'],  # noqa
        ['ceph', 'auth', 'get-or-create', 'client.cinder-backup', 'mon',
            'allow r', 'osd', 'allow class-read object_prefix rbd_children, allow rwx pool=backups'],  # noqa
    ]
    for cmd in ceph_auth_cmds:
        ceph_node.run(args=cmd)


def distribute_ceph_keys(devstack_node, ceph_node):
    log.info("Copying Ceph keys to DevStack node...")

    def copy_key(from_remote, key_name, to_remote, dest_path, owner):
        key_stringio = StringIO()
        from_remote.run(
            args=['ceph', 'auth', 'get-or-create', key_name],
            stdout=key_stringio)
        key_stringio.seek(0)
        misc.sudo_write_file(to_remote, dest_path,
                             key_stringio, owner=owner)
    keys = [
        dict(name='client.glance',
             path='/etc/ceph/ceph.client.glance.keyring',
             # devstack appears to just want root:root
             #owner='glance:glance',
             ),
        dict(name='client.cinder',
             path='/etc/ceph/ceph.client.cinder.keyring',
             # devstack appears to just want root:root
             #owner='cinder:cinder',
             ),
        dict(name='client.cinder-backup',
             path='/etc/ceph/ceph.client.cinder-backup.keyring',
             # devstack appears to just want root:root
             #owner='cinder:cinder',
             ),
    ]
    for key_dict in keys:
        copy_key(ceph_node, key_dict['name'], devstack_node,
                 key_dict['path'], key_dict.get('owner'))


def set_libvirt_secret(devstack_node, ceph_node):
    log.info("Setting libvirt secret...")

    cinder_key_stringio = StringIO()
    ceph_node.run(args=['ceph', 'auth', 'get-key', 'client.cinder'],
                  stdout=cinder_key_stringio)
    cinder_key_stringio.seek(0)
    cinder_key = cinder_key_stringio.read().strip()

    uuid_stringio = StringIO()
    devstack_node.run(args=['uuidgen'], stdout=uuid_stringio)
    uuid_stringio.seek(0)
    uuid = uuid_stringio.read().strip()

    secret_path = '/tmp/secret.xml'
    secret_template = textwrap.dedent("""
    <secret ephemeral='no' private='no'>
        <uuid>{uuid}</uuid>
        <usage type='ceph'>
            <name>client.cinder secret</name>
        </usage>
    </secret>""")
    misc.sudo_write_file(devstack_node, secret_path,
                         secret_template.format(uuid=uuid))
    devstack_node.run(args=['sudo', 'virsh', 'secret-define', '--file',
                            secret_path])
    devstack_node.run(args=['sudo', 'virsh', 'secret-set-value', '--secret',
                            uuid, '--base64', cinder_key])
    return uuid


def update_devstack_config_files(devstack_node, secret_uuid):
    log.info("Updating DevStack config files to use Ceph...")

    def backup_config(node, file_name, backup_ext='.orig.teuth'):
        node.run(args=['cp', '-f', file_name, file_name + backup_ext])

    def update_config(config_name, config_stream, update_dict,
                      section='DEFAULT'):
        parser = ConfigParser()
        parser.read_file(config_stream)
        for (key, value) in update_dict.items():
            parser.set(section, key, value)
        out_stream = StringIO()
        parser.write(out_stream)
        out_stream.seek(0)
        return out_stream

    updates = [
        dict(name='/etc/glance/glance-api.conf', options=dict(
            default_store='rbd',
            rbd_store_user='glance',
            rbd_store_pool='images',
            show_image_direct_url='True',)),
        dict(name='/etc/cinder/cinder.conf', options=dict(
            volume_driver='cinder.volume.drivers.rbd.RBDDriver',
            rbd_pool='volumes',
            rbd_ceph_conf='/etc/ceph/ceph.conf',
            rbd_flatten_volume_from_snapshot='false',
            rbd_max_clone_depth='5',
            glance_api_version='2',
            rbd_user='cinder',
            rbd_secret_uuid=secret_uuid,
            backup_driver='cinder.backup.drivers.ceph',
            backup_ceph_conf='/etc/ceph/ceph.conf',
            backup_ceph_user='cinder-backup',
            backup_ceph_chunk_size='134217728',
            backup_ceph_pool='backups',
            backup_ceph_stripe_unit='0',
            backup_ceph_stripe_count='0',
            restore_discard_excess_bytes='true',
            )),
        dict(name='/etc/nova/nova.conf', options=dict(
            libvirt_images_type='rbd',
            libvirt_images_rbd_pool='volumes',
            libvirt_images_rbd_ceph_conf='/etc/ceph/ceph.conf',
            rbd_user='cinder',
            rbd_secret_uuid=secret_uuid,
            libvirt_inject_password='false',
            libvirt_inject_key='false',
            libvirt_inject_partition='-2',
            )),
    ]

    for update in updates:
        file_name = update['name']
        options = update['options']
        config_str = misc.get_file(devstack_node, file_name, sudo=True)
        config_stream = StringIO(config_str)
        backup_config(devstack_node, file_name)
        new_config_stream = update_config(file_name, config_stream, options)
        misc.sudo_write_file(devstack_node, file_name, new_config_stream)


def set_apache_servername(node):
    # Apache complains: "Could not reliably determine the server's fully
    # qualified domain name, using 127.0.0.1 for ServerName"
    # So, let's make sure it knows its name.
    log.info("Setting Apache ServerName...")

    hostname = node.hostname
    config_file = '/etc/apache2/conf.d/servername'
    misc.sudo_write_file(node, config_file,
                         "ServerName {name}".format(name=hostname))


def reboot(node, timeout=300, interval=30):
    log.info("Rebooting {host}...".format(host=node.hostname))
    node.run(args=['sudo', 'shutdown', '-r', 'now'])
    reboot_start_time = time.time()
    while time.time() - reboot_start_time < timeout:
        time.sleep(interval)
        if node.is_online or node.reconnect():
            return
    raise RuntimeError(
        "{host} did not come up after reboot within {time}s".format(
            host=node.hostname, time=timeout))


def start_devstack(devstack_node):
    log.info("Patching devstack start script...")
    # This causes screen to start headless - otherwise rejoin-stack.sh fails
    # because there is no terminal attached.
    cmd = "cd devstack && sed -ie 's/screen -c/screen -dm -c/' rejoin-stack.sh"
    devstack_node.run(args=cmd)

    log.info("Starting devstack...")
    cmd = "cd devstack && ./rejoin-stack.sh"
    devstack_node.run(args=cmd)


def restart_apache(node):
    node.run(args=['sudo', '/etc/init.d/apache2', 'restart'], wait=True)
