#!/usr/bin/env python3

PODMAN_PREFERENCE = ['podman', 'docker']  # prefer podman to docker
"""
You can invoke ceph-daemon in two ways:

1. The normal way, at the command line.

2. By piping the script to the python3 binary.  In this latter case, you should
   prepend one or more lines to the beginning of the script.

   For arguments,

       injected_argv = [...]

   e.g.,

       injected_argv = ['ls']

   For reading stdin from the '--config-and-json -' argument,

       injected_stdin = '...'
"""

import configparser
import json
import logging
import os
import socket
import subprocess
import sys
import tempfile
import time

from argparser import cmdline_args
from container import CephContainer
from utils import make_fsid, get_hostname, create_daemon_dirs, get_unit_name, extract_uid_gid
from config import Config
from keyrings import Keyring
from operation import Operation
from ssh import SSH

try:
    from StringIO import StringIO
except ImportError:
    pass
try:
    from io import StringIO
except ImportError:
    pass

logging.basicConfig(level=logging.INFO)

podman_path = None

##################################


def get_legacy_daemon_fsid(cluster_name, daemon_type, daemon_id):
    # TODO: args
    fsid = None
    if daemon_type == 'osd':
        try:
            with open(
                    os.path.join(args.data_dir, daemon_type,
                                 'ceph-%s' % daemon_id, 'ceph_fsid'),
                    'r') as f:
                fsid = f.read().strip()
        except IOError:
            pass
    if not fsid:
        fsid = get_legacy_config_fsid(cluster)
    return fsid


def gen_ssh_key(fsid):
    tmp_dir = tempfile.TemporaryDirectory()
    path = tmp_dir.name + '/key'
    subprocess.check_output(
        ['ssh-keygen', '-C',
         'ceph-%s' % fsid, '-N', '', '-f', path])
    with open(path, 'r') as f:
        secret = f.read()
    with open(path + '.pub', 'r') as f:
        pub = f.read()
    os.unlink(path)
    os.unlink(path + '.pub')
    tmp_dir.cleanup()
    return (secret, pub)


##################################


def command_deploy():
    (daemon_type, daemon_id) = args.name.split('.')
    if daemon_type not in ['mon', 'mgr', 'mds', 'osd', 'rgw']:
        raise RuntimeError('daemon type %s not recognized' % daemon_type)
    (config, keyring) = get_config_and_keyring()
    if daemon_type == 'mon':
        if args.mon_ip:
            config += '[mon.%s]\n\tpublic_addr = %s\n' % (daemon_id,
                                                          args.mon_ip)
        elif args.mon_network:
            config += '[mon.%s]\n\tpublic_network = %s\n' % (daemon_id,
                                                             args.mon_network)
        else:
            raise RuntimeError('must specify --mon-ip or --mon-network')
    (uid, gid) = extract_uid_gid()
    c = get_container(args.fsid, daemon_type, daemon_id)
    deploy_daemon(args.fsid, daemon_type, daemon_id, c, uid, gid, config,
                  keyring)


##################################


def command_ceph_volume():
    make_log_dir(args.fsid)

    mounts = get_container_mounts(args.fsid, 'osd', None)

    tmp_config = None
    tmp_keyring = None

    if args.config_and_keyring:
        # note: this will always pull from args.config_and_keyring (we
        # require it) and never args.config or args.keyring.
        (config, keyring) = get_config_and_keyring()

        # tmp keyring file
        tmp_keyring = tempfile.NamedTemporaryFile(mode='w')
        os.fchmod(tmp_keyring.fileno(), 0o600)
        tmp_keyring.write(keyring)
        tmp_keyring.flush()

        # tmp config file
        tmp_config = tempfile.NamedTemporaryFile(mode='w')
        os.fchmod(tmp_config.fileno(), 0o600)
        tmp_config.write(config)
        tmp_config.flush()

        mounts[tmp_config.name] = '/etc/ceph/ceph.conf:z'
        mounts[tmp_keyring.name] = '/var/lib/ceph/bootstrap-osd/ceph.keyring:z'

    c = CephContainer(
        image=args.image,
        entrypoint='/usr/sbin/ceph-volume',
        args=args.command,
        podman_args=['--privileged'],
        volume_mounts=mounts,
    )
    subprocess.call(c.run_cmd())


##################################


def command_unit():
    (daemon_type, daemon_id) = args.name.split('.')
    unit_name = get_unit_name(args.fsid, daemon_type, daemon_id)
    subprocess.call(['systemctl', args.command, unit_name])


##################################


def command_ls():
    ls = []

    # /var/lib/ceph
    if os.path.exists(args.data_dir):
        for i in os.listdir(args.data_dir):
            if i in ['mon', 'osd', 'mds', 'mgr']:
                daemon_type = i
                for j in os.listdir(os.path.join(args.data_dir, i)):
                    if '-' not in j:
                        continue
                    (cluster, daemon_id) = j.split('-', 1)
                    fsid = get_legacy_daemon_fsid(cluster, daemon_type,
                                                  daemon_id) or 'unknown'
                    (enabled, active) = check_unit(
                        'ceph-%s@%s' % (daemon_type, daemon_id))
                    ls.append({
                        'style': 'legacy',
                        'name': '%s.%s' % (daemon_type, daemon_id),
                        'fsid': fsid,
                        'enabled': enabled,
                        'active': active,
                    })
            elif is_fsid(i):
                fsid = i
                for j in os.listdir(os.path.join(args.data_dir, i)):
                    (daemon_type, daemon_id) = j.split('.', 1)
                    (enabled, active) = check_unit(
                        get_unit_name(fsid, daemon_type, daemon_id))
                    ls.append({
                        'style': 'ceph-daemon:v1',
                        'name': '%s.%s' % (daemon_type, daemon_id),
                        'fsid': fsid,
                        'enabled': enabled,
                        'active': active,
                    })

    # /var/lib/rook
    # WRITE ME

    print(json.dumps(ls, indent=4))


##################################


def command_adopt():
    (daemon_type, daemon_id) = args.name.split('.')
    (uid, gid) = extract_uid_gid()
    if args.style == 'legacy':
        fsid = get_legacy_daemon_fsid(args.cluster, daemon_type, daemon_id)
        if not fsid:
            raise RuntimeError(
                'could not detect fsid; add fsid = to ceph.conf')

        # NOTE: implicit assumption here that the units correspond to the
        # cluster we are adopting based on the /etc/{defaults,sysconfig}/ceph
        # CLUSTER field.
        unit_name = 'ceph-%s@%s' % (daemon_type, daemon_id)
        (enabled, active) = check_unit(unit_name)

        if active:
            logging.info('Stopping old systemd unit %s...' % unit_name)
            subprocess.check_output(['systemctl', 'stop', unit_name])
        if enabled:
            logging.info('Disabling old systemd unit %s...' % unit_name)
            subprocess.check_output(['systemctl', 'disable', unit_name])

        logging.info('Moving data...')
        make_data_dir_base(fsid, uid, gid)
        data_dir = get_data_dir(fsid, daemon_type, daemon_id)
        subprocess.check_output([
            'mv',
            '/var/lib/ceph/%s/%s-%s' % (daemon_type, args.cluster, daemon_id),
            data_dir
        ])
        subprocess.check_output([
            'cp',
            '/etc/ceph/%s.conf' % args.cluster,
            os.path.join(data_dir, 'config')
        ])
        os.chmod(data_dir, DATA_DIR_MODE)
        os.chown(data_dir, uid, gid)

        logging.info('Moving logs...')
        log_dir = make_log_dir(fsid, uid=uid, gid=gid)
        try:
            subprocess.check_output([
                'mv',
                '/var/log/ceph/%s-%s.%s.log*' %
                (args.cluster, daemon_type, daemon_id),
                os.path.join(log_dir)
            ],
                                    shell=True)
        except Exception as e:
            logging.warning('unable to move log file: %s' % e)
            pass

        logging.info('Creating new units...')
        c = get_container(fsid, daemon_type, daemon_id)
        deploy_daemon_units(
            fsid,
            daemon_type,
            daemon_id,
            c,
            enable=True,  # unconditionally enable the new unit
            start=active)
    else:
        raise RuntimeError('adoption of style %s not implemented' % args.style)


##################################


def command_rm_daemon():
    (daemon_type, daemon_id) = args.name.split('.')
    if daemon_type in ['mon', 'osd'] and not args.force:
        raise RuntimeError('must pass --force to proceed: '
                           'this command may destroy precious data!')
    unit_name = get_unit_name(args.fsid, daemon_type, daemon_id)
    subprocess.check_output(['systemctl', 'stop', unit_name])
    subprocess.check_output(['systemctl', 'disable', unit_name])
    data_dir = get_data_dir(args.fsid, daemon_type, daemon_id)
    subprocess.check_output(['rm', '-rf', data_dir])


##################################


def command_rm_cluster():
    if not args.force:
        raise RuntimeError('must pass --force to proceed: '
                           'this command may destroy precious data!')

    unit_name = 'ceph-%s.target' % args.fsid
    try:
        subprocess.check_output(['systemctl', 'stop', unit_name])
        subprocess.check_output(['systemctl', 'disable', unit_name])
    except subprocess.CalledProcessError:
        pass

    slice_name = 'system-%s.slice' % (
        ('ceph-%s' % args.fsid).replace('-', '\\x2d'))
    try:
        subprocess.check_output(['systemctl', 'stop', slice_name])
    except subprocess.CalledProcessError:
        pass

    # FIXME: stop + disable individual daemon units, too?

    # rm units
    subprocess.check_output(
        ['rm', '-f', args.unit_dir + '/ceph-%s@.service' % args.fsid])
    subprocess.check_output(
        ['rm', '-f', args.unit_dir + '/ceph-%s.target' % args.fsid])
    subprocess.check_output(
        ['rm', '-rf', args.unit_dir + '/ceph-%s.target.wants' % args.fsid])
    # rm data
    subprocess.check_output(['rm', '-rf', args.data_dir + '/' + args.fsid])
    # rm logs
    subprocess.check_output(['rm', '-rf', args.log_dir + '/' + args.fsid])
    subprocess.check_output(
        ['rm', '-rf', args.log_dir + '/*.wants/ceph-%s@*' % args.fsid])


##################################


class Version(object):
    def __init__(self, config):
        self.image = config.image
        self.version = self.command_version()
        self.report()

    def command_version(self):
        ret = CephContainer(self.image, 'ceph', ['--version']).run()
        return ret.stdout

    def report(self):
        # consider __call__()
        print(self.version)


class Deploy(object):
    pass


class Bootstrap(object):
    def __init__(self, args):
        self.config = Config(args)
        # TODO: this sucks
        self.config.ceph_uid, self.config.ceph_gid = extract_uid_gid(
            CephContainer, self.image)
        self.keyring = Keyring(self.config)
        self.operation = Operation(self.config)
        self.ssh = SSH(self.config)
        self.bootstrap()

    def __getattr__(self, attr):
        # Inheritance vs Composition..
        return getattr(self.config, attr)

    def create_initial_config(self):
        # config
        cp = configparser.ConfigParser()
        cp.add_section('global')
        cp['global']['fsid'] = self.fsid
        cp['global']['mon host'] = self.addr_arg
        with StringIO() as f:
            cp.write(f)
            config = f.getvalue()
        return config

    def bootstrap(self):
        logging.info('Cluster fsid: %s' % self.fsid)

        keyring, _, admin_key, mgr_key = self.keyring.create_initial_keyring(
            self.mgr_id)

        tmp_keyring = self.keyring.write_tmp_keyring(keyring)

        config = self.create_initial_config()

        self.operation.bootstrap_mon(tmp_keyring, config, 'mon', self.mon_id)

        # output files
        if self.output_keyring:
            with open(self.output_keyring, 'w') as f:
                os.fchmod(f.fileno(), 0o600)
                f.write('[client.admin]\n' '\tkey = ' + admin_key + '\n')
            with open('/etc/ceph/bootstrap/ceph.keyring', 'w') as f:
                os.fchmod(f.fileno(), 0o600)
                f.write('[client.admin]\n' '\tkey = ' + admin_key + '\n')
            logging.info('Wrote keyring to %s' % self.output_keyring)

        if self.output_config:
            with open(self.output_config, 'w') as f:
                f.write(config)
            logging.info('Wrote config to %s' % self.output_config)

        self.operation.create_mgr(mgr_key)

        self.operation.wait_for_health()

        # ssh
        # TODO
        #self.ssh.do_ssh_things()


def run(config):
    Operation(config).wait_for_health()


def main(config):
    print(config)

    mapper = {
        'version': Version,
        'deploy': Deploy,
        'bootstrap': Bootstrap,
        'run': run
    }
    # No default values or exception checking needed here,
    # argparse only allows defined subcommands.
    mapper.get(config.command)(config)


if __name__ == '__main__':

    if sys.version_info < (2, 6, 0):
        sys.stderr.write("You need python 2.6 or later to run this script\n")
        sys.exit(1)

    try:
        args = cmdline_args()
        main(args)

    # TODO: No bare except
    except Exception:
        raise
