from directory import Directory
from container import CephContainer
import subprocess
from utils import get_daemon_args, get_unit_name
import tempfile
import os
import logging
import json
import time


class Operation(object):
    def __init__(self, config):
        self.config = config
        self.directory = Directory(self.config)

    def __getattr__(self, attr):
        return getattr(self.config, attr)

    def create_mgr(self, mgr_key):
        # create mgr
        logging.info('Creating mgr...')
        mgr_keyring = '[mgr.%s]\n\tkey = %s\n' % (self.mgr_id, mgr_key)
        mgr_c = self.get_container('mgr', self.mgr_id)
        # get ceph_conf
        self.deploy_daemon(
            'mgr', self.mgr_id, container=mgr_c, keyring=mgr_keyring)

    def create_monmap(self):
        # create initial monmap, tmp monmap file
        logging.info('Creating initial monmap...')
        tmp_monmap = tempfile.NamedTemporaryFile(mode='w')
        os.fchmod(tmp_monmap.fileno(), 0o644)
        out = CephContainer(
            image=self.image,
            entrypoint='/usr/bin/monmaptool',
            args=[
                '--create', '--clobber', '--fsid', self.fsid, '--addv',
                self.mon_id, self.addr_arg, '/tmp/monmap'
            ],
            volume_mounts={
                tmp_monmap.name: '/tmp/monmap:z',
            },
        ).run().stdout
        return tmp_monmap

    def bootstrap_mon(self, tmp_keyring, config, daemon_type, daemon_id):
        # create mon
        logging.info('Creating mon...')

        self.directory.create_daemon_dirs(
            daemon_type='mon', daemon_id=self.mon_id)

        tmp_monmap = self.create_monmap()

        mon_dir = self.directory.get_data_dir(
            daemon_type='mon', daemon_id=self.mon_id)
        log_dir = self.directory.get_log_dir()

        out = CephContainer(
            image=self.image,
            entrypoint='/usr/bin/ceph-mon',
            args=[
                '--mkfs',
                '-i',
                self.mon_id,
                '--fsid',
                self.fsid,
                # '-c',
                # '/dev/null',
                '--monmap',
                '/tmp/monmap',
                '--keyring',
                '/tmp/keyring',
            ] + get_daemon_args(self.fsid, 'mon', self.mon_id),
            volume_mounts={
                log_dir: '/var/log/ceph:z',
                mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % (self.mon_id),
                tmp_keyring.name: '/tmp/keyring:z',
                tmp_monmap.name: '/tmp/monmap:z',
            },
        ).run()

        with open(mon_dir + '/config', 'w') as f:
            os.fchown(f.fileno(), self.ceph_uid, self.ceph_gid)
            os.fchmod(f.fileno(), 0o600)
            f.write(config)

        mon_c = self.get_container(daemon_type, daemon_id)
        self.deploy_daemon_units('mon', self.mon_id, mon_c)

    def get_container(self,
                      daemon_type,
                      daemon_id,
                      privileged=False,
                      data_dir=None,
                      log_dir=None):
        # TODO: args
        podman_args = []

        if daemon_type == 'osd' or privileged:
            podman_args += ['--privileged']
        return CephContainer(
            image=self.image,
            entrypoint='/usr/bin/ceph-' + daemon_type,
            args=[
                '-n',
                '%s.%s' % (daemon_type, daemon_id),
                '-f',  # foreground
            ] + get_daemon_args(self.fsid, daemon_type, daemon_id),
            podman_args=podman_args,
            volume_mounts=self.get_container_mounts(daemon_type, daemon_id),
            cname='ceph-%s-%s.%s' % (self.fsid, daemon_type, daemon_id),
        )

    def get_container_mounts(self, daemon_type, daemon_id):
        mounts = {}

        log_dir = self.directory.get_log_dir()
        mounts[log_dir] = '/var/log/ceph:z'

        if daemon_id:
            data_dir = self.directory.get_data_dir(
                daemon_type=daemon_type, daemon_id=daemon_id)
            cdata_dir = '/var/lib/ceph/%s/ceph-%s' % (daemon_type, daemon_id)
            mounts[data_dir] = cdata_dir + ':z'
            mounts[data_dir + '/config'] = '/etc/ceph/ceph.conf:z'

        if daemon_type in ['mon', 'osd']:
            mounts['/dev'] = '/dev:z'  # FIXME: narrow this down?
            mounts['/run/udev'] = '/run/udev:z'
        if daemon_type == 'osd':
            mounts[
                '/sys'] = '/sys:z'  # for numa.cc, pick_address, cgroups, ...
            mounts['/run/lvm'] = '/run/lvm:z'
            mounts['/run/lock/lvm'] = '/run/lock/lvm:z'

        return mounts

    def deploy_daemon_units(self,
                            daemon_type,
                            daemon_id,
                            container,
                            enable=True,
                            start=True):
        # cmd

        data_dir = self.directory.get_data_dir(
            daemon_type=daemon_type, daemon_id=daemon_id)
        with open(data_dir + '/cmd', 'w') as f:
            f.write('#!/bin/sh\n' + ' '.join(container.run_cmd()) + '\n')
            os.fchmod(f.fileno(), 0o700)

        # systemd
        self.install_base_units()

        unit = self.get_unit_file()
        unit_file = 'ceph-%s@.service' % (self.fsid)
        with open(self.unit_dir + '/' + unit_file + '.new', 'w') as f:
            f.write(unit)
            os.rename(self.unit_dir + '/' + unit_file + '.new',
                      self.unit_dir + '/' + unit_file)
        subprocess.check_output(['systemctl', 'daemon-reload'])

        unit_name = get_unit_name(self.fsid, daemon_type, daemon_id)

        if enable:
            subprocess.check_output(['systemctl', 'enable', unit_name])
        if start:
            subprocess.check_output(['systemctl', 'start', unit_name])

    def install_base_units(self):
        """
        Set up ceph.target and ceph-$fsid.target units.
        """
        # TODO: args
        existed = os.path.exists(self.unit_dir + '/ceph.target')
        with open(self.unit_dir + '/ceph.target.new', 'w') as f:
            f.write('[Unit]\n'
                    'Description=all ceph service\n'
                    '[Install]\n'
                    'WantedBy=multi-user.target\n')
            os.rename(self.unit_dir + '/ceph.target.new',
                      self.unit_dir + '/ceph.target')
        if not existed:
            subprocess.check_output(['systemctl', 'enable', 'ceph.target'])
            subprocess.check_output(['systemctl', 'start', 'ceph.target'])

        existed = os.path.exists(self.unit_dir + '/ceph-%s.target' % self.fsid)
        with open(self.unit_dir + '/ceph-%s.target.new' % self.fsid, 'w') as f:
            f.write('[Unit]\n'
                    'Description=ceph cluster {fsid}\n'
                    'PartOf=ceph.target\n'
                    'Before=ceph.target\n'
                    '[Install]\n'
                    'WantedBy=multi-user.target ceph.target\n'.format(
                        fsid=self.fsid))
            os.rename(self.unit_dir + '/ceph-%s.target.new' % self.fsid,
                      self.unit_dir + '/ceph-%s.target' % self.fsid)
        if not existed:
            subprocess.check_output(
                ['systemctl', 'enable',
                 'ceph-%s.target' % self.fsid])
            subprocess.check_output(
                ['systemctl', 'start',
                 'ceph-%s.target' % self.fsid])

    def get_unit_file(self):
        u = """[Unit]
    Description=Ceph daemon for {fsid}

    # According to:
    #   http://www.freedesktop.org/wiki/Software/systemd/NetworkTarget
    # these can be removed once ceph-mon will dynamically change network
    # configuration.
    After=network-online.target local-fs.target time-sync.target
    Wants=network-online.target local-fs.target time-sync.target

    PartOf=ceph-{fsid}.target
    Before=ceph-{fsid}.target

    [Service]
    LimitNOFILE=1048576
    LimitNPROC=1048576
    EnvironmentFile=-/etc/environment
    ExecStartPre=-{podman_path} rm ceph-{fsid}-%i
    ExecStartPre=-mkdir -p /var/run/ceph
    ExecStart={data_dir}/{fsid}/%i/cmd
    ExecStop=-{podman_path} stop ceph-{fsid}-%i
    ExecStopPost=-/bin/rm -f /var/run/ceph/{fsid}-%i.asok
    Restart=on-failure
    RestartSec=10s
    TimeoutStartSec=120
    TimeoutStopSec=15
    StartLimitInterval=30min
    StartLimitBurst=5

    [Install]
    WantedBy=ceph-{fsid}.target
    """.format(
            podman_path='/usr/bin/podman',
            fsid=self.fsid,
            data_dir=self.data_dir)
        return u

    def deploy_daemon(self,
                      daemon_type,
                      daemon_id,
                      container=None,
                      config=None,
                      keyring=None):
        # TODO: args
        if daemon_type == 'mon':
            if not keyring:
                # TODO
                raise
            # tmp keyring file
            tmp_keyring = tempfile.NamedTemporaryFile(mode='w')
            os.fchmod(tmp_keyring.fileno(), 0o600)
            os.fchown(tmp_keyring.fileno(), uid, gid)
            tmp_keyring.write(keyring)
            tmp_keyring.flush()

            # tmp config file
            if not config:
                # TODO
                raise
            tmp_config = tempfile.NamedTemporaryFile(mode='w')
            os.fchmod(tmp_config.fileno(), 0o600)
            os.fchown(tmp_config.fileno(), uid, gid)
            tmp_config.write(config)
            tmp_config.flush()

            # --mkfs
            self.directory.create_daemon_dirs(
                daemon_type=daemon_type, daemon_id=daemon_id)

            mon_dir = get_data_dir(fsid, 'mon', daemon_id)
            log_dir = get_log_dir(fsid)

            out = CephContainer(
                image=self.image,
                entrypoint='/usr/bin/ceph-mon',
                args=[
                    '--mkfs',
                    '-i',
                    daemon_id,
                    '--fsid',
                    fsid,
                    '-c',
                    '/tmp/config',
                    '--keyring',
                    '/tmp/keyring',
                ] + get_daemon_args(self.fsid, 'mon', daemon_id),
                volume_mounts={
                    log_dir: '/var/log/ceph:z',
                    mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % (daemon_id),
                    tmp_keyring.name: '/tmp/keyring:z',
                    tmp_config.name: '/tmp/config:z',
                },
            ).run()

            # write conf
            with open(mon_dir + '/config', 'w') as f:
                os.fchown(f.fileno(), uid, gid)
                os.fchmod(f.fileno(), 0o600)
                f.write(config)
        else:
            # TODO: this is only for bootstrapping
            with open(self.output_config, 'r') as _fd:
                config = _fd.read()

            self.directory.create_daemon_dirs(
                daemon_type=daemon_type,
                daemon_id=daemon_id,
                config=config,
                keyring=keyring)

        if daemon_type == 'osd' and args.osd_fsid:
            pc = CephContainer(
                image=args.image,
                entrypoint='/usr/sbin/ceph-volume',
                args=[
                    'lvm', 'activate', daemon_id, args.osd_fsid, '--no-systemd'
                ],
                podman_args=['--privileged'],
                volume_mounts=get_container_mounts(fsid, daemon_type,
                                                   daemon_id),
                cname='ceph-%s-activate-%s.%s' % (fsid, daemon_type,
                                                  daemon_id),
            )
            pc.run()

        self.deploy_daemon_units(daemon_type, daemon_id, container)

    def wait_for_health(self):
        while True:
            out = CephContainer(
                image=self.image,
                entrypoint='/usr/bin/ceph',
                args=[
                    '-k', '/etc/ceph/bootstrap/ceph.keyring', '-c',
                    '/var/lib/ceph/mon/ceph-admin/config', 'status', '-f',
                    'json-pretty'
                ],
                volume_mounts={
                    # TODO: remove hardcoded values (mon, admin)
                    f'/var/lib/ceph/{self.fsid}/mon.admin': '/var/lib/ceph/mon/ceph-admin:z',
                    f'/etc/ceph/bootstrap': '/etc/ceph/bootstrap'
                },
            ).run()

            json_out = json.loads(out.stdout)
            if json_out.get('mgrmap', {}).get('available', False):
                logging.info('mgr is up')
                break

            logging.info('mgr is still not available yet, waiting...')
            time.sleep(1)
