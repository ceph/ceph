class SSH(object):


    def __init__(self, config):
        self.config = config

    def __getattr__(self, attr):
        # Inheritance vs Composition..
        return getattr(self.config, attr)


    def do_ssh_things(self):
        if self.skip_ssh:
            return True
        logging.info('Generating ssh key...')
        (ssh_key, ssh_pub) = gen_ssh_key(fsid)

        tmp_key = tempfile.NamedTemporaryFile(mode='w')
        os.fchmod(tmp_key.fileno(), 0o600)
        os.fchown(tmp_key.fileno(), uid, gid)
        tmp_key.write(ssh_key)
        tmp_key.flush()
        tmp_pub = tempfile.NamedTemporaryFile(mode='w')
        os.fchmod(tmp_pub.fileno(), 0o600)
        os.fchown(tmp_pub.fileno(), uid, gid)
        tmp_pub.write(ssh_pub)
        tmp_pub.flush()

        if args.output_pub_ssh_key:
            with open(args.output_put_ssh_key, 'w') as f:
                f.write(ssh_pub)
            logging.info(
                'Wrote public SSH key to to %s' % args.output_pub_ssh_key)

        CephContainer(
            image=args.image,
            entrypoint='/usr/bin/ceph',
            args=[
                '-n', 'mon.', '-k',
                '/var/lib/ceph/mon/ceph-%s/keyring' % mon_id, '-c',
                '/var/lib/ceph/mon/ceph-%s/config' % mon_id, 'config-key',
                'set', 'mgr/ssh/ssh_identity_key', '-i', '/tmp/key'
            ],
            volume_mounts={
                mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % (mon_id),
                tmp_key.name: '/tmp/key:z',
            },
        ).run()
        CephContainer(
            image=args.image,
            entrypoint='/usr/bin/ceph',
            args=[
                '-n', 'mon.', '-k',
                '/var/lib/ceph/mon/ceph-%s/keyring' % mon_id, '-c',
                '/var/lib/ceph/mon/ceph-%s/config' % mon_id, 'config-key',
                'set', 'mgr/ssh/ssh_identity_pub', '-i', '/tmp/pub'
            ],
            volume_mounts={
                mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % (mon_id),
                tmp_pub.name: '/tmp/pub:z',
            },
        ).run()

        logging.info('Adding key to root@localhost\'s authorized_keys...')
        with open('/root/.ssh/authorized_keys', 'a') as f:
            os.fchmod(f.fileno(), 0o600)  # just in case we created it
            f.write(ssh_pub + '\n')

        logging.info('Enabling ssh module...')
        CephContainer(
            image=args.image,
            entrypoint='/usr/bin/ceph',
            args=[
                '-n', 'mon.', '-k',
                '/var/lib/ceph/mon/ceph-%s/keyring' % mon_id, '-c',
                '/var/lib/ceph/mon/ceph-%s/config' % mon_id, 'mgr',
                'module', 'enable', 'ssh'
            ],
            volume_mounts={
                mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % (mon_id),
                tmp_pub.name: '/tmp/pub:z',
            },
        ).run()
        logging.info('Setting orchestrator backend to ssh...')
        CephContainer(
            image=args.image,
            entrypoint='/usr/bin/ceph',
            args=[
                '-n', 'mon.', '-k',
                '/var/lib/ceph/mon/ceph-%s/keyring' % mon_id, '-c',
                '/var/lib/ceph/mon/ceph-%s/config' % mon_id,
                'orchestrator', 'set', 'backend', 'ssh'
            ],
            volume_mounts={
                mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % (mon_id),
                tmp_pub.name: '/tmp/pub:z',
            },
        ).run()
        host = get_hostname()
        logging.info('Adding host %s...' % host)
        CephContainer(
            image=args.image,
            entrypoint='/usr/bin/ceph',
            args=[
                '-n', 'mon.', '-k',
                '/var/lib/ceph/mon/ceph-%s/keyring' % mon_id, '-c',
                '/var/lib/ceph/mon/ceph-%s/config' % mon_id,
                'orchestrator', 'host', 'add', host
            ],
            volume_mounts={
                mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % (mon_id),
                tmp_pub.name: '/tmp/pub:z',
            },
        ).run()
        return True
