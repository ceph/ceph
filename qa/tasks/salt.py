'''
Task that deploys a Salt cluster on all the nodes
'''
import logging

from salt_manager import SaltManager
from teuthology.misc import delete_file, move_file, sh, sudo_write_file
from teuthology.orchestra import run
from teuthology.task import Task

log = logging.getLogger(__name__)


class Salt(Task):
    """
    Deploy a Salt cluster on all remotes (test nodes).

    This task assumes all relevant Salt packages (salt, salt-master,
    salt-minion, salt-api, python-salt, etc. - whatever they may be called for
    the OS in question) are already installed. This should be done using the
    install task.

    One, and only one, of the machines must have a role corresponding to the
    value of the variable salt.sm.master_role (see salt_manager.py). This node
    is referred to as the "Salt Master", or the "master node".

    The task starts the Salt Master daemon on the master node, and Salt Minion
    daemons on all the nodes (including the master node), and ensures that the
    minions are properly linked to the master. Finally, it tries to ping all
    the minions from the Salt Master.

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """

    def __init__(self, ctx, config):
        super(Salt, self).__init__(ctx, config)
        log.debug("beginning of constructor method")
        log.debug("munged config is {}".format(self.config))
        self.remotes = self.cluster.remotes
        self.sm = SaltManager(self.ctx)
        log.debug("end of constructor method")

    def __generate_minion_keys(self):
        '''
        Generate minion key on salt master to be used to preseed this cluster's
        minions.
        '''
        for rem in self.remotes.iterkeys():
            minion_id = rem.hostname
            log.info('Ensuring that minion ID {} has a keypair on the master'
                     .format(minion_id))
            # mode 777 is necessary to be able to generate keys reliably
            # we hit this before:
            # https://github.com/saltstack/salt/issues/31565
            self.sm.master_remote.run(args=[
                'sudo',
                'sh',
                '-c',
                'test -d salt || mkdir -m 777 salt',
            ])
            self.sm.master_remote.run(args=[
                'sudo',
                'sh',
                '-c',
                'test -d salt/minion-keys || mkdir -m 777 salt/minion-keys',
            ])
            self.sm.master_remote.run(args=[
                'sudo',
                'sh',
                '-c',
                ('if [ ! -f salt/minion-keys/{mid}.pem ]; then '
                 'salt-key --gen-keys={mid} '
                 '--gen-keys-dir=salt/minion-keys/; '
                 ' fi').format(mid=minion_id),
            ])

    def __preseed_minions(self):
        '''
        Preseed minions with generated and accepted keys; set minion id
        to the remote's hostname.
        '''
        for rem in self.remotes.iterkeys():
            minion_id = rem.hostname
            src = 'salt/minion-keys/{}.pub'.format(minion_id)
            dest = '/etc/salt/pki/master/minions/{}'.format(minion_id)
            self.sm.master_remote.run(args=[
                'sudo',
                'sh',
                '-c',
                ('if [ ! -f {d} ]; then '
                 'cp {s} {d} ; '
                 'chown root {d} ; '
                 'fi').format(s=src, d=dest)
            ])
            self.sm.master_remote.run(args=[
                'sudo',
                'chown',
                'ubuntu',
                'salt/minion-keys/{}.pem'.format(minion_id),
                'salt/minion-keys/{}.pub'.format(minion_id),
            ])
            #
            # copy the keys via the teuthology VM. The worker VMs can't ssh to
            # each other. scp -3 does a 3-point copy through the teuthology VM.
            sh('scp -3 {}:salt/minion-keys/{}.* {}:'.format(
                self.sm.master_remote.name,
                minion_id, rem.name))
            sudo_write_file(rem, '/etc/salt/minion_id', minion_id)
            #
            # set proper owner and permissions on keys
            rem.run(
                args=[
                    'sudo',
                    'chown',
                    'root',
                    '{}.pem'.format(minion_id),
                    '{}.pub'.format(minion_id),
                    run.Raw(';'),
                    'sudo',
                    'chmod',
                    '600',
                    '{}.pem'.format(minion_id),
                    run.Raw(';'),
                    'sudo',
                    'chmod',
                    '644',
                    '{}.pub'.format(minion_id),
                ],
            )
            #
            # move keys to correct location
            move_file(rem, '{}.pem'.format(minion_id),
                      '/etc/salt/pki/minion/minion.pem', sudo=True,
                      preserve_perms=False)
            move_file(rem, '{}.pub'.format(minion_id),
                      '/etc/salt/pki/minion/minion.pub', sudo=True,
                      preserve_perms=False)

    def __set_minion_master(self):
        """Points all minions to the master"""
        master_id = self.sm.master_remote.hostname
        for rem in self.remotes.iterkeys():
            # remove old master public key if present. Minion will refuse to
            # start if master name changed but old key is present
            delete_file(rem, '/etc/salt/pki/minion/minion_master.pub',
                        sudo=True, check=False)

            # set master id
            sed_cmd = ('echo master: {} > '
                       '/etc/salt/minion.d/master.conf').format(master_id)
            rem.run(args=[
                'sudo',
                'sh',
                '-c',
                sed_cmd,
            ])

    def __set_debug_log_level(self):
        """Sets log_level: debug for all salt daemons"""
        for rem in self.remotes.iterkeys():
            rem.run(args=[
                'sudo',
                'sed', '--in-place', '--regexp-extended',
                '-e', 's/^\s*#\s*log_level:.*$/log_level: debug/g',
                '-e', '/^\s*#.*$/d', '-e', '/^\s*$/d',
                '/etc/salt/master',
                '/etc/salt/minion',
            ])

    def setup(self):
        super(Salt, self).setup()
        log.debug("beginning of setup method")
        self.__generate_minion_keys()
        self.__preseed_minions()
        self.__set_minion_master()
        self.__set_debug_log_level()
        self.sm.start_master()
        self.sm.start_minions()
        self.sm.restart_master()
        log.debug("end of setup method")

    def begin(self):
        super(Salt, self).begin()
        log.debug("beginning of begin method")
        self.sm.check_salt_daemons()
        self.sm.cat_salt_master_conf()
        self.sm.cat_salt_minion_confs()
        self.sm.ping_minions()
        log.debug("end of begin method")

    def end(self):
        super(Salt, self).end()
        log.debug("beginning of end method")
        self.sm.gather_logs('salt')
        log.debug("end of end method")

    def teardown(self):
        super(Salt, self).teardown()
        # log.debug("beginning of teardown method")
        pass
        # log.debug("end of teardown method")


task = Salt
