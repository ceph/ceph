'''
Task that deploys a CAASP cluster on all the nodes
Linter:
    flake8 --max-line-length=100
'''
import logging
from teuthology.misc import (
    all_roles_of_type,
    append_lines_to_file,
    get_file,
    )
from teuthology.task import Task
from tasks.util import get_remote_for_role
log = logging.getLogger(__name__)


class Caasp(Task):
    """
    Deploy a Caasp cluster on all remotes using Skuba.
    Nodes are declared in suites/caasp folder
    """

    def __init__(self, ctx, config):
        super(Caasp, self).__init__(ctx, config)
        log.debug("beginning of constructor method")
        self.ctx['roles'] = self.ctx.config['roles']
        self.log = log
        self.remotes = self.cluster.remotes
        self.mgmt = get_remote_for_role(self.ctx, "skuba_mgmt_host.0")
        self.ssh_priv = 'caasp_key.rsa'
        self.ssh_pub = 'caasp_key.rsa.pub'
        self.set_agent = "eval $(ssh-agent) && ssh-add ~/{} && ".format(
            self.ssh_priv)

    def __ssh_setup(self):
        """ Generate keys on management node. Copy pub to all of them. """
        log.debug("Executing SSH setup")
        self.__ssh_gen_key()
        self.__ssh_copy_pub_to_caasp()

    def __ssh_gen_key(self):
        self.mgmt.sh('ssh-keygen -t rsa -b 2048 -N "" -f {}'.format(self.ssh_priv))

    def __ssh_copy_pub_to_caasp(self):
        log.debug("Copying public key to remotes")
        data = get_file(self.mgmt, self.ssh_pub)
        for i, _ in enumerate(all_roles_of_type(self.ctx.cluster, 'caasp_master')):
            r = get_remote_for_role(self.ctx, 'caasp_master.' + str(i))
            append_lines_to_file(r, '.ssh/authorized_keys', data)
        for i, _ in enumerate(all_roles_of_type(self.ctx.cluster, 'caasp_worker')):
            r = get_remote_for_role(self.ctx, 'caasp_worker.' + str(i))
            append_lines_to_file(r, '.ssh/authorized_keys', data)

    def __create_cluster(self):
        log.debug('Creating cluster')
        master_remote = get_remote_for_role(self.ctx, "caasp_master.0")
        self.mgmt.sh(self.set_agent + """
            ssh-add -L
            skuba cluster init --control-plane {master} cluster
            cd cluster
            skuba node bootstrap --user {user} --sudo --target {master} my-master
            """.format(master=master_remote.hostname, user='ubuntu'))

        for i, _ in enumerate(all_roles_of_type(self.ctx.cluster, 'caasp_worker')):
            r = get_remote_for_role(self.ctx, 'caasp_worker.' + str(i))
            command = "cd cluster;skuba node join --role worker --user ubuntu \
                --sudo --target {} worker.{}".format(r.hostname, str(i))
            self.mgmt.sh("%s %s" % (self.set_agent, command))

    def __show_cluster_status(self):
        command = 'cd cluster && skuba cluster status'
        self.mgmt.sh("%s %s" % (self.set_agent, command))

    def begin(self):
        self.__ssh_setup()
        self.__create_cluster()
        self.__show_cluster_status()

    def end(self):
        pass

    def teardown(self):
        pass


task = Caasp
