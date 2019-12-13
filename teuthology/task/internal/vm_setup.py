import logging
import os
import subprocess

from teuthology.parallel import parallel
from teuthology.task import ansible
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


def vm_setup(ctx, config):
    """
    Look for virtual machines and handle their initialization
    """
    all_tasks = [list(x.keys())[0] for x in ctx.config['tasks']]
    need_ansible = False
    if 'kernel' in all_tasks and 'ansible.cephlab' not in all_tasks:
        need_ansible = True
    ansible_hosts = set()
    with parallel():
        editinfo = os.path.join(os.path.dirname(__file__), 'edit_sudoers.sh')
        for rem in ctx.cluster.remotes.keys():
            if rem.is_vm:
                ansible_hosts.add(rem.shortname)
                try:
                    rem.sh('test -e /ceph-qa-ready')
                except CommandFailedError:
                    p1 = subprocess.Popen(['cat', editinfo],
                                          stdout=subprocess.PIPE)
                    p2 = subprocess.Popen(
                        [
                            'ssh',
                            '-o', 'StrictHostKeyChecking=no',
                            '-t', '-t',
                            str(rem),
                            'sudo',
                            'sh'
                        ],
                        stdin=p1.stdout, stdout=subprocess.PIPE
                    )
                    _, err = p2.communicate()
                    if err:
                        log.error("Edit of /etc/sudoers failed: %s", err)
    if need_ansible and ansible_hosts:
        log.info("Running ansible on %s", list(ansible_hosts))
        ansible_config = dict(
            hosts=list(ansible_hosts),
        )
        with ansible.CephLab(ctx, config=ansible_config):
            pass
