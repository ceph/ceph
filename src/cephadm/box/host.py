import argparse
import os
from typing import List

from util import (Config, Target, inside_container, run_dc_shell_command,
                  run_shell_command)


def _setup_ssh(container_index):
    if inside_container():
        if not os.path.exists('/root/.ssh/known_hosts'):
            run_shell_command('ssh-keygen -A')

        run_shell_command('echo "root:root" | chpasswd')
        with open('/etc/ssh/sshd_config', 'a+') as f:
            f.write('PermitRootLogin yes\n')
            f.write('PasswordAuthentication yes\n')
            f.flush()
        run_shell_command('/usr/sbin/sshd')
    else:
        print('Redirecting to _setup_ssh to container') 
        verbose = '-v' if Config.get('verbose') else ''
        run_dc_shell_command(f'/cephadm/box/box.py {verbose} host setup_ssh {container_index}', container_index, 'hosts')
        

def _copy_cluster_ssh_key(ips: List[str]):
    if inside_container():
        local_ip = run_shell_command('hostname -i')
        for ip in ips:
            if ip != local_ip:
                run_shell_command(('sshpass -p "root" ssh-copy-id -f '
                                    f'-o StrictHostKeyChecking=no -i /etc/ceph/ceph.pub "root@{ip}"'))

    else:
        print('Redirecting to _copy_cluster_ssh to container') 
        verbose = '-v' if Config.get('verbose') else ''
        print(ips)
        ips = ' '.join(ips)
        ips = f"{ips}"
        # assume we only have one seed
        run_dc_shell_command(f'/cephadm/box/box.py {verbose} host copy_cluster_ssh_key 1 --ips {ips}',
                             1, 'seed')
class Host(Target):
    _help = 'Run seed/host related commands'
    actions = ['setup_ssh', 'copy_cluster_ssh_key']

    def set_args(self):
        self.parser.add_argument('action', choices=Host.actions)
        self.parser.add_argument('host_container_index', type=str, help='box_host_{index}')
        self.parser.add_argument('--ips', nargs='*', help='List of host ips')

    def setup_ssh(self):
        _setup_ssh(Config.get('host_container_index'))


    def copy_cluster_ssh_key(self):
        ips = Config.get('ips')
        if not ips:
            ips = get_host_ips()
        _copy_cluster_ssh_key(ips)
