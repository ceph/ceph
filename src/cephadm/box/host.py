import argparse
import os
from typing import List
from util import inside_container, run_shell_command, run_dc_shell_command, Config

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
class Host:
    _help = 'Run seed/host related commands'
    actions = ['setup_ssh', 'copy_cluster_ssh_key']
    parser = None

    def __init__(self, argv):
        self.argv = argv

    @staticmethod
    def add_parser(subparsers):
        assert not Host.parser
        Host.parser = subparsers.add_parser('host', help=Host._help)
        parser = Host.parser
        parser.add_argument('action', choices=Host.actions)
        parser.add_argument('host_container_index', type=str, help='box_host_{index}')
        parser.add_argument('--ips', nargs='*', help='List of host ips')

    def setup_ssh(self):
        _setup_ssh(Config.get('host_container_index'))


    def copy_cluster_ssh_key(self):
        ips = Config.get('ips')
        if not ips:
            ips = get_host_ips()
        _copy_cluster_ssh_key(ips)
        

    def main(self):
        parser = Host.parser
        args = parser.parse_args(self.argv)
        Config.add_args(vars(args))
        function = getattr(self, args.action)
        function()
