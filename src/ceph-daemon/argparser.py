import argparse

DEFAULT_IMAGE = 'ceph/daemon-base'
DATA_DIR = '/var/lib/ceph'
LOG_DIR = '/var/log/ceph'
UNIT_DIR = '/etc/systemd/system'

def cmdline_args():

    ##### MAIN PARSERS
    parser = argparse.ArgumentParser(
        description='Bootstrap Ceph daemons with systemd and containers.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '--image', default=DEFAULT_IMAGE, help='container image')

    parser.add_argument(
        '--docker',
        action='store_true',
        help='use docker instead of podman',
        default=False)

    parser.add_argument(
        '--data-dir', default=DATA_DIR, help='base directory for daemon data')

    parser.add_argument(
        '--log-dir', default=LOG_DIR, help='base directory for daemon logs')

    parser.add_argument(
        '--unit-dir',
        default=UNIT_DIR,
        help='base directory for systemd units')

    parser.add_argument('--version', action='version', version='%(prog)s 2.0')

    ###### SUBPARSERS
    subparsers = parser.add_subparsers(help='sub-command', dest='command')

    ####### version parser
    parser_version = subparsers.add_parser(
        'version', help='get ceph version from container')

    ####### ls parser
    parser_ls = subparsers.add_parser(
        'ls', help='list daemon instances on this host')

    ####### adopt parser
    parser_adopt = subparsers.add_parser(
        'adopt', help='adopt daemon deployed with a different tool')

    parser_adopt.add_argument(
        '--name', '-n', required=True, help='daemon name (type.id)')

    parser_adopt.add_argument(
        '--style', required=True, help='deployment style (legacy, ...)')

    parser_adopt.add_argument('--cluster', default='ceph', help='cluster name')

    ####### rm-daemon parser
    parser_rm_daemon = subparsers.add_parser(
        'rm-daemon', help='remove daemon instance')

    parser_rm_daemon.add_argument(
        '--name', '-n', required=True, help='daemon name (type.id)')

    parser_rm_daemon.add_argument('--fsid', required=True, help='cluster FSID')

    parser_rm_daemon.add_argument(
        '--force',
        action='store_true',
        help='proceed, even though this may destroy valuable data')

    ###### rm-cluster parser
    parser_rm_cluster = subparsers.add_parser(
        'rm-cluster', help='remove all daemons for a cluster')

    parser_rm_cluster.add_argument(
        '--fsid', required=True, help='cluster FSID')

    parser_rm_cluster.add_argument(
        '--force',
        action='store_true',
        help='proceed, even though this may destroy valuable data')

    ##### run parser
    parser_run = subparsers.add_parser(
        'run', help='run a ceph daemon, in a container, in the foreground')

    parser_run.add_argument(
        '--name', '-n', required=True, help='daemon name (type.id)')

    parser_run.add_argument('--fsid', required=True, help='cluster FSID')

    ##### shell parser
    parser_shell = subparsers.add_parser(
        'shell', help='run an interactive shell inside a daemon container')

    parser_shell.add_argument('--fsid', help='cluster FSID')

    parser_shell.add_argument('--name', '-n', help='daemon name (type.id)')

    parser_shell.add_argument(
        '--config', '-c', help='ceph.conf to pass through to the container')

    parser_shell.add_argument(
        '--keyring',
        '-k',
        help='ceph.keyring to pass through to the container')

    ##### enter parser
    parser_enter = subparsers.add_parser(
        'enter',
        help='run an interactive shell inside a running daemon container')

    parser_enter.add_argument('--fsid', required=True, help='cluster FSID')

    parser_enter.add_argument(
        '--name', '-n', required=True, help='daemon name (type.id)')

    ##### exec parser
    parser_exec = subparsers.add_parser(
        'exec', help='run command inside a running daemon container')

    parser_exec.add_argument('--fsid', required=True, help='cluster FSID')

    parser_exec.add_argument(
        '--name', '-n', required=True, help='daemon name (type.id)')

    parser_exec.add_argument(
        '--privileged', action='store_true', help='use a privileged container')

    parser_exec.add_argument('command', nargs='+', help='command')

    ##### ceph-volume parser
    parser_ceph_volume = subparsers.add_parser(
        'ceph-volume', help='run ceph-volume inside a container')

    parser_ceph_volume.add_argument(
        '--fsid', required=True, help='cluster FSID')

    parser_ceph_volume.add_argument(
        '--config-and-keyring',
        help='JSON file with config and (client.bootrap-osd) key')

    parser_ceph_volume.add_argument('command', nargs='+', help='command')

    #### unit parser
    parser_unit = subparsers.add_parser(
        'unit', help='operate on the daemon\'s systemd unit')

    parser_unit.add_argument(
        'command',
        help='systemd command (start, stop, restart, enable, disable, ...)')

    parser_unit.add_argument('--fsid', required=True, help='cluster FSID')

    parser_unit.add_argument(
        '--name', '-n', required=True, help='daemon name (type.id)')

    ##### bootstrap parser
    parser_bootstrap = subparsers.add_parser(
        'bootstrap', help='bootstrap a cluster (mon + mgr daemons)')

    parser_bootstrap.add_argument(
        '--config', '-c', help='ceph conf file to incorporate')

    parser_bootstrap.add_argument(
        '--mon-id', required=False, help='mon id (default: local hostname)')

    parser_bootstrap.add_argument(
        '--mon-addrv',
        help='mon IPs (e.g., [v2:localipaddr:3300,v1:localipaddr:6789])')

    parser_bootstrap.add_argument('--mon-ip', help='mon IP')

    parser_bootstrap.add_argument(
        '--mgr-id', required=False, help='mgr id (default: local hostname)')

    parser_bootstrap.add_argument('--fsid', help='cluster FSID')

    parser_bootstrap.add_argument(
        '--output-keyring',
        help=
        'location to write keyring file with new cluster admin and mon keys')
    parser_bootstrap.add_argument(
        '--output-config',
        help='location to write conf file to connect to new cluster')
    parser_bootstrap.add_argument(
        '--output-pub-ssh-key',
        help='location to write the cluster\'s public SSH key')
    parser_bootstrap.add_argument(
        '--skip-ssh',
        action='store_true',
        help='skip setup of ssh key on local host')

    ##### deploy parser
    parser_deploy = subparsers.add_parser('deploy', help='deploy a daemon')
    parser_deploy.add_argument(
        '--name', required=True, help='daemon name (type.id)')
    parser_deploy.add_argument('--fsid', required=True, help='cluster FSID')
    parser_deploy.add_argument(
        '--config', '-c', help='config file for new daemon')
    parser_deploy.add_argument('--keyring', help='keyring for new daemon')
    parser_deploy.add_argument('--key', help='key for new daemon')
    parser_deploy.add_argument(
        '--config-and-keyring', help='JSON file with config and key')
    parser_deploy.add_argument('--mon-ip', help='mon IP')
    parser_deploy.add_argument('--mon-network', help='mon network (CIDR)')
    parser_deploy.add_argument(
        '--osd-fsid', help='OSD uuid, if creating an OSD container')

    return parser.parse_args()
