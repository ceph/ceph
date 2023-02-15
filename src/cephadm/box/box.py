#!/bin/python3
import argparse
import os
import stat
import sys
import host
import osd
from util import (
    Config,
    Target,
    ensure_inside_container,
    ensure_outside_container,
    get_boxes_container_info,
    run_cephadm_shell_command,
    run_dc_shell_command,
    run_dc_shell_commands,
    run_shell_command,
    run_shell_commands,
    colored,
    engine,
    engine_compose,
    Colors
)

CEPH_IMAGE = 'quay.ceph.io/ceph-ci/ceph:main'
BOX_IMAGE = 'cephadm-box:latest'

# NOTE: this image tar is a trickeroo so cephadm won't pull the image everytime
# we deploy a cluster. Keep in mind that you'll be responsible for pulling the
# image yourself with `./box.py -v cluster setup`
CEPH_IMAGE_TAR = 'docker/ceph/image/quay.ceph.image.tar'
CEPH_ROOT = '../../../'
DASHBOARD_PATH = '../../../src/pybind/mgr/dashboard/frontend/'

root_error_msg = """
WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
sudo with this script can kill your computer, try again without sudo
if you value your time.
"""

def remove_ceph_image_tar():
    if os.path.exists(CEPH_IMAGE_TAR):
        os.remove(CEPH_IMAGE_TAR)


def cleanup_box() -> None:
    osd.cleanup()
    remove_ceph_image_tar()


def image_exists(image_name: str):
    # extract_tag
    assert image_name.find(':')
    image_name, tag = image_name.split(':')
    images = run_shell_command(f'{engine()} image ls').split('\n')
    IMAGE_NAME = 0
    TAG = 1
    for image in images:
        image = image.split()
        print(image)
        print(image_name, tag)
        if image[IMAGE_NAME] == image_name and image[TAG] == tag:
            return True
    return False


def get_ceph_image():
    print('Getting ceph image')
    run_shell_command(f'{engine()} pull {CEPH_IMAGE}')
    # update
    run_shell_command(f'{engine()} build -t {CEPH_IMAGE} docker/ceph')
    if not os.path.exists('docker/ceph/image'):
        os.mkdir('docker/ceph/image')

    remove_ceph_image_tar()

    run_shell_command(f'{engine()} save {CEPH_IMAGE} -o {CEPH_IMAGE_TAR}')
    run_shell_command(f'chmod 777 {CEPH_IMAGE_TAR}')
    print('Ceph image added')


def get_box_image():
    print('Getting box image')
    if engine() == 'docker':
        run_shell_command(f'{engine()} build -t cephadm-box -f DockerfileDocker .')
    else:
        run_shell_command(f'{engine()} build -t cephadm-box -f DockerfilePodman .')
    print('Box image added')

def check_dashboard():
    if not os.path.exists(os.path.join(CEPH_ROOT, 'dist')):
        print(colored('Missing build in dashboard', Colors.WARNING))

def check_cgroups():
    if not os.path.exists('/sys/fs/cgroup/cgroup.controllers'):
        print(colored('cgroups v1 is not supported', Colors.FAIL))
        print('Enable cgroups v2 please')
        sys.exit(666)

def check_selinux():
    selinux = run_shell_command('getenforce')
    if 'Disabled' not in selinux:
        print(colored('selinux should be disabled, please disable it if you '
                       'don\'t want unexpected behaviour.', Colors.WARNING))


class Cluster(Target):
    _help = 'Manage docker cephadm boxes'
    actions = ['bootstrap', 'start', 'down', 'list', 'sh', 'setup', 'cleanup']

    def set_args(self):
        self.parser.add_argument(
            'action', choices=Cluster.actions, help='Action to perform on the box'
        )
        self.parser.add_argument('--osds', type=int, default=3, help='Number of osds')

        self.parser.add_argument('--hosts', type=int, default=1, help='Number of hosts')
        self.parser.add_argument('--skip-deploy-osds', action='store_true', help='skip deploy osd')
        self.parser.add_argument('--skip-create-loop', action='store_true', help='skip create loopback device')
        self.parser.add_argument('--skip-monitoring-stack', action='store_true', help='skip monitoring stack')
        self.parser.add_argument('--skip-dashboard', action='store_true', help='skip dashboard')
        self.parser.add_argument('--expanded', action='store_true', help='deploy 3 hosts and 3 osds')

    @ensure_outside_container
    def setup(self):
        if engine() == 'podman':
            run_shell_command('pip3 install https://github.com/containers/podman-compose/archive/devel.tar.gz')

        check_cgroups()
        check_selinux()

        get_ceph_image()
        get_box_image()

    @ensure_outside_container
    def cleanup(self):
        cleanup_box()

    @ensure_inside_container
    def bootstrap(self):
        print('Running bootstrap on seed')
        cephadm_path = os.environ.get('CEPHADM_PATH')
        os.symlink('/cephadm/cephadm', cephadm_path)


        if engine() == 'docker':
            # restart to ensure docker is using daemon.json
            run_shell_command(
                'systemctl restart docker'
            )

        st = os.stat(cephadm_path)
        os.chmod(cephadm_path, st.st_mode | stat.S_IEXEC)

        run_shell_command(f'{engine()} load < /cephadm/box/docker/ceph/image/quay.ceph.image.tar')
        # cephadm guid error because it sometimes tries to use quay.ceph.io/ceph-ci/ceph:<none>
        # instead of main branch's tag
        run_shell_command('export CEPH_SOURCE_FOLDER=/ceph')
        run_shell_command('export CEPHADM_IMAGE=quay.ceph.io/ceph-ci/ceph:main')
        run_shell_command(
            'echo "export CEPHADM_IMAGE=quay.ceph.io/ceph-ci/ceph:main" >> ~/.bashrc'
        )

        extra_args = []

        extra_args.append('--skip-pull')

        # cephadm prints in warning, let's redirect it to the output so shell_command doesn't
        # complain
        extra_args.append('2>&0')

        extra_args = ' '.join(extra_args)
        skip_monitoring_stack = (
            '--skip-monitoring-stack' if Config.get('skip-monitoring-stack') else ''
        )
        skip_dashboard = '--skip-dashboard' if Config.get('skip-dashboard') else ''

        fsid = Config.get('fsid')
        config_folder = Config.get('config_folder')
        config = Config.get('config')
        keyring = Config.get('keyring')
        if not os.path.exists(config_folder):
            os.mkdir(config_folder)

        cephadm_bootstrap_command = (
            '$CEPHADM_PATH --verbose bootstrap '
            '--mon-ip "$(hostname -i)" '
            '--allow-fqdn-hostname '
            '--initial-dashboard-password admin '
            '--dashboard-password-noupdate '
            '--shared_ceph_folder /ceph '
            '--allow-overwrite '
            f'--output-config {config} '
            f'--output-keyring {keyring} '
            f'--output-config {config} '
            f'--fsid "{fsid}" '
            '--log-to-file '
            f'{skip_dashboard} '
            f'{skip_monitoring_stack} '
            f'{extra_args} '
        )

        print('Running cephadm bootstrap...')
        run_shell_command(cephadm_bootstrap_command)
        print('Cephadm bootstrap complete')

        run_shell_command('sudo vgchange --refresh')
        run_shell_command('cephadm ls')
        run_shell_command('ln -s /ceph/src/cephadm/box/box.py /usr/bin/box')

        run_cephadm_shell_command('ceph -s')

        print('Bootstrap completed!')

    @ensure_outside_container
    def start(self):
        check_cgroups()
        check_selinux()
        osds = Config.get('osds')
        hosts = Config.get('hosts')

        # ensure boxes don't exist
        self.down()

        # podman is ran without sudo
        if engine() == 'podman':
            I_am = run_shell_command('whoami')
            if 'root' in I_am:
                print(root_error_msg)
                sys.exit(1)

        print('Checking docker images')
        if not image_exists(CEPH_IMAGE):
            get_ceph_image()
        if not image_exists(BOX_IMAGE):
            get_box_image()

        used_loop = ""
        if not Config.get('skip_create_loop'):
            print('Adding logical volumes (block devices) in loopback device...')
            used_loop = osd.create_loopback_devices(osds)
            print(f'Added {osds} logical volumes in a loopback device')
        loop_device_arg = ""
        if used_loop:
            loop_device_arg = f'--device {used_loop} -v /dev/vg1:/dev/vg1:Z'
            for o in range(osds):
                loop_device_arg += f' --device /dev/dm-{o}'

        print('Starting containers')

        if engine() == 'docker':
            dcflags = f'-f {Config.get("docker_yaml")}'
            if not os.path.exists('/sys/fs/cgroup/cgroup.controllers'):
                dcflags += f' -f {Config.get("docker_v1_yaml")}'
            run_shell_command(f'{engine_compose()} {dcflags} up --scale hosts={hosts} -d')
        else:
            run_shell_command(f'{engine_compose()} -f {Config.get("podman_yaml")} --podman-run-args "--group-add keep-groups --network=host --device /dev/fuse -it {loop_device_arg}" up --scale hosts={hosts} -d')

        run_shell_command('sudo sysctl net.ipv4.conf.all.forwarding=1')
        run_shell_command('sudo iptables -P FORWARD ACCEPT')

        # don't update clock with chronyd / setup chronyd on all boxes
        chronyd_setup = """
        sed 's/$OPTIONS/-x/g' /usr/lib/systemd/system/chronyd.service -i
        systemctl daemon-reload
        systemctl start chronyd
        systemctl status --no-pager chronyd
        """
        for h in range(hosts):
            run_dc_shell_commands(h + 1, 'hosts', chronyd_setup)
        run_dc_shell_commands(1, 'seed', chronyd_setup)

        print('Seting up host ssh servers')
        for h in range(hosts):
            host._setup_ssh('hosts', h + 1)

        host._setup_ssh('seed', 1)

        verbose = '-v' if Config.get('verbose') else ''
        skip_deploy = '--skip-deploy-osds' if Config.get('skip-deploy-osds') else ''
        skip_monitoring_stack = (
            '--skip-monitoring-stack' if Config.get('skip-monitoring-stack') else ''
        )
        skip_dashboard = '--skip-dashboard' if Config.get('skip-dashboard') else ''
        box_bootstrap_command = (
            f'/cephadm/box/box.py {verbose} --engine {engine()} cluster bootstrap '
            f'--osds {osds} '
            f'--hosts {hosts} '
            f'{skip_deploy} '
            f'{skip_dashboard} '
            f'{skip_monitoring_stack} '
        )
        run_dc_shell_command(box_bootstrap_command, 1, 'seed')

        info = get_boxes_container_info()
        ips = info['ips']
        hostnames = info['hostnames']
        print(ips)
        host._copy_cluster_ssh_key(ips)

        expanded = Config.get('expanded')
        if expanded:
            host._add_hosts(ips, hostnames)

        # TODO: add osds
        if expanded and not Config.get('skip-deploy-osds'):
            if engine() == 'podman':
                print('osd deployment not supported in podman')
            else:
                print('Deploying osds... This could take up to minutes')
                osd.deploy_osds_in_vg('vg1')
                print('Osds deployed')

        dashboard_ip = 'localhost'
        info = get_boxes_container_info(with_seed=True)
        if engine() == 'docker':
            for i in range(info['size']):
                if 'seed' in info['container_names'][i]:
                    dashboard_ip = info["ips"][i]
        print(colored(f'dashboard available at https://{dashboard_ip}:8443', Colors.OKGREEN))

        print('Bootstrap finished successfully')

    @ensure_outside_container
    def down(self):
        if engine() == 'podman':
            run_shell_command(f'{engine_compose()} -f {Config.get("podman_yaml")} down')
        else:
            run_shell_command(f'{engine_compose()} -f {Config.get("docker_yaml")} down')
        print('Successfully killed all boxes')

    @ensure_outside_container
    def list(self):
        info = get_boxes_container_info(with_seed=True)
        for i in range(info['size']):
            ip = info['ips'][i]
            name = info['container_names'][i]
            hostname = info['hostnames'][i]
            print(f'{name} \t{ip} \t{hostname}')

    @ensure_outside_container
    def sh(self):
        # we need verbose to see the prompt after running shell command
        Config.set('verbose', True)
        print('Seed bash')
        run_shell_command(f'{engine_compose()} -f {Config.get("docker_yaml")} exec seed bash')


targets = {
    'cluster': Cluster,
    'osd': osd.Osd,
    'host': host.Host,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-v', action='store_true', dest='verbose', help='be more verbose'
    )
    parser.add_argument(
        '--engine', type=str, default='podman',
        dest='engine', help='choose engine between "docker" and "podman"'
    )

    subparsers = parser.add_subparsers()
    target_instances = {}
    for name, target in targets.items():
        target_instances[name] = target(None, subparsers)

    for count, arg in enumerate(sys.argv, 1):
        if arg in targets:
            instance = target_instances[arg]
            if hasattr(instance, 'main'):
                instance.argv = sys.argv[count:]
                instance.set_args()
                args = parser.parse_args()
                Config.add_args(vars(args))
                instance.main()
                sys.exit(0)

    parser.print_help()


if __name__ == '__main__':
    main()
