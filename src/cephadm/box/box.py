#!/bin/python3
import argparse
import os
import stat
import json
import sys
import host
import osd
from multiprocessing import Pool
from util import (
    Config,
    Target,
    ensure_inside_container,
    ensure_outside_container,
    get_boxes_container_info,
    run_cephadm_shell_command,
    run_dc_shell_command,
    run_dc_shell_commands,
    get_container_engine,
    run_shell_command,
    DockerEngine,
    PodmanEngine,
    colored,
    engine_compose,
    Colors,
    get_seed_name
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
    osd.cleanup_osds()
    remove_ceph_image_tar()


def image_exists(image_name: str):
    # extract_tag
    assert image_name.find(':')
    image_name, tag = image_name.split(':')
    engine = get_container_engine()
    images = engine.run('image ls').split('\n')
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
    engine = get_container_engine()
    engine.run(f'pull {CEPH_IMAGE}')
    # update
    engine.run(f'build -t {CEPH_IMAGE} docker/ceph')
    if not os.path.exists('docker/ceph/image'):
        os.mkdir('docker/ceph/image')

    remove_ceph_image_tar()

    engine.run(f'save {CEPH_IMAGE} -o {CEPH_IMAGE_TAR}')
    run_shell_command(f'chmod 777 {CEPH_IMAGE_TAR}')
    print('Ceph image added')


def get_box_image():
    print('Getting box image')
    engine = get_container_engine()
    engine.run(f'build -t cephadm-box -f {engine.dockerfile} .')
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
def dashboard_setup():
    command = f'cd {DASHBOARD_PATH} && npm install'
    run_shell_command(command)
    command = f'cd {DASHBOARD_PATH} && npm run build'
    run_shell_command(command)

class Cluster(Target):
    _help = 'Manage docker cephadm boxes'
    actions = ['bootstrap', 'start', 'down', 'list', 'bash', 'setup', 'cleanup']

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
        self.parser.add_argument('--jobs', type=int, help='Number of jobs scheduled in parallel')

    @ensure_outside_container
    def setup(self):
        check_cgroups()
        check_selinux()

        targets = [
                get_ceph_image,
                get_box_image,
                dashboard_setup
        ]
        results = []
        jobs = Config.get('jobs')
        if jobs:
            jobs = int(jobs)
        else:
            jobs = None
        pool = Pool(jobs)
        for target in targets:
            results.append(pool.apply_async(target))

        for result in results:
            result.wait()


    @ensure_outside_container
    def cleanup(self):
        cleanup_box()

    @ensure_inside_container
    def bootstrap(self):
        print('Running bootstrap on seed')
        cephadm_path = str(os.environ.get('CEPHADM_PATH'))

        engine = get_container_engine()
        if isinstance(engine, DockerEngine):
            engine.restart()
        st = os.stat(cephadm_path)
        os.chmod(cephadm_path, st.st_mode | stat.S_IEXEC)

        engine.run('load < /cephadm/box/docker/ceph/image/quay.ceph.image.tar')
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
        config_folder = str(Config.get('config_folder'))
        config = str(Config.get('config'))
        keyring = str(Config.get('keyring'))
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
        run_shell_command(cephadm_bootstrap_command, expect_exit_code=120) 
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
        osds = int(Config.get('osds'))
        hosts = int(Config.get('hosts'))
        engine = get_container_engine()

        # ensure boxes don't exist
        self.down()

        # podman is ran without sudo
        if isinstance(engine, PodmanEngine):
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
            print('Creating OSD devices...')
            used_loop = osd.create_loopback_devices(osds)
            print(f'Added {osds} logical volumes in a loopback device')

        print('Starting containers')

        engine.up(hosts)

        containers = engine.get_containers()
        seed = engine.get_seed()
        # Umounting somehow brings back the contents of the host /sys/dev/block. 
        # On startup /sys/dev/block is empty. After umount, we can see symlinks again
        # so that lsblk is able to run as expected
        run_dc_shell_command('umount /sys/dev/block', seed)

        run_shell_command('sudo sysctl net.ipv4.conf.all.forwarding=1')
        run_shell_command('sudo iptables -P FORWARD ACCEPT')

        # don't update clock with chronyd / setup chronyd on all boxes
        chronyd_setup = """
        sed 's/$OPTIONS/-x/g' /usr/lib/systemd/system/chronyd.service -i
        systemctl daemon-reload
        systemctl start chronyd
        systemctl status --no-pager chronyd
        """
        for container in containers:
            print(colored('Got container:', Colors.OKCYAN), str(container))
        for container in containers:
            run_dc_shell_commands(chronyd_setup, container)

        print('Seting up host ssh servers')
        for container in containers:
            print(colored('Setting up ssh server for:', Colors.OKCYAN), str(container))
            host._setup_ssh(container)

        verbose = '-v' if Config.get('verbose') else ''
        skip_deploy = '--skip-deploy-osds' if Config.get('skip-deploy-osds') else ''
        skip_monitoring_stack = (
            '--skip-monitoring-stack' if Config.get('skip-monitoring-stack') else ''
        )
        skip_dashboard = '--skip-dashboard' if Config.get('skip-dashboard') else ''
        box_bootstrap_command = (
            f'/cephadm/box/box.py {verbose} --engine {engine.command} cluster bootstrap '
            f'--osds {osds} '
            f'--hosts {hosts} '
            f'{skip_deploy} '
            f'{skip_dashboard} '
            f'{skip_monitoring_stack} '
        )
        print(box_bootstrap_command)
        run_dc_shell_command(box_bootstrap_command, seed)

        expanded = Config.get('expanded')
        if expanded:
            info = get_boxes_container_info()
            ips = info['ips']
            hostnames = info['hostnames']
            print(ips)
            if hosts > 0:
                host._copy_cluster_ssh_key(ips)
                host._add_hosts(ips, hostnames)
            if not Config.get('skip-deploy-osds'):
                print('Deploying osds... This could take up to minutes')
                osd.deploy_osds(osds)
                print('Osds deployed')


        dashboard_ip = 'localhost'
        info = get_boxes_container_info(with_seed=True)
        if isinstance(engine, DockerEngine):
            for i in range(info['size']):
                if get_seed_name() in info['container_names'][i]:
                    dashboard_ip = info["ips"][i]
        print(colored(f'dashboard available at https://{dashboard_ip}:8443', Colors.OKGREEN))

        print('Bootstrap finished successfully')

    @ensure_outside_container
    def down(self):
        engine = get_container_engine()
        if isinstance(engine, PodmanEngine):
            containers = json.loads(engine.run('container ls --format json'))
            for container in containers:
                for name in container['Names']:
                    if name.startswith('box_hosts_'):
                        engine.run(f'container kill {name}')
                        engine.run(f'container rm {name}')
            pods = json.loads(engine.run('pod ls --format json'))
            for pod in pods:
                if 'Name' in pod and pod['Name'].startswith('box_pod_host'):
                    name = pod['Name']
                    engine.run(f'pod kill {name}')
                    engine.run(f'pod rm {name}')
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
    def bash(self):
        # we need verbose to see the prompt after running shell command
        Config.set('verbose', True)
        print('Seed bash')
        engine = get_container_engine()
        engine.run(f'exec -it {engine.seed_name} bash')


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
