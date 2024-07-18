import json
import os
import subprocess
import sys
import copy
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, List

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class Config:
    args = {
        'fsid': '00000000-0000-0000-0000-0000deadbeef',
        'config_folder': '/etc/ceph/',
        'config': '/etc/ceph/ceph.conf',
        'keyring': '/etc/ceph/ceph.keyring',
        'loop_img': 'loop-images/loop.img',
        'engine': 'podman',
        'docker_yaml': 'docker-compose-docker.yml',
        'docker_v1_yaml': 'docker-compose.cgroup1.yml',
        'podman_yaml': 'docker-compose-podman.yml',
        'loop_img_dir': 'loop-images',
    }

    @staticmethod
    def set(key, value):
        Config.args[key] = value

    @staticmethod
    def get(key):
        if key in Config.args:
            return Config.args[key]
        return None

    @staticmethod
    def add_args(args: Dict[str, str]) -> None:
        Config.args.update(args)

class Target:
    def __init__(self, argv, subparsers):
        self.argv = argv
        self.parser = subparsers.add_parser(
            self.__class__.__name__.lower(), help=self.__class__._help
        )

    def set_args(self):
        """
        adding the required arguments of the target should go here, example:
        self.parser.add_argument(..)
        """
        raise NotImplementedError()

    def main(self):
        """
        A target will be setup by first calling this main function
        where the parser is initialized.
        """
        args = self.parser.parse_args(self.argv)
        Config.add_args(vars(args))
        function = getattr(self, args.action)
        function()


def ensure_outside_container(func) -> Callable:
    def wrapper(*args, **kwargs):
        if not inside_container():
            return func(*args, **kwargs)
        else:
            raise RuntimeError('This command should be ran outside a container')

    return wrapper


def ensure_inside_container(func) -> bool:
    def wrapper(*args, **kwargs):
        if inside_container():
            return func(*args, **kwargs)
        else:
            raise RuntimeError('This command should be ran inside a container')

    return wrapper


def colored(msg, color: Colors):
    return color + msg + Colors.ENDC

class BoxType(str, Enum):
  SEED = 'seed'
  HOST = 'host'

class HostContainer:
    def __init__(self, _name, _type) -> None:
        self._name: str = _name
        self._type: BoxType = _type

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> BoxType:
        return self._type
    def __str__(self) -> str:
        return f'{self.name} {self.type}'

def run_shell_command(command: str, expect_error=False, verbose=True, expect_exit_code=0) -> str:
    if Config.get('verbose'):
        print(f'{colored("Running command", Colors.HEADER)}: {colored(command, Colors.OKBLUE)}')

    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    out = ''
    err = ''
    # let's read when output comes so it is in real time
    while True:
        # TODO: improve performance of this part, I think this part is a problem
        pout = process.stdout.read(1).decode('latin1')
        if pout == '' and process.poll() is not None:
            break
        if pout:
            if Config.get('verbose') and verbose:
                sys.stdout.write(pout)
                sys.stdout.flush()
            out += pout

    process.wait()

    err += process.stderr.read().decode('latin1').strip()
    out = out.strip()

    if process.returncode != 0 and not expect_error and process.returncode != expect_exit_code:
        err = colored(err, Colors.FAIL);
        
        raise RuntimeError(f'Failed command: {command}\n{err}\nexit code: {process.returncode}')
        sys.exit(1)
    return out


def run_dc_shell_commands(commands: str, container: HostContainer, expect_error=False) -> str:
    for command in commands.split('\n'):
        command = command.strip()
        if not command:
            continue
        run_dc_shell_command(command.strip(), container, expect_error=expect_error)

def run_shell_commands(commands: str, expect_error=False) -> str:
    for command in commands.split('\n'):
        command = command.strip()
        if not command:
            continue
        run_shell_command(command, expect_error=expect_error)

@ensure_inside_container
def run_cephadm_shell_command(command: str, expect_error=False) -> str:
    config = Config.get('config')
    keyring = Config.get('keyring')
    fsid = Config.get('fsid')

    with_cephadm_image = 'CEPHADM_IMAGE=quay.ceph.io/ceph-ci/ceph:main'
    out = run_shell_command(
        f'{with_cephadm_image} cephadm --verbose shell --fsid {fsid} --config {config} --keyring {keyring} -- {command}',
        expect_error,
    )
    return out


def run_dc_shell_command(
        command: str, container: HostContainer, expect_error=False
) -> str:
    out = get_container_engine().run_exec(container, command, expect_error=expect_error)
    return out

def inside_container() -> bool:
    return os.path.exists('/.box_container')

def get_container_id(container_name: str):
    return run_shell_command(f"{engine()} ps | \grep " + container_name + " | awk '{ print $1 }'")

def engine():
    return Config.get('engine')

def engine_compose():
    return f'{engine()}-compose'

def get_seed_name():
    if engine() == 'docker':
        return 'seed'
    elif engine() == 'podman':
        return 'box_hosts_0'
    else:
        print(f'unkown engine {engine()}')
        sys.exit(1)


@ensure_outside_container
def get_boxes_container_info(with_seed: bool = False) -> Dict[str, Any]:
    # NOTE: this could be cached
    ips_query = engine() + " inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}} %tab% {{.Name}} %tab% {{.Config.Hostname}}' $("+ engine() + " ps -aq) --format json"
    containers = json.loads(run_shell_command(ips_query, verbose=False))
    # FIXME: if things get more complex a class representing a container info might be useful,
    # for now representing data this way is faster.
    info = {'size': 0, 'ips': [], 'container_names': [], 'hostnames': []}
    for container in containers:
        # Most commands use hosts only
        name = container['Name']
        if name.startswith('box_hosts'):
            if not with_seed and name == get_seed_name():
                continue
            info['size'] += 1
            print(container['NetworkSettings'])
            if 'Networks' in container['NetworkSettings']:
                info['ips'].append(container['NetworkSettings']['Networks']['box_network']['IPAddress'])
            else:
                info['ips'].append('n/a')
            info['container_names'].append(name)
            info['hostnames'].append(container['Config']['Hostname'])
    return info


def get_orch_hosts():
    if inside_container():
        orch_host_ls_out = run_cephadm_shell_command('ceph orch host ls --format json')
    else:
        orch_host_ls_out = run_dc_shell_command(f'cephadm shell --keyring /etc/ceph/ceph.keyring --config /etc/ceph/ceph.conf -- ceph orch host ls --format json', 
                                                get_container_engine().get_seed())
        sp = orch_host_ls_out.split('\n')
        orch_host_ls_out  = sp[len(sp) - 1]
    hosts = json.loads(orch_host_ls_out)
    return hosts


class ContainerEngine(metaclass=ABCMeta):
    @property
    @abstractmethod
    def command(self) -> str: pass

    @property
    @abstractmethod
    def seed_name(self) -> str: pass

    @property
    @abstractmethod
    def dockerfile(self) -> str: pass

    @property
    def host_name_prefix(self) -> str: 
        return 'box_hosts_'

    @abstractmethod
    def up(self, hosts: int): pass

    def run_exec(self, container: HostContainer, command: str, expect_error: bool = False):
        return run_shell_command(' '.join([self.command, 'exec', container.name, command]), 
                                 expect_error=expect_error) 

    def run(self, engine_command: str, expect_error: bool = False):
        return run_shell_command(' '.join([self.command, engine_command]), expect_error=expect_error) 

    def get_containers(self) -> List[HostContainer]:
        ps_out = json.loads(run_shell_command('podman ps --format json'))
        containers = [] 
        for container in ps_out:
            if not container['Names']:
                raise RuntimeError(f'Container {container} missing name')
            name = container['Names'][0]
            if name == self.seed_name:
                containers.append(HostContainer(name, BoxType.SEED))
            elif name.startswith(self.host_name_prefix):
                containers.append(HostContainer(name, BoxType.HOST))
        return containers

    def get_seed(self) -> HostContainer:
        for container in self.get_containers():
            if container.type == BoxType.SEED:
                return container
        raise RuntimeError('Missing seed container')

    def get_container(self, container_name: str):
        containers = self.get_containers()
        for container in containers:
            if container.name == container_name:
                return container
        return None


    def restart(self):
        pass


class DockerEngine(ContainerEngine):
    command = 'docker'
    seed_name = 'seed'
    dockerfile = 'DockerfileDocker'

    def restart(self):
        run_shell_command('systemctl restart docker')

    def up(self, hosts: int):
        dcflags = f'-f {Config.get("docker_yaml")}'
        if not os.path.exists('/sys/fs/cgroup/cgroup.controllers'):
            dcflags += f' -f {Config.get("docker_v1_yaml")}'
        run_shell_command(f'{engine_compose()} {dcflags} up --scale hosts={hosts} -d')

class PodmanEngine(ContainerEngine):
    command = 'podman'
    seed_name = 'box_hosts_0'
    dockerfile = 'DockerfilePodman'

    CAPS = [
            "SYS_ADMIN",
            "NET_ADMIN",
            "SYS_TIME",
            "SYS_RAWIO",
            "MKNOD",
            "NET_RAW",
            "SETUID",
            "SETGID",
            "CHOWN",
            "SYS_PTRACE",
            "SYS_TTY_CONFIG",
            "CAP_AUDIT_WRITE",
            "CAP_AUDIT_CONTROL",
            ]

    VOLUMES = [
                '../../../:/ceph:z',
                '../:/cephadm:z',
                '/run/udev:/run/udev',
                '/sys/dev/block:/sys/dev/block',
                '/sys/fs/cgroup:/sys/fs/cgroup:ro',
                '/dev/fuse:/dev/fuse',
                '/dev/disk:/dev/disk',
                '/sys/devices/virtual/block:/sys/devices/virtual/block',
                '/sys/block:/dev/block',
                '/dev/mapper:/dev/mapper',
                '/dev/mapper/control:/dev/mapper/control',
            ]

    TMPFS = ['/run', '/tmp']

    # FIXME: right now we are assuming every service will be exposed through the seed, but this is far
    # from the truth. Services can be deployed on different hosts so we need a system to manage this.
    SEED_PORTS = [
            8443, # dashboard
            3000, # grafana
            9093, # alertmanager
            9095  # prometheus
            ]


    def setup_podman_env(self, hosts: int = 1, osd_devs={}):
        network_name = 'box_network'
        networks = run_shell_command('podman network ls')
        if network_name not in networks:
            run_shell_command(f'podman network create -d bridge {network_name}')

        args = [
                '--group-add', 'keep-groups', 
                '--device', '/dev/fuse' ,
                '-it' ,
                '-d',
                '-e', 'CEPH_BRANCH=main',
                '--stop-signal', 'RTMIN+3'
                ]

        for cap in self.CAPS:
            args.append('--cap-add')
            args.append(cap)

        for volume in self.VOLUMES:
            args.append('-v')
            args.append(volume)

        for tmp in self.TMPFS:
            args.append('--tmpfs')
            args.append(tmp)


        for osd_dev in osd_devs.values():
            device = osd_dev["device"]
            args.append('--device')
            args.append(f'{device}:{device}')


        for host in range(hosts+1): # 0 will be the seed
            options = copy.copy(args)
            options.append('--name')
            options.append(f'box_hosts_{host}')
            options.append('--network')
            options.append(f'{network_name}')
            if host == 0:
                for port in self.SEED_PORTS:
                    options.append('-p')
                    options.append(f'{port}:{port}')

            options.append('cephadm-box')
            options = ' '.join(options)

            run_shell_command(f'podman run {options}')

    def up(self, hosts: int):
        import osd
        self.setup_podman_env(hosts=hosts, osd_devs=osd.load_osd_devices())

def get_container_engine() -> ContainerEngine:
    if engine() == 'docker':
        return DockerEngine()
    else:
        return PodmanEngine()
