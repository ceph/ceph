import json
import os
import subprocess
import sys
import enum
from typing import Any, Callable, Dict

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

def run_shell_command(command: str, expect_error=False, verbose=True) -> str:
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

    if process.returncode != 0 and not expect_error:
        err = colored(err, Colors.FAIL);
        raise RuntimeError(f'Failed command: {command}\n{err}')
        sys.exit(1)
    return out


class BoxType(enum.IntEnum):
    SEED = 0 # where we bootstrap cephadm
    HOST = 1
    @staticmethod
    def to_enum(value: str):
        if value == 'seed':
            return BoxType.SEED
        elif value == 'host':
            return BoxType.HOST
        else:
            print(f'Wrong container type {value}')
            sys.exit(1)

    @staticmethod
    def to_string(box_type):
        if box_type == BoxType.SEED:
            return 'seed'
        elif box_type == BoxType.HOST:
            return 'host'
        else:
            print(f'Wrong container type {type_}')
            sys.exit(1)


def run_dc_shell_commands(index, box_type: BoxType, commands: str, expect_error=False) -> str:
    for command in commands.split('\n'):
        command = command.strip()
        if not command:
            continue
        run_dc_shell_command(command.strip(), index, box_type, expect_error=expect_error)

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

    with_cephadm_image = 'CEPHADM_IMAGE=quay.ceph.io/ceph-ci/ceph:main'
    out = run_shell_command(
        f'{with_cephadm_image} cephadm --verbose shell --config {config} --keyring {keyring} -- {command}',
        expect_error,
    )
    return out


def run_dc_shell_command(
    command: str, index: int, box_type: BoxType, expect_error=False
) -> str:
    box_type_str = 'box_hosts'
    if box_type == BoxType.SEED:
        index = 0
        if engine() == 'docker':
            box_type_str = 'seed'
            index = 1

    container_id = get_container_id(f'{box_type_str}_{index}')
    print(container_id)
    out = run_shell_command(
        f'{engine()} exec -it {container_id} {command}', expect_error
    )
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
        orch_host_ls_out = run_dc_shell_command('cephadm shell --keyring /etc/ceph/ceph.keyring --config /etc/ceph/ceph.conf -- ceph orch host ls --format json', 1, get_seed_name())
        sp = orch_host_ls_out.split('\n')
        orch_host_ls_out  = sp[len(sp) - 1]
        print('xd', orch_host_ls_out)
    hosts = json.loads(orch_host_ls_out)
    return hosts
