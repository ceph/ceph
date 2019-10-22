from subprocess import run, PIPE, CalledProcessError, TimeoutExpired
from utils import get_hostname, find_program



class CephContainer:
    # TODO: change the way we construct this class
    # Since I moved to class a class based approach for all operations
    # i.e. Bootstrap, Version etc, we can save one parameter in the
    # constructor by pre-instantiating the the class in the respective
    # constructor with self.image
    def __init__(self,
                 image,
                 entrypoint,
                 args=[],
                 volume_mounts={},
                 cname='',
                 podman_args=[]):
        self.image = image
        self.entrypoint = entrypoint
        self.args = args
        self.volume_mounts = volume_mounts
        self.cname = cname
        self.podman_args = podman_args
        self.podman_path = find_program('podman')

    def run_cmd(self):
        vols = sum([['-v', f'{host_dir}:{container_dir}']
                    for host_dir, container_dir in self.volume_mounts.items()],
                   [])
        envs = [
            '-e',
            f'CONTAINER_IMAGE={self.image}',
            '-e',
            f'NODE_NAME={get_hostname()}',
        ]
        cname = ['--name', self.cname] if self.cname else []
        return [
            self.podman_path,
            'run',
            '--rm',
            '--net=host',
        ] + self.podman_args + cname + envs + vols + [
            '--entrypoint', self.entrypoint, self.image
        ] + self.args

    def shell_cmd(self):
        vols = sum([['-v', f'{host_dir}:{container_dir}']
                    for host_dir, container_dir in self.volume_mounts.items()],
                   [])
        envs = [
            '-e',
            f'CONTAINER_IMAGE={self.image}',
            '-e',
            f'NODE_NAME={get_hostname()}',
        ]
        return [
            self.podman_path,
            'run',
            '-it',
            '--net=host',
            '--privileged',
        ] + self.podman_args + envs + vols + [
            '--entrypoint', '/bin/bash', self.image
        ]


    def run(self):
        try:
            print(" ".join(self.run_cmd()))
            ret = run(
                self.run_cmd(),
                stdout=PIPE,
                stderr=PIPE,
                encoding='utf-8',
                # .run implements a timeout which is neat. (in seconds)
                #timeout=timeout,
                # also it implements a 'check' kwarg that raises 'CalledProcessError' when
                # the returncode is non-0
                check=True)
        except CalledProcessError as e:
            raise e
        except TimeoutExpired as e:
            raise e
        except FileNotFoundError as e:
            raise e
        return ret
