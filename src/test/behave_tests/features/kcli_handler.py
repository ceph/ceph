import subprocess
import os


kcli_podman = r"""
podman run --net host -it --rm --security-opt label=disable
 -v $HOME/.ssh:/root/.ssh -v $HOME/.kcli:/root/.kcli
 -v /var/lib/libvirt/images:/var/lib/libvirt/images
 -v /var/run/libvirt:/var/run/libvirt -v $PWD:/workdir
 -v /var/tmp:/ignitiondir quay.io/karmab/kcli:2543a61
"""

kcli_docker = r"""
docker run --net host -it --rm --security-opt label=disable
 -v $HOME/.ssh:/root/.ssh -v $HOME/.kcli:/root/.kcli
 -v /var/lib/libvirt/images:/var/lib/libvirt/images
 -v /var/run/libvirt:/var/run/libvirt -v $PWD:/workdir
 -v /var/tmp:/ignitiondir quay.io/karmab/kcli:2543a61
"""


def _create_kcli_cmd(context, command):
    if context.container_engine == 'docker':
        cmd = kcli_docker.replace("$HOME", os.getenv("HOME"))
    else:
        cmd = kcli_podman.replace("$HOME", os.getenv("HOME"))
    cmd = cmd.replace("$PWD", os.getenv("PWD"))
    kcli = cmd.replace("\n", "").split(" ")
    return kcli + command.split(" ")


def execute_kcli_cmd(context, command):
    """
    Executes the kcli command by combining the provided command
    with kcli executable command.
    """
    cmd = _create_kcli_cmd(context, command)
    print(f"Executing kcli command : {command}")
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            # env=dict(STORAGE_OPTS=''),
        )
    except Exception as ex:
        print(f"Error executing kcli command\n{ex}")

    return (proc.stdout, proc.stderr, proc.returncode)


def execute_ssh_cmd(context, vm_name, shell, command):
    """
    Executes the provided ssh command on the provided vm machine
    """
    if shell == "cephadm_shell":
        command = f"cephadm shell {command}"
    cmd = ('ssh -oStrictHostKeyChecking=no -oBatchMode=yes -oConnectTimeout=300 '
           f'root@{context.available_nodes[vm_name]} {command}'.split(' '))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except Exception as ex:
        print(f"Error executing ssh command: {ex}")
        return ('', str(ex), 1)

    return (proc.stdout, proc.stderr, proc.returncode)


def execute_scp_cmd(context, vm_name, script, copy_location, recursive: bool = False):
    """
    Copy a given script onto a vm
    """
    r = ' -r' if recursive else ''
    cmd = (f'scp{r} -oStrictHostKeyChecking=no -oBatchMode=yes -oConnectTimeout=300 '
           f'{script} root@{context.available_nodes[vm_name]}:{copy_location}'.split(' '))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except Exception as ex:
        print(f"Error executing scp command: {ex}")
        return ('', str(ex), 1)

    return (proc.stdout, proc.stderr, proc.returncode)


def execute_local_cmd(context, command):
    """
    Executes the provided command locally
    """
    try:
        proc = subprocess.run(command.split(' '), capture_output=True, text=True, timeout=300)
    except Exception as ex:
        print(f"Error executing command {command}: {ex}")
        return ('', str(ex), 1)

    return (proc.stdout, proc.stderr, proc.returncode)
