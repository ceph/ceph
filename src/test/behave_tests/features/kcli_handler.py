import subprocess
import logging

kcli_exec = r"""
podman run --net host -it --rm --security-opt label=disable
-v /root/.ssh:/root/.ssh -v /root/.kcli:/root/.kcli
-v /var/lib/libvirt/images:/var/lib/libvirt/images
-v /var/run/libvirt:/var/run/libvirt -v /root:/workdir
-v /var/tmp:/ignitiondir jolmomar/kcli
"""


def _create_kcli_cmd(command):
    kcli = kcli_exec.replace('\n', '').split(" ")
    return kcli + command.split(" ")


def execute_kcli_cmd(command):
    cmd = _create_kcli_cmd(command)
    logging.info(f"Executing cmd : {command}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise Exception(f"Failed error \n{proc.stderr} out \n{proc.stdout}")
    return proc.stdout


def exec_ssh_cmd(vm_name, command):
    if command.startswith("ceph "):
        command = f"cephadm shell {command}"
    cmd = _create_kcli_cmd(f"ssh {vm_name} sudo {command}")
    logging.info(f"Executing ssh cmd : {command}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise Exception(f"Failed error \n{proc.stderr} out \n{proc.stdout}")
    return proc.stdout
