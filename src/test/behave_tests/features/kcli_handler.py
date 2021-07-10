import subprocess
import logging
import os
import time


kcli_exec = r"""
podman run --net host -it --rm --security-opt label=disable
 -v $HOME/.ssh:/root/.ssh -v $HOME/.kcli:/root/.kcli
 -v /var/lib/libvirt/images:/var/lib/libvirt/images
 -v /var/run/libvirt:/var/run/libvirt -v $PWD:/workdir
 -v /var/tmp:/ignitiondir jolmomar/kcli
"""


def _create_kcli_cmd(command):
    cmd = kcli_exec.replace("$HOME", os.getenv("HOME"))
    cmd = cmd.replace("$PWD", os.getenv("PWD"))
    kcli = cmd.replace("\n", "").split(" ")
    return kcli + command.split(" ")


def check_cephadm_status():
    timeout = 0
    command = " ".join(
        [
            f'"{cmd}"' for cmd in
            "journalctl --no-tail --no-pager -t cloud-init".split(" ")
        ]
    )
    cmd = _create_kcli_cmd(
        f'ssh ceph-node-00 {command} | grep "Bootstrap complete."'
    )
    while timeout < 3:
        print("Waiting for bootstrap_cluster script...")
        time.sleep(60)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if "Bootstrap complete." in proc.stdout:
            return 0
        timeout += 1
    return -1


def execute_kcli_cmd(command):
    cmd = _create_kcli_cmd(command)
    logging.info(f"Executing cmd : {command}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout:
        return proc.stdout
    return proc.stderr


def exec_ssh_cmd(vm_name, command):
    if command.startswith("ceph "):
        command = f"cephadm shell {command}"
    sudo_cmd = f"sudo -i {command}".split(" ")
    sudo_cmd = " ".join([f'"{cmd}"' for cmd in sudo_cmd])
    cmd = _create_kcli_cmd(f"ssh {vm_name} {sudo_cmd}")
    logging.info(f"Executing ssh cmd : {cmd}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout:
        return (proc.stdout, proc.returncode)
    return (proc.stderr, proc.returncode)
