import subprocess
import time
import os


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


def is_bootstrap_script_complete():
    """
    Checks for status of bootstrap script executions.
    """
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
    while timeout < 10:  # Totally waits for 5 mins before giving up
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if "Bootstrap complete." in proc.stdout:
            print("Bootstrap script completed successfully")
            return True
        timeout += 1
        print("Waiting for bootstrap_cluster script...")
        print(proc.stdout[len(proc.stdout) - 240:])
        time.sleep(30)
    print(
        f"Timeout reached {30*timeout}. Giving up for bootstrap to complete"
    )
    return False


def execute_kcli_cmd(command):
    """
    Executes the kcli command by combining the provided command
    with kcli executable command.
    """
    cmd = _create_kcli_cmd(command)
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

    op = proc.stderr if proc.stderr else proc.stdout
    return (op, proc.returncode)


def execute_ssh_cmd(vm_name, shell, command):
    """
    Executes the provided ssh command on the provided vm machine
    """
    if shell == "cephadm_shell":
        command = f"cephadm shell {command}"
    sudo_cmd = f"sudo -i {command}".split(" ")
    sudo_cmd = " ".join([f'"{cmd}"' for cmd in sudo_cmd])
    cmd = _create_kcli_cmd(f"ssh {vm_name} {sudo_cmd}")
    print(f"Executing ssh command : {cmd}")
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except Exception as ex:
        print(f"Error executing ssh command: {ex}")

    op = proc.stderr if proc.stderr else proc.stdout
    return (op, proc.returncode)
