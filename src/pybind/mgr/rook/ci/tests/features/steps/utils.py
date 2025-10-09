import subprocess
from difflib import unified_diff

ROOK_CEPH_COMMAND = "minikube kubectl -- -n rook-ceph exec -it deploy/rook-ceph-tools -- "
CLUSTER_COMMAND = "minikube kubectl -- "


def execute_command(command: str) -> str:
    output = ""
    try:
        proc = subprocess.run(command, shell=True, capture_output=True, text=True)
        output = proc.stdout
    except Exception as ex:
        output = f"Error executing command: {ex}"

    return output


def run_commands(commands: str) -> str:
    commands_list = commands.split("\n")
    output = ""
    for cmd in commands_list:
        if cmd.startswith("ceph"):
            prefix = ROOK_CEPH_COMMAND
        else:
            prefix = CLUSTER_COMMAND
        command = prefix + cmd
        output = execute_command(command)

    return output.strip("\n")

def run_k8s_commands(commands: str) -> str:
    commands_list = commands.split("\n")
    output = ""
    for cmd in commands_list:
        command = CLUSTER_COMMAND + cmd
        output = execute_command(command)

    return output.strip("\n")

def run_ceph_commands(commands: str) -> str:
    commands_list = commands.split("\n")
    output = ""
    for cmd in commands_list:
        command = ROOK_CEPH_COMMAND + cmd
        output = execute_command(command)

    return output.strip("\n")

def display_side_by_side(expected, got):
    diff = unified_diff(expected.splitlines(), got.splitlines(), lineterm='')
    print('\n'.join(diff))
