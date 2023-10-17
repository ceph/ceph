import subprocess

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
