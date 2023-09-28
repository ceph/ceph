import subprocess

KCLI_PREFIX = "kcli ssh -u root -- cephkube-ctlplane-0"
ROOK_CEPH_COMMAND = KCLI_PREFIX  + " 'kubectl rook-ceph "
CLUSTER_COMMAND = KCLI_PREFIX + " '"


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
        command = prefix + cmd + "'"
        output = execute_command(command)

    return output.strip("\n")
