import enum
import json
import subprocess


class LoadJSON(enum.Enum):
    NONE = 0
    OUTPUT = 1
    ERROR = 2
    BOTH = 3

    @classmethod
    def convert(cls, value):
        if isinstance(value, LoadJSON):
            return value
        if value:
            return cls.OUTPUT
        return cls.NONE


class JSONResult:
    def __init__(self, returncode, output, error):
        self.returncode = returncode
        self.raw_out = output
        self.out = None if output is None else json.loads(output)
        self.raw_err = error

    @property
    def obj(self):
        return self.out


def cephadm_shell_cmd(
    smb_cfg, args, load_json=None, input_json=None, **kwargs
):
    """Run a command within the cephadm shell on the cluster's admin
    node (derived via smb_cfg). If `load_json` is true return the stdout
    loaded into a json object (implies check and capture_output).
    All kwargs are treated as arguments to subprocess.run.
    """
    load = LoadJSON.convert(load_json)
    if load is not LoadJSON.NONE:
        kwargs['capture_output'] = True
        kwargs['check'] = load is LoadJSON.OUTPUT
    if input_json is not None:
        kwargs['input'] = json.dumps(input_json).encode()
    cmd = [
        'ssh',
        '-oBatchMode=yes',
        '-oUserKnownHostsFile=/dev/null',
        '-oStrictHostKeyChecking=no',
        '-q',
        f'{smb_cfg.ssh_user}@{smb_cfg.ssh_admin_host}',
        'sudo',
        f'/home/{smb_cfg.ssh_user}/cephtest/cephadm',
        'shell',
    ]
    cmd += list(args)
    proc = subprocess.run(cmd, **kwargs)
    if load is LoadJSON.BOTH:
        return JSONResult(
            proc.returncode, proc.stdout.decode(), proc.stderr.decode()
        )
    elif load is LoadJSON.OUTPUT:
        return JSONResult(proc.returncode, proc.stdout.decode(), None)
    elif load is LoadJSON.ERROR:
        return JSONResult(proc.returncode, None, proc.stderr.decode())
    return proc
