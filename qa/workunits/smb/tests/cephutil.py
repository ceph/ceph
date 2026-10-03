import enum
import json
import shlex
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
    def __init__(self, returncode, output, error, how=None):
        self.returncode = returncode
        self.raw_out = output
        self.raw_err = error
        self._how = how
        self._json_out = None
        self._json_err = None

    @property
    def obj(self):
        if self._json_out is not None:
            return self._json_out
        if self._how == LoadJSON.ERROR:
            return None
        self._json_out = json.loads(self.raw_out)
        return self._json_out

    @property
    def err_obj(self):
        if self._json_err is not None:
            return self._json_err
        if self._how == LoadJSON.OUTPUT:
            return None
        self._json_err = json.loads(self.raw_err)
        return self._json_err

    @classmethod
    def load(cls, how, proc):
        return cls(
            proc.returncode,
            _pstr(proc.stdout),
            _pstr(proc.stderr),
            how,
        )


def _pstr(value):
    return value.decode() if isinstance(value, bytes) else value


class ProcessError(subprocess.CalledProcessError):
    def __str__(self):
        return (
            f'ProcessError: returncode={self.returncode}; command={self.cmd};'
            f' stdout={self.stdout!r}; stderr={self.stderr!r}'
        )


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
    volumes = kwargs.pop('volumes', [])
    for v in volumes:
        cmd.extend(['-v', v])
    cmd += list(args)
    try:
        proc = subprocess.run(cmd, **kwargs)
    except subprocess.CalledProcessError as err:
        err.__class__ = ProcessError
        raise err
    if load is not LoadJSON.NONE:
        return JSONResult.load(load, proc)
    return proc


def cephadm_enter_cmd(smb_cfg, cluster_id, args, **kwargs):
    """Run a command inside the primary smbd container for the given
    cluster_id on the cluster's admin node (derived via smb_cfg).
    All kwargs are treated as arguments to subprocess.run.
    """
    remote_cmd = [
        'sudo',
        f'/home/{smb_cfg.ssh_user}/cephtest/cephadm',
        'enter',
        '-i',
        f'smb.{cluster_id}',
    ] + list(args)
    cmd = [
        'ssh',
        '-oBatchMode=yes',
        '-oUserKnownHostsFile=/dev/null',
        '-oStrictHostKeyChecking=no',
        '-q',
        f'{smb_cfg.ssh_user}@{smb_cfg.ssh_admin_host}',
        shlex.join(remote_cmd),
    ]
    return subprocess.run(cmd, **kwargs)
