# file_utils.py - generic file and directory utility functions


import datetime
import logging
import os
import tempfile

from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Any, Union, Tuple, Optional, Generator, IO, List

from .constants import DEFAULT_MODE, DATEFMT
from .data_utils import dict_get_join

logger = logging.getLogger()


@contextmanager
def write_new(
    destination: Union[str, Path],
    *,
    owner: Optional[Tuple[int, int]] = None,
    perms: Optional[int] = DEFAULT_MODE,
    encoding: Optional[str] = None,
) -> Generator[IO, None, None]:
    """Write a new file in a robust manner, optionally specifying the owner,
    permissions, or encoding. This function takes care to never leave a file in
    a partially-written state due to a crash or power outage by writing to
    temporary file and then renaming that temp file over to the final
    destination once all data is written.  Note that the temporary files can be
    leaked but only for a "crash" or power outage - regular exceptions will
    clean up the temporary file.
    """
    destination = os.path.abspath(destination)
    tempname = f'{destination}.new'
    open_kwargs: Dict[str, Any] = {}
    if encoding:
        open_kwargs['encoding'] = encoding
    try:
        with open(tempname, 'w', **open_kwargs) as fh:
            yield fh
            fh.flush()
            os.fsync(fh.fileno())
            if owner is not None:
                os.fchown(fh.fileno(), *owner)
            if perms is not None:
                os.fchmod(fh.fileno(), perms)
    except Exception:
        os.unlink(tempname)
        raise
    os.rename(tempname, destination)


def populate_files(config_dir, config_files, uid, gid):
    # type: (str, Dict, int, int) -> None
    """create config files for different services"""
    for fname in config_files:
        config_file = os.path.join(config_dir, fname)
        config_content = dict_get_join(config_files, fname)
        logger.info('Write file: %s' % (config_file))
        with write_new(config_file, owner=(uid, gid), encoding='utf-8') as f:
            f.write(config_content)


def touch(
    file_path: str, uid: Optional[int] = None, gid: Optional[int] = None
) -> None:
    Path(file_path).touch()
    if uid and gid:
        os.chown(file_path, uid, gid)


def write_tmp(s, uid, gid):
    # type: (str, int, int) -> IO[str]
    tmp_f = tempfile.NamedTemporaryFile(mode='w', prefix='ceph-tmp')
    os.fchown(tmp_f.fileno(), uid, gid)
    tmp_f.write(s)
    tmp_f.flush()

    return tmp_f


def makedirs(dest: Union[Path, str], uid: int, gid: int, mode: int) -> None:
    if not os.path.exists(dest):
        os.makedirs(dest, mode=mode)
    else:
        os.chmod(dest, mode)
    os.chown(dest, uid, gid)
    os.chmod(dest, mode)  # the above is masked by umask...


def recursive_chown(path: str, uid: int, gid: int) -> None:
    for dirpath, dirnames, filenames in os.walk(path):
        os.chown(dirpath, uid, gid)
        for filename in filenames:
            os.chown(os.path.join(dirpath, filename), uid, gid)


def read_file(path_list, file_name=''):
    # type: (List[str], str) -> str
    """Returns the content of the first file found within the `path_list`

    :param path_list: list of file paths to search
    :param file_name: optional file_name to be applied to a file path
    :returns: content of the file or 'Unknown'
    """
    for path in path_list:
        if file_name:
            file_path = os.path.join(path, file_name)
        else:
            file_path = path
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                try:
                    content = f.read().decode('utf-8', 'ignore').strip()
                except OSError:
                    # sysfs may populate the file, but for devices like
                    # virtio reads can fail
                    return 'Unknown'
                else:
                    return content
    return 'Unknown'


def pathify(p):
    # type: (str) -> str
    p = os.path.expanduser(p)
    return os.path.abspath(p)


def get_file_timestamp(fn):
    # type: (str) -> Optional[str]
    try:
        mt = os.path.getmtime(fn)
        return datetime.datetime.fromtimestamp(
            mt, tz=datetime.timezone.utc
        ).strftime(DATEFMT)
    except Exception:
        return None


def make_run_dir(fsid: str, uid: int, gid: int) -> None:
    makedirs(f'/var/run/ceph/{fsid}', uid, gid, 0o770)


def unlink_file(
    path: Union[str, Path],
    missing_ok: bool = False,
    ignore_errors: bool = False,
) -> None:
    """Wrapper around unlink that can either ignore missing files or all
    errors.
    """
    try:
        Path(path).unlink()
    except FileNotFoundError:
        if not missing_ok and not ignore_errors:
            raise
    except Exception:
        if not ignore_errors:
            raise
