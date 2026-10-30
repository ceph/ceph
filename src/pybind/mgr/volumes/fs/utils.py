from errno import EINVAL
from uuid import uuid4, UUID
from os.path import join
from logging import getLogger

from .exception import VolumeException, InvalidUuidError

from cephfs import Utf8BytesWrapper, DEFAULT_ENCODING


log = getLogger(__name__)


def gen_uuid():
    return to_utf8b(uuid4())


def verify_uuid(uuid):
    '''
    If UUID is invaid, raise InvalidUuidError.
    '''
    assert type(uuid) in (bytes, str, Utf8BytesWrapper), \
        f'invalid type. type(uuid) = {type(uuid)} uuid = {uuid}'

    try:
        UUID(to_str(uuid), version=4)
    except Exception as e:
        raise InvalidUuidError(EINVAL,
                               f'invalid uuid. uuid = {uuid}. exception that '
                               f'was raised by uuid module: {e}')


def to_utf8b(*args, bytes_encoding=DEFAULT_ENCODING):
    '''
    utf8b = UTF-8 bytes
    Convert all of args to bytes. Valid types: str, bytes, int, float and bool.

    :rtype: Utf8BytesWrapper object or list of it
    '''
    if len(args) == 1:
        args = args[0]
        if type(args) in (list, tuple):
            if not args:
                return
            return (Utf8BytesWrapper(args[0]), )
        else:
            return Utf8BytesWrapper(args)

    return [Utf8BytesWrapper(i) for i in args]


def to_str(*args):
    '''
    Convert all of args to str. Valid types: str, bytes, int, float and bool.

    :rtype: str or list of str
    '''
    retval = []
    for val in args:
        val_type = type(val)
        if val_type is str:
            retval.append(val)
        elif val_type is bytes:
            retval.append(val.decode('utf-8'))
        elif val_type in (int, float, bool, Utf8BytesWrapper):
            retval.append(str(val))
        else:
            raise VolumeException(EINVAL,
                                  f'invalid type. val_type = {val_type} '
                                  f'val = {val} args = {args}')

    return retval if len(retval) > 1 else retval[0]


def safe_join(*args) -> bytes:
    '''
    Convert members of args to bytes, pass them to os.path.join(), and return
    its return value.

    Acceptable types: str, int, float, bool, UUID, Utf8BytesWrapper.
    '''
    assert args
    args = [to_utf8b(val) for val in args]

    # if-else here is not needed besides making linters happy.
    join_args = [] if args else None

    for i, val in enumerate(args):
        if i > 0 and str(val)[0] == '/':
            raise VolumeException(EINVAL,
                                  'safe_join() received non-first arg starting '
                                  f'with "/", args = {args}')
        join_args.append(bytes(val))

    return join(*join_args)
