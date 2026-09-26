from errno import *
from uuid import UUID, uuid4
from os.path import join
from logging import getLogger

from .exception import VolumeException, InvalidUuidError


log = getLogger(__name__)


def gen_uuid():
    # TODO: remove
    return b'4f50c332-30a6-4871-b69d-9edd2ea529c0'
    return to_bytes(str(uuid4()))


def validate_uuid(uuid):
    '''
    If UUID is invaid, raise InvalidUuidError.
    '''
    uuid = to_str(uuid)

    try:
        UUID(uuid, version=4)
    except Exception as e:
        raise InvalidUuidError(EINVAL,
                                   (f'received invalid uuid. uuid = {uuid}. '
                                    f'exception raised by uuid module: {e}'))


def to_bytes(*args):
    '''
    Convert all of args to bytes. Valid types: str, bytes, int, float and bool.

    :rtype: bytes or list of bytes
    '''
    newargs = []
    for var in args:

        var_type = type(var)
        if var_type is bytes:
            newargs.append(var)
        elif var_type is str:
            newargs.append(var.encode('utf-8'))
        elif var_type in (int, float, bool):
            newargs.append(str(var).encode('utf-8'))
        else:
            raise VolumeException(EINVAL,
                                  f'invalid type. {var_type} = {var_type} '
                                  f'var = {var} args = {args}')

    return newargs if len(newargs) > 1 else newargs[0]


def to_str(*args):
    '''
    Convert all of args to str. Valid types: str, bytes, int, float and bool.

    :rtype: str or list of str
    '''
    newargs = []
    for var in args:
        var_type = type(var)

        if var_type is str:
            newargs.append(var)
        elif var_type is bytes:
            newargs.append(var.decode('utf-8'))
        elif var_type in (int, float, bool):
            newargs.append(str(var))
        else:
            raise VolumeException(EINVAL,
                                  f'invalid type. var_type = {var_type} '
                                  f'var = {var} args = {args}')

    return newargs if len(newargs) > 1 else newargs[0]


def safe_join(*args):
    '''
    Convert members of args to bytes before passing them to os.path.join() and
    and return its return value. Excepatable types: str, int, float, bool.

    :rtype: bytes
    '''
    newargs = to_bytes(*args)
    for index, var in enumerate(newargs):
        if index > 1 and var[0] == '/':
            raise VolumeException(EINVAL,
                                  ('safe_join() received non-first arg starting '
                                   'with "/"'))

    return join(*newargs)
