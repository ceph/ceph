from errno import *
from uuid import UUID, uuid4
from os.path import join
from logging import getLogger

from .exception import VolumeException, InvalidUuidError


log = getLogger(__name__)


def gen_uuid():
    return str(uuid4())


def validate_uuid(uuid):
    '''
    If UUID is invaid, raise InvalidUuidError.
    '''
    uuid_type = type(uuid)
    if uuid_type is str:
        pass
    elif uuid_type is bytes:
        uuid = uuid.decode('utf-8')
    else:
        raise VolumeException(EINVAL,
                              ('received invalid type for uuid, expected str '
                               f'or bytes. uuid_type={uuid_type} uuid={uuid}'))

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
