import errno
import logging

import rados

from ..exception import MirrorException
from ..utils import MIRROR_OBJECT_NAME

log = logging.getLogger(__name__)

def create_mirror_object(rados_inst, pool_id):
    log.info(f'creating mirror object: {MIRROR_OBJECT_NAME}')
    try:
        with rados_inst.open_ioctx2(pool_id) as ioctx:
            with rados.WriteOpCtx() as write_op:
                write_op.new(rados.LIBRADOS_CREATE_EXCLUSIVE)
                ioctx.operate_write_op(write_op, MIRROR_OBJECT_NAME)
    except rados.Error as e:
        if e.errno == errno.EEXIST:
            # be graceful
            return -e.errno
        log.error(f'failed to create mirror object: {e}')
        raise Exception(-e.args[0])
