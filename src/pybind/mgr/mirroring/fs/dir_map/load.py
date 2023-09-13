import errno
import pickle
import logging
from typing import Dict

import rados

from ..exception import MirrorException
from ..utils import MIRROR_OBJECT_NAME, DIRECTORY_MAP_PREFIX, \
    INSTANCE_ID_PREFIX

log = logging.getLogger(__name__)

MAX_RETURN = 256

def handle_dir_load(dir_mapping, dir_map):
    for directory_str, encoded_map in dir_map.items():
        dir_path = directory_str[len(DIRECTORY_MAP_PREFIX):]
        decoded_map = pickle.loads(encoded_map)
        log.debug(f'{dir_path} -> {decoded_map}')
        dir_mapping[dir_path] = decoded_map

def load_dir_map(ioctx):
    dir_mapping = {} # type: Dict[str, Dict]
    log.info('loading dir map...')
    try:
        with rados.ReadOpCtx() as read_op:
            start = ""
            while True:
                iter, ret = ioctx.get_omap_vals(read_op, start, DIRECTORY_MAP_PREFIX, MAX_RETURN)
                if not ret == 0:
                    log.error(f'failed to fetch dir mapping omap')
                    raise Exception(-errno.EINVAL)
                ioctx.operate_read_op(read_op, MIRROR_OBJECT_NAME)
                dir_map = dict(iter)
                if not dir_map:
                    break
                handle_dir_load(dir_mapping, dir_map)
                start = dir_map.popitem()[0]
        log.info("loaded {0} directory mapping(s) from disk".format(len(dir_mapping)))
        return dir_mapping
    except rados.Error as e:
        log.error(f'exception when loading directory mapping: {e}')
        raise Exception(-e.errno)

def handle_instance_load(instance_mapping, instance_map):
    for instance, e_data in instance_map.items():
        instance_id = instance[len(INSTANCE_ID_PREFIX):]
        d_data = pickle.loads(e_data)
        log.debug(f'{instance_id} -> {d_data}')
        instance_mapping[instance_id] = d_data

def load_instances(ioctx):
    instance_mapping = {} # type: Dict[str, Dict]
    log.info('loading instances...')
    try:
        with rados.ReadOpCtx() as read_op:
            start = ""
            while True:
                iter, ret = ioctx.get_omap_vals(read_op, start, INSTANCE_ID_PREFIX, MAX_RETURN)
                if not ret == 0:
                    log.error(f'failed to fetch instance omap')
                    raise Exception(-errno.EINVAL)
                ioctx.operate_read_op(read_op, MIRROR_OBJECT_NAME)
                instance_map = dict(iter)
                if not instance_map:
                    break
                handle_instance_load(instance_mapping, instance_map)
                start = instance_map.popitem()[0]
        log.info("loaded {0} instance(s) from disk".format(len(instance_mapping)))
        return instance_mapping
    except rados.Error as e:
        log.error(f'exception when loading instances: {e}')
        raise Exception(-e.errno)
