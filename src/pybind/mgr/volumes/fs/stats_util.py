'''
This module contains classes, methods & helpers that are used to get statistics
(specifically number of files and total size of data present under the source
and destination directory for the copy operation that is performed for snapshot
cloning) and pass, print, log and convert them to human readable format
conveniently.
'''
from typing import Optional

from mgr_util import format_bytes, format_dimless


def get_size_ratio_str(size1, size2):
    size1, size2 = format_bytes(size1, 4), format_bytes(size2, 4)

    size_string =  f'{size1}/{size2}'
    size_string = size_string.replace(' ', '')
    return size_string


def get_num_ratio_str(num1, num2):
    num1, num2 = format_dimless(num1, 4), format_dimless(num2, 4)

    num_string = f'{num1}/{num2}'
    num_string = num_string.replace(' ', '')
    return num_string


def get_amount_copied(src_path, dst_path, fs_handle):
    rbytes = 'ceph.dir.rbytes'

    size_t = int(fs_handle.getxattr(src_path, rbytes))
    size_c = int(fs_handle.getxattr(dst_path, rbytes))

    percent: Optional[float]
    if size_t == 0 or size_c == 0:
        percent = 0
    else:
        percent = ((size_c/size_t) * 100)
        percent = round(percent, 3)

    return size_t, size_c, percent


def get_stats(src_path, dst_path, fs_handle):
    rentries = 'ceph.dir.rentries'
    rentries_t = int(fs_handle.getxattr(src_path, rentries))
    rentries_c = int(fs_handle.getxattr(dst_path, rentries))

    size_t, size_c, percent = get_amount_copied(src_path, dst_path, fs_handle)

    return {
        'percentage cloned': percent,
        'amount cloned': get_size_ratio_str(size_c, size_t),
        'files cloned': get_num_ratio_str(rentries_c, rentries_t),
    }
