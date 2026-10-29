import os
from os.path import dirname, basename

from .group import Group


def splitall(path):
    if path == "/":
        return ["/"]
    s = os.path.split(path)
    return splitall(s[0]) + [s[1]]


def resolve_group_and_subvolume_name(vol_spec, subvol_mnt_path):
    '''
    :returns: (group name, subvol name)
    '''
    assert basename(subvol_mnt_path) == 'mnt', \
        f'subvol_mnt_path = {subvol_mnt_path}'

    subvol_base_path = dirname(dirname(dirname(subvol_mnt_path)))
    dir_names = subvol_base_path.split('/')

    assert dir_names[0] == '', \
         f'dir_names = {dir_names}'
    assert dir_names[1] == vol_spec.VOL_BASE_PATH[1:], \
         f'dir_names = {dir_names}'

    # deal with default group name
    dir_names[2] = None if dir_names[2] == Group.NO_GROUP_NAME else dir_names[2]
    return (dir_names[2], dir_names[3])


def resolve_trash(vol_spec, path):
    parts = splitall(path)
    if len(parts) != 6 or \
       os.path.join(parts[0], parts[1]) != vol_spec.vols_base_path or \
       parts[4] != '.trash':
        return None
    groupname = None if parts[2] == Group.NO_GROUP_NAME else parts[2]
    subvolname = parts[3]
    return (groupname, subvolname)
