import os

from .group import Group


def splitall(path):
    if path == "/":
        return ["/"]
    s = os.path.split(path)
    return splitall(s[0]) + [s[1]]


def resolve_group_and_subvolume_name(vol_spec, sv_path):
    parts = splitall(sv_path)
    if len(parts) != 4 or os.path.join(parts[0], parts[1]) != vol_spec.subvolume_prefix:
        return None
    groupname = None if parts[2] == Group.NO_GROUP_NAME else parts[2]
    subvolname = parts[3]
    return (groupname, subvolname)


def resolve_trash(vol_spec, path):
    parts = splitall(path)
    if len(parts) != 6 or os.path.join(parts[0], parts[1]) != vol_spec.subvolume_prefix or \
       parts[4] != '.trash':
        return None
    groupname = None if parts[2] == Group.NO_GROUP_NAME else parts[2]
    subvolname = parts[3]
    return (groupname, subvolname)
