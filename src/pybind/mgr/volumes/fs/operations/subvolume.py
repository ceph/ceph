from contextlib import contextmanager

from .volume import open_volume, open_volume_lockless
from .group import open_group
from .template import SubvolumeOpType
from .versions import loaded_subvolumes

def create_subvol(mgr, fs, vol_spec, group, subvolname, size, isolate_nspace, pool, mode, uid, gid):
    """
    create a subvolume (create a subvolume with the max known version).

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param group: group object for the subvolume
    :param size: In bytes, or None for no size limit
    :param isolate_nspace: If true, use separate RADOS namespace for this subvolume
    :param pool: the RADOS pool where the data objects of the subvolumes will be stored
    :param mode: the user permissions
    :param uid: the user identifier
    :param gid: the group identifier
    :return: None
    """
    subvolume = loaded_subvolumes.get_subvolume_object_max(mgr, fs, vol_spec, group, subvolname)
    subvolume.create(size, isolate_nspace, pool, mode, uid, gid)


def create_clone(mgr, fs, vol_spec, group, subvolname, pool, source_volume, source_subvolume, snapname):
    """
    create a cloned subvolume.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param group: group object for the clone
    :param subvolname: clone subvolume nam
    :param pool: the RADOS pool where the data objects of the cloned subvolume will be stored
    :param source_volume: source subvolumes volume name
    :param source_subvolume: source (parent) subvolume object
    :param snapname: source subvolume snapshot
    :return None
    """
    subvolume = loaded_subvolumes.get_subvolume_object_max(mgr, fs, vol_spec, group, subvolname)
    subvolume.create_clone(pool, source_volume, source_subvolume, snapname)


def remove_subvol(mgr, fs, vol_spec, group, subvolname, force=False, retainsnaps=False):
    """
    remove a subvolume.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param group: group object for the subvolume
    :param subvolname: subvolume name
    :param force: force remove subvolumes
    :return: None
    """
    op_type = SubvolumeOpType.REMOVE if not force else SubvolumeOpType.REMOVE_FORCE
    with open_subvol(mgr, fs, vol_spec, group, subvolname, op_type) as subvolume:
        subvolume.remove(retainsnaps)


@contextmanager
def open_subvol(mgr, fs, vol_spec, group, subvolname, op_type):
    """
    open a subvolume. This API is to be used as a context manager.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param group: group object for the subvolume
    :param subvolname: subvolume name
    :param op_type: operation type for which subvolume is being opened
    :return: yields a subvolume object (subclass of SubvolumeTemplate)
    """
    subvolume = loaded_subvolumes.get_subvolume_object(mgr, fs, vol_spec, group, subvolname)
    subvolume.open(op_type)
    yield subvolume


@contextmanager
def open_sv_in_vol(vc, vol_spec, vol_name, grp_name, sv_name, op_type,
                   lockless=True):
    open_vol = open_volume_lockless if lockless else open_volume

    with open_vol(vc, vol_name) as fsh:
        with open_group(fsh, vol_spec, grp_name) as grp:
            with open_subvol(vc.mgr, fsh, vol_spec, grp, sv_name, op_type) as sv:
                yield fsh, grp, sv


# TODO: 99% chance that lockless=True mode will suffice for all cases of usage
# this method, check that, and if so, remove parameter lockless.
@contextmanager
def open_sv_in_grp(mgr, fsh, vol_spec, grp_name, sv_name, op_type,
                   lockless=True):
    with open_group(fsh, vol_spec, grp_name) as grp:
        with open_subvol(mgr, fsh, vol_spec, grp, sv_name, op_type) as sv:
            yield sv


# TODO: 99% chance that lockless=True mode will suffice for all cases of usage
# this method, check that, and if so, remove lockless parameter.
@contextmanager
def open_clone_sv_pair_in_vol(vc, vol_spec, vol_name, grp_name, sv_name,
                              lockless=True):
    with open_sv_in_vol(
            vc, vol_spec, vol_name, grp_name, sv_name,
            SubvolumeOpType.CLONE_INTERNAL, lockless) as (fsh, _, dst_sv):
        src_volname, src_grp_name, src_sv_name, src_snap_name = \
            dst_sv.get_clone_source()

        if grp_name == src_grp_name and sv_name == src_sv_name:
            # use the same subvolume to avoid metadata overwrites
            yield (dst_sv, dst_sv, src_snap_name)
        else:
            with open_sv_in_grp(vc.mgr, fsh, vol_spec, src_grp_name, src_sv_name,
                                SubvolumeOpType.CLONE_SOURCE) as src_sv:
                yield (dst_sv, src_sv, src_snap_name)


@contextmanager
def open_clone_sv_pair_in_grp(mgr, fsh, vol_spec, volname, grp_name, sv_name,
                              lockless=True):
    with open_sv_in_grp(mgr, fsh, vol_spec, grp_name, sv_name,
                        SubvolumeOpType.CLONE_INTERNAL, lockless) as dst_sv:
        src_volname, src_grp_name, src_sv_name, src_snap_name = \
            dst_sv.get_clone_source()

        if grp_name == src_grp_name and sv_name == src_sv_name:
            # use the same subvolume to avoid metadata overwrites
            yield (dst_sv, dst_sv, src_snap_name)
        else:
            with open_sv_in_grp(mgr, fsh, vol_spec, src_grp_name, src_sv_name,
                                SubvolumeOpType.CLONE_SOURCE) as src_sv:
                yield (dst_sv, src_sv, src_snap_name)
