from contextlib import contextmanager

from .volume import open_volume, open_volume_lockless
from .group import open_group
from .template import SubvolumeOpType
from .versions import loaded_subvolumes

def create_subvol(mgr, fs, vol_spec, group, subvolname, size, isolate_nspace, pool, mode, uid, gid, earmark, normalization, casesensitive):
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
    :param earmark: metadata string to identify if subvolume is associated with nfs/smb
    :param normalization: the unicode normalization form to use (nfd, nfc, nfkd or nfkc)
    :param casesensitive: whether to make the subvolume case insensitive or not
    :return: None
    """
    subvolume = loaded_subvolumes.get_subvolume_object_max(mgr, fs, vol_spec, group, subvolname)
    subvolume.create(size, isolate_nspace, pool, mode, uid, gid, earmark, normalization, casesensitive)


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
def open_subvol_in_vol(vc, vol_spec, vol_name, group_name, subvol_name,
                       op_type, lockless=False):
    open_vol = open_volume_lockless if lockless else open_volume

    with open_vol(vc, vol_name) as vol_handle:
        with open_group(vol_handle, vol_spec, group_name) as group:
            with open_subvol(vc.mgr, vol_handle, vol_spec, group, subvol_name,
                             op_type) as subvol:
                yield vol_handle, group, subvol


@contextmanager
def open_subvol_in_group(mgr, vol_handle, vol_spec, group_name, subvol_name,
                         op_type, lockless=False):
    with open_group(vol_handle, vol_spec, group_name) as group:
        with open_subvol(mgr, vol_handle, vol_spec, group, subvol_name,
                         op_type) as subvol:
            yield subvol


@contextmanager
def open_clone_subvol_pair_in_vol(vc, vol_spec, vol_name, group_name,
                                  subvol_name, lockless=False):
    with open_subvol_in_vol(vc, vol_spec, vol_name, group_name, subvol_name,
                            SubvolumeOpType.CLONE_INTERNAL, lockless) \
                            as (vol_handle, _, dst_subvol):
        src_volname, src_group_name, src_subvol_name, src_snap_name = \
            dst_subvol.get_clone_source()

        if group_name == src_group_name and subvol_name == src_subvol_name:
            # use the same subvolume to avoid metadata overwrites
            yield (dst_subvol, dst_subvol, src_snap_name)
        else:
            with open_subvol_in_group(vc.mgr, vol_handle, vol_spec,
                                      src_group_name, src_subvol_name,
                                      SubvolumeOpType.CLONE_SOURCE) \
                                      as src_subvol:
                yield (dst_subvol, src_subvol, src_snap_name)


@contextmanager
def open_clone_subvol_pair_in_group(mgr, vol_handle, vol_spec, volname,
                                    group_name, subvol_name, lockless=False):
    with open_subvol_in_group(mgr, vol_handle, vol_spec, group_name,
                              subvol_name, SubvolumeOpType.CLONE_INTERNAL,
                              lockless) as dst_subvol:
        src_volname, src_group_name, src_subvol_name, src_snap_name = \
            dst_subvol.get_clone_source()

        if group_name == src_group_name and subvol_name == src_subvol_name:
            # use the same subvolume to avoid metadata overwrites
            yield (dst_subvol, dst_subvol, src_snap_name)
        else:
            with open_subvol_in_group(mgr, vol_handle, vol_spec,
                                      src_group_name, src_subvol_name,
                                      SubvolumeOpType.CLONE_SOURCE) \
                                      as src_subvol:
                yield (dst_subvol, src_subvol, src_snap_name)
