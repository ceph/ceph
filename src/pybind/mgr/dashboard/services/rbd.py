# -*- coding: utf-8 -*-
# pylint: disable=unused-argument
import errno
import json
import math
from enum import IntEnum

import cherrypy
import rados
import rbd

from .. import mgr
from ..exceptions import DashboardException
from ..plugins.ttl_cache import ttl_cache, ttl_cache_invalidator
from ._paginate import ListPaginator
from .ceph_service import CephService

try:
    from typing import List, Optional
except ImportError:
    pass  # For typing only


RBD_FEATURES_NAME_MAPPING = {
    rbd.RBD_FEATURE_LAYERING: "layering",
    rbd.RBD_FEATURE_STRIPINGV2: "striping",
    rbd.RBD_FEATURE_EXCLUSIVE_LOCK: "exclusive-lock",
    rbd.RBD_FEATURE_OBJECT_MAP: "object-map",
    rbd.RBD_FEATURE_FAST_DIFF: "fast-diff",
    rbd.RBD_FEATURE_DEEP_FLATTEN: "deep-flatten",
    rbd.RBD_FEATURE_JOURNALING: "journaling",
    rbd.RBD_FEATURE_DATA_POOL: "data-pool",
    rbd.RBD_FEATURE_OPERATIONS: "operations",
}

RBD_IMAGE_REFS_CACHE_REFERENCE = 'rbd_image_refs'
GET_IOCTX_CACHE = 'get_ioctx'
POOL_NAMESPACES_CACHE = 'pool_namespaces'


class MIRROR_IMAGE_MODE(IntEnum):
    journal = rbd.RBD_MIRROR_IMAGE_MODE_JOURNAL
    snapshot = rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT


def _rbd_support_remote(method_name: str, *args, **kwargs):
    try:
        return mgr.remote('rbd_support', method_name, *args, **kwargs)
    except ImportError as ie:
        raise DashboardException(f'rbd_support module not found {ie}')
    except RuntimeError as ie:
        raise DashboardException(f'rbd_support.{method_name} error: {ie}')


def format_bitmask(features):
    """
    Formats the bitmask:

    @DISABLEDOCTEST: >>> format_bitmask(45)
    ['deep-flatten', 'exclusive-lock', 'layering', 'object-map']
    """
    names = [val for key, val in RBD_FEATURES_NAME_MAPPING.items()
             if key & features == key]
    return sorted(names)


def format_features(features):
    """
    Converts the features list to bitmask:

    @DISABLEDOCTEST: >>> format_features(['deep-flatten', 'exclusive-lock',
        'layering', 'object-map'])
    45

    @DISABLEDOCTEST: >>> format_features(None) is None
    True

    @DISABLEDOCTEST: >>> format_features('deep-flatten, exclusive-lock')
    32
    """
    if isinstance(features, str):
        features = features.split(',')

    if not isinstance(features, list):
        return None

    res = 0
    for key, value in RBD_FEATURES_NAME_MAPPING.items():
        if value in features:
            res = key | res
    return res


def _sort_features(features, enable=True):
    """
    Sorts image features according to feature dependencies:

    object-map depends on exclusive-lock
    journaling depends on exclusive-lock
    fast-diff depends on object-map
    """
    ORDER = ['exclusive-lock', 'journaling', 'object-map', 'fast-diff']  # noqa: N806

    def key_func(feat):
        try:
            return ORDER.index(feat)
        except ValueError:
            return id(feat)

    features.sort(key=key_func, reverse=not enable)


def get_image_spec(pool_name, namespace, rbd_name):
    namespace = '{}/'.format(namespace) if namespace else ''
    return '{}/{}{}'.format(pool_name, namespace, rbd_name)


def parse_image_spec(image_spec):
    namespace_spec, image_name = image_spec.rsplit('/', 1)
    if '/' in namespace_spec:
        pool_name, namespace = namespace_spec.rsplit('/', 1)
    else:
        pool_name, namespace = namespace_spec, None
    return pool_name, namespace, image_name


def rbd_call(pool_name, namespace, func, *args, **kwargs):
    with mgr.rados.open_ioctx(pool_name) as ioctx:
        ioctx.set_namespace(namespace if namespace is not None else '')
        return func(ioctx, *args, **kwargs)


def rbd_image_call(pool_name, namespace, image_name, func, *args, **kwargs):
    def _ioctx_func(ioctx, image_name, func, *args, **kwargs):
        with rbd.Image(ioctx, image_name) as img:
            return func(ioctx, img, *args, **kwargs)

    return rbd_call(pool_name, namespace, _ioctx_func, image_name, func, *args, **kwargs)


class RbdConfiguration(object):
    _rbd = rbd.RBD()

    def __init__(self, pool_name: str = '', namespace: str = '', image_name: str = '',
                 pool_ioctx: Optional[rados.Ioctx] = None, image_ioctx: Optional[rbd.Image] = None):
        assert bool(pool_name) != bool(pool_ioctx)  # xor
        self._pool_name = pool_name
        self._namespace = namespace if namespace is not None else ''
        self._image_name = image_name
        self._pool_ioctx = pool_ioctx
        self._image_ioctx = image_ioctx

    @staticmethod
    def _ensure_prefix(option):
        # type: (str) -> str
        return option if option.startswith('conf_') else 'conf_' + option

    def list(self):
        # type: () -> List[dict]
        def _list(ioctx):
            if self._image_name:  # image config
                try:
                    # No need to open the context of the image again
                    # if we already did open it.
                    if self._image_ioctx:
                        result = self._image_ioctx.config_list()
                    else:
                        with rbd.Image(ioctx, self._image_name) as image:
                            result = image.config_list()
                except rbd.ImageNotFound:
                    result = []
            else:  # pool config
                pg_status = list(CephService.get_pool_pg_status(self._pool_name).keys())
                if len(pg_status) == 1 and 'incomplete' in pg_status[0]:
                    # If config_list would be called with ioctx if it's a bad pool,
                    # the dashboard would stop working, waiting for the response
                    # that would not happen.
                    #
                    # This is only a workaround for https://tracker.ceph.com/issues/43771 which
                    # already got rejected as not worth the effort.
                    #
                    # Are more complete workaround for the dashboard will be implemented with
                    # https://tracker.ceph.com/issues/44224
                    #
                    # @TODO: If #44224 is addressed remove this workaround
                    return []
                result = self._rbd.config_list(ioctx)
            return list(result)

        if self._pool_name:
            ioctx = mgr.rados.open_ioctx(self._pool_name)
            ioctx.set_namespace(self._namespace)
        else:
            ioctx = self._pool_ioctx

        return _list(ioctx)

    def get(self, option_name):
        # type: (str) -> str
        option_name = self._ensure_prefix(option_name)
        with mgr.rados.open_ioctx(self._pool_name) as pool_ioctx:
            pool_ioctx.set_namespace(self._namespace)
            if self._image_name:
                with rbd.Image(pool_ioctx, self._image_name) as image:
                    return image.metadata_get(option_name)
            return self._rbd.pool_metadata_get(pool_ioctx, option_name)

    def set(self, option_name, option_value):
        # type: (str, str) -> None

        option_value = str(option_value)
        option_name = self._ensure_prefix(option_name)

        pool_ioctx = self._pool_ioctx
        if self._pool_name:  # open ioctx
            pool_ioctx = mgr.rados.open_ioctx(self._pool_name)
            pool_ioctx.__enter__()  # type: ignore
            pool_ioctx.set_namespace(self._namespace)  # type: ignore

        image_ioctx = self._image_ioctx
        if self._image_name:
            image_ioctx = rbd.Image(pool_ioctx, self._image_name)
            image_ioctx.__enter__()  # type: ignore

        if image_ioctx:
            image_ioctx.metadata_set(option_name, option_value)  # type: ignore
        else:
            self._rbd.pool_metadata_set(pool_ioctx, option_name, option_value)

        if self._image_name:  # Name provided, so we opened it and now have to close it
            image_ioctx.__exit__(None, None, None)  # type: ignore
        if self._pool_name:
            pool_ioctx.__exit__(None, None, None)  # type: ignore

    def remove(self, option_name):
        """
        Removes an option by name. Will not raise an error, if the option hasn't been found.
        :type option_name str
        """
        def _remove(ioctx):
            try:
                if self._image_name:
                    with rbd.Image(ioctx, self._image_name) as image:
                        image.metadata_remove(option_name)
                else:
                    self._rbd.pool_metadata_remove(ioctx, option_name)
            except KeyError:
                pass

        option_name = self._ensure_prefix(option_name)

        if self._pool_name:
            with mgr.rados.open_ioctx(self._pool_name) as pool_ioctx:
                pool_ioctx.set_namespace(self._namespace)
                _remove(pool_ioctx)
        else:
            _remove(self._pool_ioctx)

    def set_configuration(self, configuration):
        if configuration:
            for option_name, option_value in configuration.items():
                if option_value is not None:
                    self.set(option_name, option_value)
                else:
                    self.remove(option_name)


class RbdService(object):
    _rbd_inst = rbd.RBD()

    # set of image features that can be enable on existing images
    ALLOW_ENABLE_FEATURES = {"exclusive-lock", "object-map", "fast-diff", "journaling"}

    # set of image features that can be disabled on existing images
    ALLOW_DISABLE_FEATURES = {"exclusive-lock", "object-map", "fast-diff", "deep-flatten",
                              "journaling"}

    @classmethod
    def _rbd_disk_usage(cls, image, snaps, whole_object=True):
        class DUCallback(object):
            def __init__(self):
                self.used_size = 0

            def __call__(self, offset, length, exists):
                if exists:
                    self.used_size += length

        snap_map = {}
        prev_snap = None
        total_used_size = 0
        for _, size, name in snaps:
            image.set_snap(name)
            du_callb = DUCallback()
            image.diff_iterate(0, size, prev_snap, du_callb,
                               whole_object=whole_object)
            snap_map[name] = du_callb.used_size
            total_used_size += du_callb.used_size
            prev_snap = name

        return total_used_size, snap_map

    @classmethod
    def _rbd_image(cls, ioctx, pool_name, namespace, image_name,  # pylint: disable=R0912
                   omit_usage=False):
        with rbd.Image(ioctx, image_name) as img:
            stat = img.stat()
            mirror_info = img.mirror_image_get_info()
            mirror_mode = img.mirror_image_get_mode()
            if mirror_mode == rbd.RBD_MIRROR_IMAGE_MODE_JOURNAL and mirror_info['state'] != rbd.RBD_MIRROR_IMAGE_DISABLED:  # noqa E501 #pylint: disable=line-too-long
                stat['mirror_mode'] = 'journal'
            elif mirror_mode == rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
                stat['mirror_mode'] = 'snapshot'
                schedule_status = json.loads(_rbd_support_remote(
                    'mirror_snapshot_schedule_status')[1])
                for scheduled_image in schedule_status['scheduled_images']:
                    if scheduled_image['image'] == get_image_spec(pool_name, namespace, image_name):
                        stat['schedule_info'] = scheduled_image
            else:
                stat['mirror_mode'] = 'Disabled'

            stat['name'] = image_name

            stat['primary'] = None
            if mirror_info['state'] == rbd.RBD_MIRROR_IMAGE_ENABLED:
                stat['primary'] = mirror_info['primary']

            if img.old_format():
                stat['unique_id'] = get_image_spec(pool_name, namespace, stat['block_name_prefix'])
                stat['id'] = stat['unique_id']
                stat['image_format'] = 1
            else:
                stat['unique_id'] = get_image_spec(pool_name, namespace, img.id())
                stat['id'] = img.id()
                stat['image_format'] = 2

            stat['pool_name'] = pool_name
            stat['namespace'] = namespace
            features = img.features()
            stat['features'] = features
            stat['features_name'] = format_bitmask(features)

            # the following keys are deprecated
            del stat['parent_pool']
            del stat['parent_name']

            stat['timestamp'] = "{}Z".format(img.create_timestamp()
                                             .isoformat())

            stat['stripe_count'] = img.stripe_count()
            stat['stripe_unit'] = img.stripe_unit()

            data_pool_name = CephService.get_pool_name_from_id(
                img.data_pool_id())
            if data_pool_name == pool_name:
                data_pool_name = None
            stat['data_pool'] = data_pool_name

            stat['parent'] = cls._rbd_image_stat_parent(img)

            # snapshots
            stat['snapshots'] = []
            for snap in img.list_snaps():
                try:
                    snap['mirror_mode'] = MIRROR_IMAGE_MODE(img.mirror_image_get_mode()).name
                except ValueError as ex:
                    raise DashboardException(f'Unknown RBD Mirror mode: {ex}')

                snap['timestamp'] = "{}Z".format(
                    img.get_snap_timestamp(snap['id']).isoformat())

                snap['is_protected'] = None
                if mirror_mode != rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
                    snap['is_protected'] = img.is_protected_snap(snap['name'])
                snap['used_bytes'] = None
                snap['children'] = []

                if mirror_mode != rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
                    img.set_snap(snap['name'])
                    for child_pool_name, child_image_name in img.list_children():
                        snap['children'].append({
                            'pool_name': child_pool_name,
                            'image_name': child_image_name
                        })
                stat['snapshots'].append(snap)

            # disk usage
            img_flags = img.flags()
            if not omit_usage and 'fast-diff' in stat['features_name'] and \
                    not rbd.RBD_FLAG_FAST_DIFF_INVALID & img_flags and \
                    mirror_mode != rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
                snaps = [(s['id'], s['size'], s['name'])
                         for s in stat['snapshots']]
                snaps.sort(key=lambda s: s[0])
                snaps += [(snaps[-1][0] + 1 if snaps else 0, stat['size'], None)]
                total_prov_bytes, snaps_prov_bytes = cls._rbd_disk_usage(
                    img, snaps, True)
                stat['total_disk_usage'] = total_prov_bytes
                for snap, prov_bytes in snaps_prov_bytes.items():
                    if snap is None:
                        stat['disk_usage'] = prov_bytes
                        continue
                    for ss in stat['snapshots']:
                        if ss['name'] == snap:
                            ss['disk_usage'] = prov_bytes
                            break
            else:
                stat['total_disk_usage'] = None
                stat['disk_usage'] = None

            stat['configuration'] = RbdConfiguration(
                pool_ioctx=ioctx, image_name=image_name, image_ioctx=img).list()

            stat['metadata'] = RbdImageMetadataService(img).list()

            return stat

    @classmethod
    def _rbd_image_stat_parent(cls, img):
        stat_parent = None
        try:
            stat_parent = img.get_parent_image_spec()
        except rbd.ImageNotFound:
            # no parent image
            stat_parent = None
        return stat_parent

    @classmethod
    @ttl_cache(10, label=GET_IOCTX_CACHE)
    def get_ioctx(cls, pool_name, namespace=''):
        ioctx = mgr.rados.open_ioctx(pool_name)
        ioctx.set_namespace(namespace)
        return ioctx

    @classmethod
    @ttl_cache(30, label=RBD_IMAGE_REFS_CACHE_REFERENCE)
    def _rbd_image_refs(cls, pool_name, namespace=''):
        # We add and set the namespace here so that we cache by ioctx and namespace.
        images = []
        ioctx = cls.get_ioctx(pool_name, namespace)
        images = cls._rbd_inst.list2(ioctx)
        return images

    @classmethod
    @ttl_cache(30, label=POOL_NAMESPACES_CACHE)
    def _pool_namespaces(cls, pool_name, namespace=None):
        namespaces = []
        if namespace:
            namespaces = [namespace]
        else:
            ioctx = cls.get_ioctx(pool_name, namespace=rados.LIBRADOS_ALL_NSPACES)
            namespaces = cls._rbd_inst.namespace_list(ioctx)
            # images without namespace
            namespaces.append('')
        return namespaces

    @classmethod
    def _rbd_image_stat(cls, ioctx, pool_name, namespace, image_name):
        return cls._rbd_image(ioctx, pool_name, namespace, image_name)

    @classmethod
    def _rbd_image_stat_removing(cls, ioctx, pool_name, namespace, image_id):
        img = cls._rbd_inst.trash_get(ioctx, image_id)
        img_spec = get_image_spec(pool_name, namespace, image_id)

        if img['source'] == 'REMOVING':
            img['unique_id'] = img_spec
            img['pool_name'] = pool_name
            img['namespace'] = namespace
            img['deletion_time'] = "{}Z".format(img['deletion_time'].isoformat())
            img['deferment_end_time'] = "{}Z".format(img['deferment_end_time'].isoformat())
            return img
        raise rbd.ImageNotFound('No image {} in status `REMOVING` found.'.format(img_spec),
                                errno=errno.ENOENT)

    @classmethod
    def _rbd_pool_image_refs(cls, pool_names: List[str], namespace: Optional[str] = None):
        joint_refs = []
        for pool in pool_names:
            for current_namespace in cls._pool_namespaces(pool, namespace=namespace):
                image_refs = cls._rbd_image_refs(pool, current_namespace)
                for image in image_refs:
                    image['namespace'] = current_namespace
                    image['pool_name'] = pool
                    joint_refs.append(image)
        return joint_refs

    @classmethod
    def rbd_pool_list(cls, pool_names: List[str], namespace: Optional[str] = None, offset: int = 0,
                      limit: int = 5, search: str = '', sort: str = ''):
        image_refs = cls._rbd_pool_image_refs(pool_names, namespace)
        params = ['name', 'pool_name', 'namespace']
        paginator = ListPaginator(offset, limit, sort, search, image_refs,
                                  searchable_params=params, sortable_params=params,
                                  default_sort='+name')

        result = []
        for image_ref in paginator.list():
            with mgr.rados.open_ioctx(image_ref['pool_name']) as ioctx:
                ioctx.set_namespace(image_ref['namespace'])
                # Check if the RBD has been deleted partially. This happens for example if
                # the deletion process of the RBD has been started and was interrupted.

                try:
                    stat = cls._rbd_image_stat(
                        ioctx, image_ref['pool_name'], image_ref['namespace'], image_ref['name'])
                except rbd.ImageNotFound:
                    try:
                        stat = cls._rbd_image_stat_removing(
                            ioctx, image_ref['pool_name'], image_ref['namespace'], image_ref['id'])
                    except rbd.ImageNotFound:
                        continue
                result.append(stat)
        return result, paginator.get_count()

    @classmethod
    def get_image(cls, image_spec, omit_usage=False):
        pool_name, namespace, image_name = parse_image_spec(image_spec)
        ioctx = mgr.rados.open_ioctx(pool_name)
        if namespace:
            ioctx.set_namespace(namespace)
        try:
            return cls._rbd_image(ioctx, pool_name, namespace, image_name, omit_usage)
        except rbd.ImageNotFound:
            raise cherrypy.HTTPError(404, 'Image not found')

    @classmethod
    @ttl_cache_invalidator(RBD_IMAGE_REFS_CACHE_REFERENCE)
    def create(cls, name, pool_name, size, namespace=None,
               obj_size=None, features=None, stripe_unit=None, stripe_count=None,
               data_pool=None, configuration=None, metadata=None):
        size = int(size)

        def _create(ioctx):
            rbd_inst = cls._rbd_inst

            # Set order
            l_order = None
            if obj_size and obj_size > 0:
                l_order = int(round(math.log(float(obj_size), 2)))

            # Set features
            feature_bitmask = format_features(features)

            rbd_inst.create(ioctx, name, size, order=l_order, old_format=False,
                            features=feature_bitmask, stripe_unit=stripe_unit,
                            stripe_count=stripe_count, data_pool=data_pool)
            RbdConfiguration(pool_ioctx=ioctx, namespace=namespace,
                             image_name=name).set_configuration(configuration)
            if metadata:
                with rbd.Image(ioctx, name) as image:
                    RbdImageMetadataService(image).set_metadata(metadata)
        rbd_call(pool_name, namespace, _create)

    @classmethod
    @ttl_cache_invalidator(RBD_IMAGE_REFS_CACHE_REFERENCE)
    def set(cls, image_spec, name=None, size=None, features=None,
            configuration=None, metadata=None, enable_mirror=None, primary=None,
            force=False, resync=False, mirror_mode=None, schedule_interval='',
            remove_scheduling=False):
        # pylint: disable=too-many-branches
        pool_name, namespace, image_name = parse_image_spec(image_spec)

        def _edit(ioctx, image):
            rbd_inst = cls._rbd_inst
            # check rename image
            if name and name != image_name:
                rbd_inst.rename(ioctx, image_name, name)

            # check resize
            if size and size != image.size():
                image.resize(size)

            mirror_image_info = image.mirror_image_get_info()
            if enable_mirror and mirror_image_info['state'] == rbd.RBD_MIRROR_IMAGE_DISABLED:
                RbdMirroringService.enable_image(
                    image_name, pool_name, namespace,
                    MIRROR_IMAGE_MODE[mirror_mode])
            elif (enable_mirror is False
                  and mirror_image_info['state'] == rbd.RBD_MIRROR_IMAGE_ENABLED):
                RbdMirroringService.disable_image(
                    image_name, pool_name, namespace)

            # check enable/disable features
            if features is not None:
                curr_features = format_bitmask(image.features())
                # check disabled features
                _sort_features(curr_features, enable=False)
                for feature in curr_features:
                    if (feature not in features
                       and feature in cls.ALLOW_DISABLE_FEATURES
                       and feature in format_bitmask(image.features())):
                        f_bitmask = format_features([feature])
                        image.update_features(f_bitmask, False)
                # check enabled features
                _sort_features(features)
                for feature in features:
                    if (feature not in curr_features
                       and feature in cls.ALLOW_ENABLE_FEATURES
                       and feature not in format_bitmask(image.features())):
                        f_bitmask = format_features([feature])
                        image.update_features(f_bitmask, True)

            RbdConfiguration(pool_ioctx=ioctx, image_name=image_name).set_configuration(
                configuration)
            if metadata:
                RbdImageMetadataService(image).set_metadata(metadata)

            if primary and not mirror_image_info['primary']:
                RbdMirroringService.promote_image(
                    image_name, pool_name, namespace, force)
            elif primary is False and mirror_image_info['primary']:
                RbdMirroringService.demote_image(
                    image_name, pool_name, namespace)

            if resync:
                RbdMirroringService.resync_image(image_name, pool_name, namespace)

            if schedule_interval:
                RbdMirroringService.snapshot_schedule_add(image_spec, schedule_interval)

            if remove_scheduling:
                RbdMirroringService.snapshot_schedule_remove(image_spec)

        return rbd_image_call(pool_name, namespace, image_name, _edit)

    @classmethod
    @ttl_cache_invalidator(RBD_IMAGE_REFS_CACHE_REFERENCE)
    def delete(cls, image_spec):
        pool_name, namespace, image_name = parse_image_spec(image_spec)

        image = RbdService.get_image(image_spec)
        snapshots = image['snapshots']
        for snap in snapshots:
            RbdSnapshotService.remove_snapshot(image_spec, snap['name'], snap['is_protected'])

        rbd_inst = rbd.RBD()
        return rbd_call(pool_name, namespace, rbd_inst.remove, image_name)

    @classmethod
    @ttl_cache_invalidator(RBD_IMAGE_REFS_CACHE_REFERENCE)
    def copy(cls, image_spec, dest_pool_name, dest_namespace, dest_image_name,
             snapshot_name=None, obj_size=None, features=None,
             stripe_unit=None, stripe_count=None, data_pool=None,
             configuration=None, metadata=None):
        pool_name, namespace, image_name = parse_image_spec(image_spec)

        def _src_copy(s_ioctx, s_img):
            def _copy(d_ioctx):
                # Set order
                l_order = None
                if obj_size and obj_size > 0:
                    l_order = int(round(math.log(float(obj_size), 2)))

                # Set features
                feature_bitmask = format_features(features)

                if snapshot_name:
                    s_img.set_snap(snapshot_name)

                s_img.copy(d_ioctx, dest_image_name, feature_bitmask, l_order,
                           stripe_unit, stripe_count, data_pool)
                RbdConfiguration(pool_ioctx=d_ioctx, image_name=dest_image_name).set_configuration(
                    configuration)
                if metadata:
                    with rbd.Image(d_ioctx, dest_image_name) as image:
                        RbdImageMetadataService(image).set_metadata(metadata)

            return rbd_call(dest_pool_name, dest_namespace, _copy)

        return rbd_image_call(pool_name, namespace, image_name, _src_copy)

    @classmethod
    @ttl_cache_invalidator(RBD_IMAGE_REFS_CACHE_REFERENCE)
    def flatten(cls, image_spec):
        def _flatten(ioctx, image):
            image.flatten()

        pool_name, namespace, image_name = parse_image_spec(image_spec)
        return rbd_image_call(pool_name, namespace, image_name, _flatten)

    @classmethod
    def move_image_to_trash(cls, image_spec, delay):
        pool_name, namespace, image_name = parse_image_spec(image_spec)
        rbd_inst = cls._rbd_inst
        return rbd_call(pool_name, namespace, rbd_inst.trash_move, image_name, delay)


class RbdSnapshotService(object):

    @classmethod
    def remove_snapshot(cls, image_spec, snapshot_name, unprotect=False):
        def _remove_snapshot(ioctx, img, snapshot_name, unprotect):
            if unprotect:
                img.unprotect_snap(snapshot_name)
            img.remove_snap(snapshot_name)

        pool_name, namespace, image_name = parse_image_spec(image_spec)
        return rbd_image_call(pool_name, namespace, image_name,
                              _remove_snapshot, snapshot_name, unprotect)


class RBDSchedulerInterval:
    def __init__(self, interval: str):
        self.amount = int(interval[:-1])
        self.unit = interval[-1]
        if self.unit not in 'mhd':
            raise ValueError(f'Invalid interval unit {self.unit}')

    def __str__(self):
        return f'{self.amount}{self.unit}'


class RbdMirroringService:

    @classmethod
    def enable_image(cls, image_name: str, pool_name: str, namespace: str, mode: MIRROR_IMAGE_MODE):
        rbd_image_call(pool_name, namespace, image_name,
                       lambda ioctx, image: image.mirror_image_enable(mode))

    @classmethod
    def disable_image(cls, image_name: str, pool_name: str, namespace: str, force: bool = False):
        rbd_image_call(pool_name, namespace, image_name,
                       lambda ioctx, image: image.mirror_image_disable(force))

    @classmethod
    def promote_image(cls, image_name: str, pool_name: str, namespace: str, force: bool = False):
        rbd_image_call(pool_name, namespace, image_name,
                       lambda ioctx, image: image.mirror_image_promote(force))

    @classmethod
    def demote_image(cls, image_name: str, pool_name: str, namespace: str):
        rbd_image_call(pool_name, namespace, image_name,
                       lambda ioctx, image: image.mirror_image_demote())

    @classmethod
    def resync_image(cls, image_name: str, pool_name: str, namespace: str):
        rbd_image_call(pool_name, namespace, image_name,
                       lambda ioctx, image: image.mirror_image_resync())

    @classmethod
    def snapshot_schedule_add(cls, image_spec: str, interval: str):
        _rbd_support_remote('mirror_snapshot_schedule_add', image_spec,
                            str(RBDSchedulerInterval(interval)))

    @classmethod
    def snapshot_schedule_remove(cls, image_spec: str):
        _rbd_support_remote('mirror_snapshot_schedule_remove', image_spec)


class RbdImageMetadataService(object):
    def __init__(self, image):
        self._image = image

    def list(self):
        result = self._image.metadata_list()
        # filter out configuration metadata
        return {v[0]: v[1] for v in result if not v[0].startswith('conf_')}

    def get(self, name):
        return self._image.metadata_get(name)

    def set(self, name, value):
        self._image.metadata_set(name, value)

    def remove(self, name):
        try:
            self._image.metadata_remove(name)
        except KeyError:
            pass

    def set_metadata(self, metadata):
        for name, value in metadata.items():
            if value is not None:
                self.set(name, value)
            else:
                self.remove(name)
