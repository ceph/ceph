# -*- coding: utf-8 -*-
# pylint: disable=unused-argument
from __future__ import absolute_import

import six

import cherrypy

import rbd

from .. import mgr
from ..tools import ViewCache
from .ceph_service import CephService

try:
    from typing import List
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
    if isinstance(features, six.string_types):
        features = features.split(',')

    if not isinstance(features, list):
        return None

    res = 0
    for key, value in RBD_FEATURES_NAME_MAPPING.items():
        if value in features:
            res = key | res
    return res


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
        func(ioctx, *args, **kwargs)


def rbd_image_call(pool_name, namespace, image_name, func, *args, **kwargs):
    def _ioctx_func(ioctx, image_name, func, *args, **kwargs):
        with rbd.Image(ioctx, image_name) as img:
            func(ioctx, img, *args, **kwargs)

    return rbd_call(pool_name, namespace, _ioctx_func, image_name, func, *args, **kwargs)


class RbdConfiguration(object):
    _rbd = rbd.RBD()

    def __init__(self, pool_name='', namespace='', image_name='', pool_ioctx=None,
                 image_ioctx=None):
        # type: (str, str, str, object, object) -> None
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
    def _rbd_image(cls, ioctx, pool_name, namespace, image_name):
        with rbd.Image(ioctx, image_name) as img:

            stat = img.stat()
            stat['name'] = image_name
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

            try:
                stat['parent'] = img.get_parent_image_spec()
            except rbd.ImageNotFound:
                # no parent image
                stat['parent'] = None

            # snapshots
            stat['snapshots'] = []
            for snap in img.list_snaps():
                snap['timestamp'] = "{}Z".format(
                    img.get_snap_timestamp(snap['id']).isoformat())
                snap['is_protected'] = img.is_protected_snap(snap['name'])
                snap['used_bytes'] = None
                snap['children'] = []
                img.set_snap(snap['name'])
                for child_pool_name, child_image_name in img.list_children():
                    snap['children'].append({
                        'pool_name': child_pool_name,
                        'image_name': child_image_name
                    })
                stat['snapshots'].append(snap)

            # disk usage
            img_flags = img.flags()
            if 'fast-diff' in stat['features_name'] and \
                    not rbd.RBD_FLAG_FAST_DIFF_INVALID & img_flags:
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

            stat['configuration'] = RbdConfiguration(pool_ioctx=ioctx, image_name=image_name).list()

            return stat

    @classmethod
    def _rbd_image_names(cls, ioctx):
        rbd_inst = rbd.RBD()
        return rbd_inst.list(ioctx)

    @classmethod
    def _rbd_image_stat(cls, ioctx, pool_name, namespace, image_name):
        return cls._rbd_image(ioctx, pool_name, namespace, image_name)

    @classmethod
    @ViewCache()
    def rbd_pool_list(cls, pool_name, namespace=None):
        rbd_inst = rbd.RBD()
        with mgr.rados.open_ioctx(pool_name) as ioctx:
            result = []
            if namespace:
                namespaces = [namespace]
            else:
                namespaces = rbd_inst.namespace_list(ioctx)
                # images without namespace
                namespaces.append('')
            for current_namespace in namespaces:
                ioctx.set_namespace(current_namespace)
                names = cls._rbd_image_names(ioctx)
                for name in names:
                    try:
                        stat = cls._rbd_image_stat(ioctx, pool_name, current_namespace, name)
                    except rbd.ImageNotFound:
                        # may have been removed in the meanwhile
                        continue
                    result.append(stat)
            return result

    @classmethod
    def get_image(cls, image_spec):
        pool_name, namespace, image_name = parse_image_spec(image_spec)
        ioctx = mgr.rados.open_ioctx(pool_name)
        if namespace:
            ioctx.set_namespace(namespace)
        try:
            return cls._rbd_image(ioctx, pool_name, namespace, image_name)
        except rbd.ImageNotFound:
            raise cherrypy.HTTPError(404, 'Image not found')


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
