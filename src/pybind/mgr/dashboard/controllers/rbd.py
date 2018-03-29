# -*- coding: utf-8 -*-
# pylint: disable=too-many-arguments,too-many-locals
from __future__ import absolute_import

import math
import cherrypy
import rbd

from . import ApiController, AuthRequired, RESTController
from .. import mgr
from ..services.ceph_service import CephService
from ..tools import ViewCache, TaskManager


@ApiController('rbd')
@AuthRequired()
class Rbd(RESTController):

    RBD_FEATURES_NAME_MAPPING = {
        rbd.RBD_FEATURE_LAYERING: "layering",
        rbd.RBD_FEATURE_STRIPINGV2: "striping",
        rbd.RBD_FEATURE_EXCLUSIVE_LOCK: "exclusive-lock",
        rbd.RBD_FEATURE_OBJECT_MAP: "object-map",
        rbd.RBD_FEATURE_FAST_DIFF: "fast-diff",
        rbd.RBD_FEATURE_DEEP_FLATTEN: "deep-flatten",
        rbd.RBD_FEATURE_JOURNALING: "journaling",
        rbd.RBD_FEATURE_DATA_POOL: "data-pool",
    }

    # set of image features that can be enable on existing images
    ALLOW_ENABLE_FEATURES = set(["exclusive-lock", "object-map", "fast-diff",
                                 "journaling"])

    # set of image features that can be disabled on existing images
    ALLOW_DISABLE_FEATURES = set(["exclusive-lock", "object-map", "fast-diff",
                                  "deep-flatten", "journaling"])

    @staticmethod
    def _format_bitmask(features):
        """
        Formats the bitmask:

        >>> Rbd._format_bitmask(45)
        ['deep-flatten', 'exclusive-lock', 'layering', 'object-map']
        """
        names = [val for key, val in Rbd.RBD_FEATURES_NAME_MAPPING.items()
                 if key & features == key]
        return sorted(names)

    @staticmethod
    def _format_features(features):
        """
        Converts the features list to bitmask:

        >>> Rbd._format_features(['deep-flatten', 'exclusive-lock', 'layering', 'object-map'])
        45

        >>> Rbd._format_features(None) is None
        True

        >>> Rbd._format_features('not a list') is None
        True
        """
        if not isinstance(features, list):
            return None

        res = 0
        for key, value in Rbd.RBD_FEATURES_NAME_MAPPING.items():
            if value in features:
                res = key | res
        return res

    @classmethod
    def _rbd_disk_usage(cls, image, snaps):
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
            image.diff_iterate(0, size, prev_snap, du_callb, whole_object=True)
            snap_map[name] = du_callb.used_size
            total_used_size += du_callb.used_size
            prev_snap = name

        return total_used_size, snap_map

    def _rbd_image(self, ioctx, pool_name, image_name):
        img = rbd.Image(ioctx, image_name)
        stat = img.stat()
        stat['name'] = image_name
        stat['id'] = img.id()
        stat['pool_name'] = pool_name
        features = img.features()
        stat['features'] = features
        stat['features_name'] = self._format_bitmask(features)

        # the following keys are deprecated
        del stat['parent_pool']
        del stat['parent_name']

        stat['timestamp'] = "{}Z".format(img.create_timestamp().isoformat())

        stat['stripe_count'] = img.stripe_count()
        stat['stripe_unit'] = img.stripe_unit()

        data_pool_name = CephService.get_pool_name_from_id(img.data_pool_id())
        if data_pool_name == pool_name:
            data_pool_name = None
        stat['data_pool'] = data_pool_name

        try:
            parent_info = img.parent_info()
            stat['parent'] = {
                'pool_name': parent_info[0],
                'image_name': parent_info[1],
                'snap_name': parent_info[2]
            }
        except rbd.ImageNotFound:
            # no parent image
            stat['parent'] = None

        # snapshots
        stat['snapshots'] = []
        for snap in img.list_snaps():
            snap['timestamp'] = "{}Z".format(img.get_snap_timestamp(snap['id']).isoformat())
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
        if 'fast-diff' in stat['features_name']:
            snaps = [(s['id'], s['size'], s['name']) for s in stat['snapshots']]
            snaps.sort(key=lambda s: s[0])
            snaps += [(snaps[-1][0]+1 if snaps else 0, stat['size'], None)]
            total_used_bytes, snaps_used_bytes = self._rbd_disk_usage(img, snaps)
            stat['total_disk_usage'] = total_used_bytes
            for snap, used_bytes in snaps_used_bytes.items():
                if snap is None:
                    stat['disk_usage'] = used_bytes
                    continue
                for ss in stat['snapshots']:
                    if ss['name'] == snap:
                        ss['disk_usage'] = used_bytes
                        break
        else:
            stat['total_disk_usage'] = None
            stat['disk_usage'] = None

        return stat

    @ViewCache()
    def _rbd_pool_list(self, pool_name):
        rbd_inst = rbd.RBD()
        ioctx = mgr.rados.open_ioctx(pool_name)
        names = rbd_inst.list(ioctx)
        result = []
        for name in names:
            try:
                stat = self._rbd_image(ioctx, pool_name, name)
            except rbd.ImageNotFound:
                # may have been removed in the meanwhile
                continue
            result.append(stat)
        return result

    def _rbd_list(self, pool_name=None):
        if pool_name:
            pools = [pool_name]
        else:
            pools = [p['pool_name'] for p in CephService.get_pool_list('rbd')]

        result = []
        for pool in pools:
            # pylint: disable=unbalanced-tuple-unpacking
            status, value = self._rbd_pool_list(pool)
            result.append({'status': status, 'value': value, 'pool_name': pool})
        return result

    def list(self, pool_name=None):
        # pylint: disable=unbalanced-tuple-unpacking
        return self._rbd_list(pool_name)

    def get(self, pool_name, image_name):
        ioctx = mgr.rados.open_ioctx(pool_name)
        try:
            return self._rbd_image(ioctx, pool_name, image_name)
        except rbd.ImageNotFound:
            raise cherrypy.HTTPError(404)

    @classmethod
    def _create_image(cls, name, pool_name, size, obj_size=None, features=None,
                      stripe_unit=None, stripe_count=None, data_pool=None):
        # pylint: disable=too-many-locals
        rbd_inst = rbd.RBD()

        # Set order
        order = None
        if obj_size and obj_size > 0:
            order = int(round(math.log(float(obj_size), 2)))

        # Set features
        feature_bitmask = cls._format_features(features)

        ioctx = mgr.rados.open_ioctx(pool_name)

        try:
            rbd_inst.create(ioctx, name, size, order=order, old_format=False,
                            features=feature_bitmask, stripe_unit=stripe_unit,
                            stripe_count=stripe_count, data_pool=data_pool)
        except rbd.OSError as e:
            return {'success': False, 'detail': str(e), 'errno': e.errno}
        return {'success': True}

    @RESTController.args_from_json
    def create(self, name, pool_name, size, obj_size=None, features=None,
               stripe_unit=None, stripe_count=None, data_pool=None):
        task = TaskManager.run('rbd/create',
                               {'pool_name': pool_name, 'image_name': name},
                               self._create_image,
                               [name, pool_name, size, obj_size, features,
                                stripe_unit, stripe_count, data_pool])
        status, value = task.wait(1.0)
        return {'status': status, 'value': value}

    @classmethod
    def _remove_image(cls, pool_name, image_name):
        rbd_inst = rbd.RBD()
        ioctx = mgr.rados.open_ioctx(pool_name)
        try:
            rbd_inst.remove(ioctx, image_name)
        except rbd.OSError as e:
            return {'success': False, 'detail': str(e), 'errno': e.errno}
        return {'success': True}

    def delete(self, pool_name, image_name):
        task = TaskManager.run('rbd/delete',
                               {'pool_name': pool_name, 'image_name': image_name},
                               self._remove_image, [pool_name, image_name])
        status, value = task.wait(2.0)
        cherrypy.response.status = 200
        return {'status': status, 'value': value}

    @classmethod
    def _sort_features(cls, features, enable=True):
        """
        Sorts image features according to feature dependencies:

        object-map depends on exclusive-lock
        journaling depends on exclusive-lock
        fast-diff depends on object-map
        """
        ORDER = ['exclusive-lock', 'journaling', 'object-map', 'fast-diff']

        def key_func(feat):
            try:
                return ORDER.index(feat)
            except ValueError:
                return id(feat)

        features.sort(key=key_func, reverse=not enable)

    @classmethod
    def _edit_image(cls, pool_name, image_name, name, size, features):
        rbd_inst = rbd.RBD()
        ioctx = mgr.rados.open_ioctx(pool_name)
        image = rbd.Image(ioctx, image_name)

        # check rename image
        if name and name != image_name:
            try:
                rbd_inst.rename(ioctx, image_name, name)
            except rbd.OSError as e:
                return {'success': False, 'detail': str(e), 'errno': e.errno}

        # check resize
        if size and size != image.size():
            try:
                image.resize(size)
            except rbd.OSError as e:
                return {'success': False, 'detail': str(e), 'errno': e.errno}

        # check enable/disable features
        if features is not None:
            curr_features = cls._format_bitmask(image.features())
            # check disabled features
            cls._sort_features(curr_features, enable=False)
            for feature in curr_features:
                if feature not in features and feature in cls.ALLOW_DISABLE_FEATURES:
                    f_bitmask = cls._format_features([feature])
                    image.update_features(f_bitmask, False)
            # check enabled features
            cls._sort_features(features)
            for feature in features:
                if feature not in curr_features and feature in cls.ALLOW_ENABLE_FEATURES:
                    f_bitmask = cls._format_features([feature])
                    image.update_features(f_bitmask, True)

        return {'success': True}

    @RESTController.args_from_json
    def set(self, pool_name, image_name, name=None, size=None, features=None):
        task = TaskManager.run('rbd/edit',
                               {'pool_name': pool_name, 'image_name': image_name},
                               self._edit_image,
                               [pool_name, image_name, name, size, features])
        status, value = task.wait(4.0)
        return {'status': status, 'value': value}


@ApiController('rbd/:pool_name/:image_name/snap')
class RbdSnapshot(RESTController):

    @classmethod
    def _create_snapshot(cls, pool_name, image_name, snapshot_name):
        ioctx = mgr.rados.open_ioctx(pool_name)
        img = rbd.Image(ioctx, image_name)
        try:
            img.create_snap(snapshot_name)
        except rbd.OSError as e:
            return {'success': False, 'detail': str(e), 'errno': e.errno}
        return {'success': True}

    @RESTController.args_from_json
    def create(self, pool_name, image_name, snapshot_name):
        task = TaskManager.run('rbd/snap/create',
                               {'pool_name': pool_name, 'image_name': image_name,
                                'snapshot_name': snapshot_name},
                               self._create_snapshot,
                               [pool_name, image_name, snapshot_name])
        status, value = task.wait(1.0)
        return {'status': status, 'value': value}

    @classmethod
    def _remove_snapshot(cls, pool_name, image_name, snapshot_name):
        ioctx = mgr.rados.open_ioctx(pool_name)
        img = rbd.Image(ioctx, image_name)
        try:
            img.remove_snap(snapshot_name)
        except rbd.OSError as e:
            return {'success': False, 'detail': str(e), 'errno': e.errno}
        return {'success': True}

    def delete(self, pool_name, image_name, snapshot_name):
        task = TaskManager.run('rbd/snap/delete',
                               {'pool_name': pool_name,
                                'image_name': image_name,
                                'snapshot_name': snapshot_name},
                               self._remove_snapshot,
                               [pool_name, image_name, snapshot_name])
        status, value = task.wait(1.0)
        cherrypy.response.status = 200
        return {'status': status, 'value': value}
