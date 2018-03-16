# -*- coding: utf-8 -*-
from __future__ import absolute_import

import math
import cherrypy
import rbd

from . import ApiController, AuthRequired, RESTController
from .. import mgr
from ..services.ceph_service import CephService
from ..tools import ViewCache


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
        if not features or not isinstance(features, list):
            return None

        res = 0
        for key, value in Rbd.RBD_FEATURES_NAME_MAPPING.items():
            if value in features:
                res = key | res
        return res

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
            snap['children'] = []
            img.set_snap(snap['name'])
            for child_pool_name, child_image_name in img.list_children():
                snap['children'].append({
                    'pool_name': child_pool_name,
                    'image_name': child_image_name
                })
            stat['snapshots'].append(snap)

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

    def create(self, data):
        # pylint: disable=too-many-locals
        rbd_inst = rbd.RBD()

        # Get input values
        name = data.get('name')
        pool_name = data.get('pool_name')
        size = data.get('size')
        obj_size = data.get('obj_size')
        features = data.get('features')
        stripe_unit = data.get('stripe_unit')
        stripe_count = data.get('stripe_count')
        data_pool = data.get('data_pool')

        # Set order
        order = None
        if obj_size and obj_size > 0:
            order = int(round(math.log(float(obj_size), 2)))

        # Set features
        feature_bitmask = self._format_features(features)

        ioctx = mgr.rados.open_ioctx(pool_name)

        try:
            rbd_inst.create(ioctx, name, size, order=order, old_format=False,
                            features=feature_bitmask, stripe_unit=stripe_unit,
                            stripe_count=stripe_count, data_pool=data_pool)
        except rbd.OSError as e:
            cherrypy.response.status = 400
            return {'success': False, 'detail': str(e), 'errno': e.errno}
        return {'success': True}
