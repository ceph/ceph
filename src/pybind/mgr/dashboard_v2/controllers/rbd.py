# -*- coding: utf-8 -*-
from __future__ import absolute_import

import math
import cherrypy
import rbd

from .. import mgr
from ..tools import ApiController, AuthRequired, RESTController, ViewCache


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
        rbd.RBD_FEATURE_OPERATIONS: "operations",
    }

    def __init__(self):
        self.rbd = None

    @staticmethod
    def _format_bitmask(features):
        """
        Formats the bitmask:

        >>> Rbd._format_bitmask(45)
        'deep-flatten, exclusive-lock, layering, object-map'
        """
        names = [val for key, val in Rbd.RBD_FEATURES_NAME_MAPPING.items()
                 if key & features == key]
        return ', '.join(sorted(names))

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

    @ViewCache()
    def _rbd_list(self, pool_name):
        ioctx = mgr.rados.open_ioctx(pool_name)
        self.rbd = rbd.RBD()
        names = self.rbd.list(ioctx)
        result = []
        for name in names:
            i = rbd.Image(ioctx, name)
            stat = i.stat()
            stat['name'] = name
            features = i.features()
            stat['features'] = features
            stat['features_name'] = self._format_bitmask(features)

            try:
                parent_info = i.parent_info()
                parent = "{}@{}".format(parent_info[0], parent_info[1])
                if parent_info[0] != pool_name:
                    parent = "{}/{}".format(parent_info[0], parent)
                stat['parent'] = parent
            except rbd.ImageNotFound:
                pass
            result.append(stat)
        return result

    def get(self, pool_name):
        # pylint: disable=unbalanced-tuple-unpacking
        status, value = self._rbd_list(pool_name)
        if status == ViewCache.VALUE_EXCEPTION:
            raise value
        return {'status': status, 'value': value}

    def create(self, data):
        if not self.rbd:
            self.rbd = rbd.RBD()

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
            self.rbd.create(ioctx, name, size, order=order, old_format=False,
                            features=feature_bitmask, stripe_unit=stripe_unit,
                            stripe_count=stripe_count, data_pool=data_pool)
        except rbd.OSError as e:
            cherrypy.response.status = 400
            return {'success': False, 'detail': str(e), 'errno': e.errno}
        return {'success': True}
