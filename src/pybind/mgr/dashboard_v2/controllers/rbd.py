# -*- coding: utf-8 -*-
from __future__ import absolute_import

import rbd

from .. import mgr
from ..tools import ApiController, AuthRequired, RESTController, ViewCache


@ApiController('rbd')
@AuthRequired()
class Rbd(RESTController):

    def __init__(self):
        self.rbd = None

    @staticmethod
    def _format_bitmask(features):
        """
        Formats the bitmask:

        >>> Rbd._format_bitmask(45)
        'deep-flatten, exclusive-lock, layering, object-map'
        """
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
        names = [val for key, val in RBD_FEATURES_NAME_MAPPING.items()
                 if key & features == key]
        return ', '.join(sorted(names))

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
