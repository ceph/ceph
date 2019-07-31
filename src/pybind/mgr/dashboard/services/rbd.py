# -*- coding: utf-8 -*-
from __future__ import absolute_import

import six

import rbd

from .. import mgr


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

    >>> format_bitmask(45)
    ['deep-flatten', 'exclusive-lock', 'layering', 'object-map']
    """
    names = [val for key, val in RBD_FEATURES_NAME_MAPPING.items()
             if key & features == key]
    return sorted(names)


def format_features(features):
    """
    Converts the features list to bitmask:

    >>> format_features(['deep-flatten', 'exclusive-lock', 'layering', 'object-map'])
    45

    >>> format_features(None) is None
    True

    >>> format_features('deep-flatten, exclusive-lock')
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


class RbdConfiguration(object):
    _rbd = rbd.RBD()

    def __init__(self, pool_name='', image_name='', pool_ioctx=None, image_ioctx=None):
        # type: (str, str, object, object) -> None
        assert bool(pool_name) != bool(pool_ioctx)  # xor
        self._pool_name = pool_name
        self._image_name = image_name
        self._pool_ioctx = pool_ioctx
        self._image_ioctx = image_ioctx

    @staticmethod
    def _ensure_prefix(option):
        # type: (str) -> str
        return option if option.startswith('conf_') else 'conf_' + option

    def list(self):
        # type: () -> [dict]
        def _list(ioctx):
            if self._image_name:  # image config
                with rbd.Image(ioctx, self._image_name) as image:
                    result = image.config_list()
            else:  # pool config
                result = self._rbd.config_list(ioctx)
            return list(result)

        if self._pool_name:
            ioctx = mgr.rados.open_ioctx(self._pool_name)
        else:
            ioctx = self._pool_ioctx

        return _list(ioctx)

    def get(self, option_name):
        # type: (str) -> str
        option_name = self._ensure_prefix(option_name)
        with mgr.rados.open_ioctx(self._pool_name) as pool_ioctx:
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
            pool_ioctx.__enter__()

        image_ioctx = self._image_ioctx
        if self._image_name:
            image_ioctx = rbd.Image(pool_ioctx, self._image_name)
            image_ioctx.__enter__()

        if image_ioctx:
            image_ioctx.metadata_set(option_name, option_value)
        else:
            self._rbd.pool_metadata_set(pool_ioctx, option_name, option_value)

        if self._image_name:  # Name provided, so we opened it and now have to close it
            image_ioctx.__exit__(None, None, None)
        if self._pool_name:
            pool_ioctx.__exit__(None, None, None)

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
