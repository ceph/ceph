# -*- coding: utf-8 -*-
from __future__ import absolute_import

from enum import Enum
import cherrypy
from mgr_module import CLICommand, Option

from . import PLUGIN_MANAGER as PM
from . import interfaces as I


try:
    from functools import lru_cache
except ImportError:
    try:
        from backports.functools_lru_cache import lru_cache
    except ImportError:
        """
        This is a minimal implementation of lru_cache function.

        Based on Python 3 functools and backports.functools_lru_cache.
        """

        from functools import wraps
        from collections import OrderedDict
        from threading import RLock

        def lru_cache(maxsize=128, typed=False):
            if typed is not False:
                raise NotImplementedError("typed caching not supported")

            def decorating_function(function):
                cache = OrderedDict()
                stats = [0, 0]
                rlock = RLock()
                setattr(
                    function,
                    'cache_info',
                    lambda:
                    "hits={}, misses={}, maxsize={}, currsize={}".format(
                        stats[0], stats[1], maxsize, len(cache)))

                @wraps(function)
                def wrapper(*args, **kwargs):
                    key = args + tuple(kwargs.items())
                    with rlock:
                        if key in cache:
                            ret = cache[key]
                            del cache[key]
                            cache[key] = ret
                            stats[0] += 1
                        else:
                            ret = function(*args, **kwargs)
                            if len(cache) == maxsize:
                                cache.popitem(last=False)
                            cache[key] = ret
                            stats[1] += 1
                    return ret

                return wrapper
            return decorating_function


class Features(Enum):
    RBD_IMAGES = 'rbd_images'
    RBD_MIRRORING = 'rbd_mirroring'
    RBD_ISCSI = 'rbd_iscsi'
    CEPHFS = 'cephfs'
    RGW = 'rgw'


class Actions(Enum):
    ENABLE = 'enable'
    DISABLE = 'disable'
    STATUS = 'status'


PREDISABLED_FEATURES = set()

Feature2Endpoint = {
    Features.RBD_IMAGES: ["/api/block/image"],
    Features.RBD_MIRRORING: ["/api/block/mirroring"],
    Features.RBD_ISCSI: ["/api/tcmuiscsi"],
    Features.CEPHFS: ["/api/cephfs"],
    Features.RGW: ["/api/rgw"],
}


@PM.add_plugin
class FeatureToggles(I.CanMgr, I.CanLog, I.Setupable, I.HasOptions,
                     I.HasCommands, I.FilterRequest.BeforeHandler,
                     I.HasEndpoints):
    OPTION_FMT = 'FEATURE_TOGGLE_{}'
    CACHE_MAX_SIZE = 128  # Optimum performance with 2^N sizes

    @PM.add_hook
    def setup(self):
        url_prefix = self.mgr.get_module_option('url_prefix')
        self.Endpoint2Feature = {
            '{}{}'.format(url_prefix, endpoint): feature
            for feature, endpoints in Feature2Endpoint.items()
            for endpoint in endpoints}

    @PM.add_hook
    def get_options(self):
        return [Option(
            name=self.OPTION_FMT.format(feature.value),
            default=(feature not in PREDISABLED_FEATURES),
            type='bool',) for feature in Features]

    @PM.add_hook
    def register_commands(self):
        @CLICommand(
            "dashboard feature",
            "name=action,type=CephChoices,strings={} ".format(
                "|".join(a.value for a in Actions))
            + "name=features,type=CephChoices,strings={},req=false,n=N".format(
                "|".join(f.value for f in Features)),
            "Enable or disable features in Ceph-Mgr Dashboard")
        def _(mgr, action, features=None):
            ret = 0
            msg = []
            if action in [Actions.ENABLE.value, Actions.DISABLE.value]:
                if features is None:
                    ret = 1
                    msg = ["Feature '{}' requires at least a feature specified".format(
                        action)]
                else:
                    for feature in features:
                        mgr.set_module_option(
                            self.OPTION_FMT.format(feature),
                            action == Actions.ENABLE.value)
                        msg += ["Feature '{}': {}".format(
                            feature,
                            'enabled' if action == Actions.ENABLE.value else 'disabled')]
            else:
                for feature in features or [f.value for f in Features]:
                    enabled = mgr.get_module_option(self.OPTION_FMT.format(feature))
                    msg += ["Feature '{}': '{}'".format(
                        feature,
                        'enabled' if enabled else 'disabled')]
            return ret, '\n'.join(msg), ''

    @lru_cache(maxsize=CACHE_MAX_SIZE)
    def __get_feature_from_path(self, path):
        for endpoint in self.Endpoint2Feature:
            if path.startswith(endpoint):
                return self.Endpoint2Feature[endpoint]
        return None

    @PM.add_hook
    def filter_request_before_handler(self, request):
        feature = self.__get_feature_from_path(request.path_info)

        if feature is None:
            return

        if self.mgr.get_module_option(self.OPTION_FMT.format(feature.value)) is False:
            raise cherrypy.HTTPError(
                501, "Feature='{}' (path='{}') disabled by option '{}'".format(
                    feature.value,
                    request.path_info,
                    self.OPTION_FMT.format(feature.value)
                    ))

    @PM.add_hook
    def register_endpoints(self):
        pass
