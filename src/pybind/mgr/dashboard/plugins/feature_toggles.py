# -*- coding: utf-8 -*-

from enum import Enum
from typing import List, Optional, Set, no_type_check

import cherrypy
from mgr_module import CLICommand, Option

from ..controllers.cephfs import CephFS
from ..controllers.iscsi import Iscsi, IscsiTarget
from ..controllers.nfs import NFSGaneshaExports, NFSGaneshaUi
from ..controllers.rbd import Rbd, RbdSnapshot, RbdTrash
from ..controllers.rbd_mirroring import RbdMirroringPoolMode, \
    RbdMirroringPoolPeer, RbdMirroringSummary
from ..controllers.rgw import Rgw, RgwBucket, RgwDaemon, RgwUser
from . import PLUGIN_MANAGER as PM
from . import interfaces as I  # noqa: E741,N812
from .ttl_cache import ttl_cache


class Features(Enum):
    RBD = 'rbd'
    MIRRORING = 'mirroring'
    ISCSI = 'iscsi'
    CEPHFS = 'cephfs'
    RGW = 'rgw'
    NFS = 'nfs'
    DASHBOARD = 'dashboard'


PREDISABLED_FEATURES = set()  # type: Set[str]

Feature2Controller = {
    Features.RBD: [Rbd, RbdSnapshot, RbdTrash],
    Features.MIRRORING: [
        RbdMirroringSummary, RbdMirroringPoolMode, RbdMirroringPoolPeer],
    Features.ISCSI: [Iscsi, IscsiTarget],
    Features.CEPHFS: [CephFS],
    Features.RGW: [Rgw, RgwDaemon, RgwBucket, RgwUser],
    Features.NFS: [NFSGaneshaUi, NFSGaneshaExports],
}


class Actions(Enum):
    ENABLE = 'enable'
    DISABLE = 'disable'
    STATUS = 'status'


# pylint: disable=too-many-ancestors
@PM.add_plugin
class FeatureToggles(I.CanMgr, I.Setupable, I.HasOptions,
                     I.HasCommands, I.FilterRequest.BeforeHandler,
                     I.HasControllers):
    OPTION_FMT = 'FEATURE_TOGGLE_{.name}'
    CACHE_MAX_SIZE = 128  # Optimum performance with 2^N sizes
    CACHE_TTL = 10  # seconds

    @PM.add_hook
    def setup(self):
        # pylint: disable=attribute-defined-outside-init
        self.Controller2Feature = {
            controller: feature
            for feature, controllers in Feature2Controller.items()
            for controller in controllers}  # type: ignore

    @PM.add_hook
    def get_options(self):
        return [Option(
            name=self.OPTION_FMT.format(feature),
            default=(feature not in PREDISABLED_FEATURES),
            type='bool',) for feature in Features]

    @PM.add_hook
    def register_commands(self):
        @CLICommand("dashboard feature")
        def cmd(mgr,
                action: Actions = Actions.STATUS,
                features: Optional[List[Features]] = None):
            '''
            Enable or disable features in Ceph-Mgr Dashboard
            '''
            ret = 0
            msg = []
            if action in [Actions.ENABLE, Actions.DISABLE]:
                if features is None:
                    ret = 1
                    msg = ["At least one feature must be specified"]
                else:
                    for feature in features:
                        mgr.set_module_option(
                            self.OPTION_FMT.format(feature),
                            action == Actions.ENABLE)
                        msg += ["Feature '{.value}': {}".format(
                            feature,
                            'enabled' if action == Actions.ENABLE else
                            'disabled')]
            else:
                for feature in features or list(Features):
                    enabled = mgr.get_module_option(self.OPTION_FMT.format(feature))
                    msg += ["Feature '{.value}': {}".format(
                        feature,
                        'enabled' if enabled else 'disabled')]
            return ret, '\n'.join(msg), ''
        return {'handle_command': cmd}

    @no_type_check  # https://github.com/python/mypy/issues/7806
    def _get_feature_from_request(self, request):
        try:
            return self.Controller2Feature[
                request.handler.callable.__self__]
        except (AttributeError, KeyError):
            return None

    @ttl_cache(ttl=CACHE_TTL, maxsize=CACHE_MAX_SIZE)
    @no_type_check  # https://github.com/python/mypy/issues/7806
    def _is_feature_enabled(self, feature):
        return self.mgr.get_module_option(self.OPTION_FMT.format(feature))

    @PM.add_hook
    def filter_request_before_handler(self, request):
        feature = self._get_feature_from_request(request)
        if feature is None:
            return

        if not self._is_feature_enabled(feature):
            raise cherrypy.HTTPError(
                404, "Feature='{}' disabled by option '{}'".format(
                    feature.value,
                    self.OPTION_FMT.format(feature),
                )
            )

    @PM.add_hook
    def get_controllers(self):
        from ..controllers import APIDoc, APIRouter, EndpointDoc, RESTController

        FEATURES_SCHEMA = {
            "rbd": (bool, ''),
            "mirroring": (bool, ''),
            "iscsi": (bool, ''),
            "cephfs": (bool, ''),
            "rgw": (bool, ''),
            "nfs": (bool, ''),
            "dashboard": (bool, '')
        }

        @APIRouter('/feature_toggles')
        @APIDoc("Manage Features API", "FeatureTogglesEndpoint")
        class FeatureTogglesEndpoint(RESTController):
            @EndpointDoc("Get List Of Features",
                         responses={200: FEATURES_SCHEMA})
            def list(_):  # pylint: disable=no-self-argument  # noqa: N805
                return {
                    # pylint: disable=protected-access
                    feature.value: self._is_feature_enabled(feature)
                    for feature in Features
                }
        return [FeatureTogglesEndpoint]
