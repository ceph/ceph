# -*- coding: utf-8 -*-

import inspect


class Scope(object):
    """
    List of Dashboard Security Scopes.
    If you need another security scope, please add it here.
    """

    HOSTS = "hosts"
    CONFIG_OPT = "config-opt"
    POOL = "pool"
    OSD = "osd"
    MONITOR = "monitor"
    RBD_IMAGE = "rbd-image"
    ISCSI = "iscsi"
    RBD_MIRRORING = "rbd-mirroring"
    RGW = "rgw"
    CEPHFS = "cephfs"
    MANAGER = "manager"
    LOG = "log"
    GRAFANA = "grafana"
    PROMETHEUS = "prometheus"
    USER = "user"
    DASHBOARD_SETTINGS = "dashboard-settings"
    NFS_GANESHA = "nfs-ganesha"
    NVME_OF = "nvme-of"

    @classmethod
    def all_scopes(cls):
        return [val for scope, val in
                inspect.getmembers(cls,
                                   lambda memb: not inspect.isroutine(memb))
                if not scope.startswith('_')]

    @classmethod
    def valid_scope(cls, scope_name):
        return scope_name in cls.all_scopes()


class Permission(object):
    """
    Scope permissions types
    """
    READ = "read"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"

    @classmethod
    def all_permissions(cls):
        return [val for perm, val in
                inspect.getmembers(cls,
                                   lambda memb: not inspect.isroutine(memb))
                if not perm.startswith('_')]

    @classmethod
    def valid_permission(cls, perm_name):
        return perm_name in cls.all_permissions()
