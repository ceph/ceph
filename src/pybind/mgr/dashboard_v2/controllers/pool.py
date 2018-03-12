# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ..services.ceph_service import CephService
from ..tools import ApiController, RESTController, AuthRequired


@ApiController('pool')
@AuthRequired()
class Pool(RESTController):

    @classmethod
    def _serialize_pool(cls, pool, attrs):
        if not attrs or not isinstance(attrs, list):
            return pool

        res = {}
        for attr in attrs:
            if attr not in pool:
                continue
            if attr == 'type':
                res[attr] = {1: 'replicated', 3: 'erasure'}[pool[attr]]
            else:
                res[attr] = pool[attr]

        # pool_name is mandatory
        res['pool_name'] = pool['pool_name']
        return res

    @staticmethod
    def _str_to_bool(var):
        if isinstance(var, bool):
            return var
        return var.lower() in ("true", "yes", "1", 1)

    def list(self, attrs=None, stats=False):
        if attrs:
            attrs = attrs.split(',')

        if self._str_to_bool(stats):
            pools = CephService.get_pool_list_with_stats()
        else:
            pools = CephService.get_pool_list()

        return [self._serialize_pool(pool, attrs) for pool in pools]

    def get(self, pool_name, attrs=None, stats=False):
        pools = self.list(attrs, stats)
        return [pool for pool in pools if pool['pool_name'] == pool_name][0]
