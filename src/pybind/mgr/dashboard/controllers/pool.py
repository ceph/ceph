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

    def delete(self, pool_name):
        return CephService.send_command('mon', 'osd pool delete', pool=pool_name, pool2=pool_name,
                                        sure='--yes-i-really-really-mean-it')

    # pylint: disable=too-many-arguments, too-many-locals
    @RESTController.args_from_json
    def create(self, pool, pg_num, pool_type, erasure_code_profile=None, flags=None,
               application_metadata=None, rule_name=None, **kwargs):
        ecp = erasure_code_profile if erasure_code_profile else None
        CephService.send_command('mon', 'osd pool create', pool=pool, pg_num=int(pg_num),
                                 pgp_num=int(pg_num), pool_type=pool_type, erasure_code_profile=ecp,
                                 rule=rule_name)

        if flags and 'ec_overwrites' in flags:
            CephService.send_command('mon', 'osd pool set', pool=pool, var='allow_ec_overwrites',
                                     val='true')

        if application_metadata:
            for app in application_metadata.split(','):
                CephService.send_command('mon', 'osd pool application enable', pool=pool, app=app)

        for key, value in kwargs.items():
            CephService.send_command('mon', 'osd pool set', pool=pool, var=key, val=value)
