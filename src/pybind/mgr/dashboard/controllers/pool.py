# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import ApiController, RESTController, AuthRequired
from .. import mgr
from ..services.ceph_service import CephService


@ApiController('pool')
@AuthRequired()
class Pool(RESTController):

    @classmethod
    def _serialize_pool(cls, pool, attrs):
        if not attrs or not isinstance(attrs, list):
            attrs = pool.keys()

        crush_rules = {r['rule_id']: r["rule_name"] for r in mgr.get('osd_map_crush')['rules']}

        res = {}
        for attr in attrs:
            if attr not in pool:
                continue
            if attr == 'type':
                res[attr] = {1: 'replicated', 3: 'erasure'}[pool[attr]]
            elif attr == 'crush_rule':
                res[attr] = crush_rules[pool[attr]]
            elif attr == 'application_metadata':
                res[attr] = list(pool[attr].keys())
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

    @cherrypy.tools.json_out()
    @cherrypy.expose
    def _info(self):
        """Used by the create-pool dialog"""
        def rules(pool_type):
            return [r
                    for r in mgr.get('osd_map_crush')['rules']
                    if r['type'] == pool_type]

        def all_bluestore():
            return all(o['osd_objectstore'] == 'bluestore'
                       for o in mgr.get('osd_metadata').values())

        def compression_enum(conf_name):
            return [o['enum_values'] for o in mgr.get('config_options')['options']
                    if o['name'] == conf_name][0]

        return {
            "pool_names": [p['pool_name'] for p in self.list()],
            "crush_rules_replicated": rules(1),
            "crush_rules_erasure": rules(3),
            "is_all_bluestore": all_bluestore(),
            "osd_count": len(mgr.get('osd_map')['osds']),
            "compression_algorithms": compression_enum('bluestore_compression_algorithm'),
            "compression_modes": compression_enum('bluestore_compression_mode'),
        }
