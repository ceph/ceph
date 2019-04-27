# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import ApiController, RESTController, Endpoint, ReadPermission, Task
from .. import mgr
from ..security import Scope
from ..services.ceph_service import CephService
from ..services.rbd import RbdConfiguration
from ..services.exception import handle_send_command_error
from ..tools import str_to_bool


def pool_task(name, metadata, wait_for=2.0):
    return Task("pool/{}".format(name), metadata, wait_for)


@ApiController('/pool', Scope.POOL)
class Pool(RESTController):

    @staticmethod
    def _serialize_pool(pool, attrs):
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

    def _pool_list(self, attrs=None, stats=False):
        if attrs:
            attrs = attrs.split(',')

        if str_to_bool(stats):
            pools = CephService.get_pool_list_with_stats()
        else:
            pools = CephService.get_pool_list()

        return [self._serialize_pool(pool, attrs) for pool in pools]

    def list(self, attrs=None, stats=False):
        return self._pool_list(attrs, stats)

    def _get(self, pool_name, attrs=None, stats=False):
        # type: (str, str, bool) -> dict
        pools = self._pool_list(attrs, stats)
        pool = [pool for pool in pools if pool['pool_name'] == pool_name]
        if not pool:
            raise cherrypy.NotFound('No such pool')
        return pool[0]

    def get(self, pool_name, attrs=None, stats=False):
        # type: (str, str, bool) -> dict
        pool = self._get(pool_name, attrs, stats)
        pool['configuration'] = RbdConfiguration(pool_name).list()
        return pool

    @pool_task('delete', ['{pool_name}'])
    @handle_send_command_error('pool')
    def delete(self, pool_name):
        return CephService.send_command('mon', 'osd pool delete', pool=pool_name, pool2=pool_name,
                                        yes_i_really_really_mean_it=True)

    @pool_task('edit', ['{pool_name}'])
    def set(self, pool_name, flags=None, application_metadata=None, configuration=None, **kwargs):
        self._set_pool_values(pool_name, application_metadata, flags, True, kwargs)
        RbdConfiguration(pool_name).set_configuration(configuration)

    @pool_task('create', {'pool_name': '{pool}'})
    @handle_send_command_error('pool')
    def create(self, pool, pg_num, pool_type, erasure_code_profile=None, flags=None,
               application_metadata=None, rule_name=None, configuration=None, **kwargs):
        ecp = erasure_code_profile if erasure_code_profile else None
        CephService.send_command('mon', 'osd pool create', pool=pool, pg_num=int(pg_num),
                                 pgp_num=int(pg_num), pool_type=pool_type, erasure_code_profile=ecp,
                                 rule=rule_name)
        self._set_pool_values(pool, application_metadata, flags, False, kwargs)
        RbdConfiguration(pool).set_configuration(configuration)

    def _set_pool_values(self, pool, application_metadata, flags, update_existing, kwargs):
        update_name = False
        if update_existing:
            current_pool = self._get(pool)
            self._handle_update_compression_args(current_pool.get('options'), kwargs)
        if flags and 'ec_overwrites' in flags:
            CephService.send_command('mon', 'osd pool set', pool=pool, var='allow_ec_overwrites',
                                     val='true')
        if application_metadata is not None:
            def set_app(what, app):
                CephService.send_command('mon', 'osd pool application ' + what, pool=pool, app=app,
                                         yes_i_really_mean_it=True)
            if update_existing:
                original_app_metadata = set(
                    current_pool.get('application_metadata'))
            else:
                original_app_metadata = set()

            for app in original_app_metadata - set(application_metadata):
                set_app('disable', app)
            for app in set(application_metadata) - original_app_metadata:
                set_app('enable', app)

        def set_key(key, value):
            CephService.send_command('mon', 'osd pool set', pool=pool, var=key, val=str(value))

        for key, value in kwargs.items():
            if key == 'pool':
                update_name = True
                destpool = value
            else:
                set_key(key, value)
                if key == 'pg_num':
                    set_key('pgp_num', value)
        if update_name:
            CephService.send_command('mon', 'osd pool rename', srcpool=pool, destpool=destpool)

    def _handle_update_compression_args(self, options, kwargs):
        if kwargs.get('compression_mode') == 'unset' and options is not None:
            def reset_arg(arg, value):
                if options.get(arg):
                    kwargs[arg] = value
            for arg in ['compression_min_blob_size', 'compression_max_blob_size',
                        'compression_required_ratio']:
                reset_arg(arg, '0')
            reset_arg('compression_algorithm', 'unset')

    @RESTController.Resource()
    @ReadPermission
    def configuration(self, pool_name):
        return RbdConfiguration(pool_name).list()

    @Endpoint()
    @ReadPermission
    def _info(self, pool_name=''):
        # type: (str) -> dict
        """Used by the create-pool dialog"""

        def rules(pool_type):
            return [r
                    for r in mgr.get('osd_map_crush')['rules']
                    if r['type'] == pool_type]

        def all_bluestore():
            return all(o['osd_objectstore'] == 'bluestore'
                       for o in mgr.get('osd_metadata').values())

        def compression_enum(conf_name):
            return [[v for v in o['enum_values'] if len(v) > 0]
                    for o in mgr.get('config_options')['options']
                    if o['name'] == conf_name][0]

        result = {
            "pool_names": [p['pool_name'] for p in self._pool_list()],
            "crush_rules_replicated": rules(1),
            "crush_rules_erasure": rules(3),
            "is_all_bluestore": all_bluestore(),
            "osd_count": len(mgr.get('osd_map')['osds']),
            "bluestore_compression_algorithm": mgr.get('config')['bluestore_compression_algorithm'],
            "compression_algorithms": compression_enum('bluestore_compression_algorithm'),
            "compression_modes": compression_enum('bluestore_compression_mode'),
        }

        if pool_name:
            result['pool_options'] = RbdConfiguration(pool_name).list()

        return result
