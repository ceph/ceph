# -*- coding: utf-8 -*-

import math
import time
from typing import Any, Dict, Iterable, List, Optional, Union, cast

import cherrypy

from .. import mgr
from ..security import Scope
from ..services.ceph_service import CephService
from ..services.exception import handle_send_command_error
from ..services.rbd import RbdConfiguration
from ..tools import TaskManager, str_to_bool
from . import APIDoc, APIRouter, Endpoint, EndpointDoc, ReadPermission, \
    RESTController, Task, UIRouter
from .rbd_mirroring import RbdMirroringPoolMode

POOL_SCHEMA = ([{
    "pool": (int, "pool id"),
    "pool_name": (str, "pool name"),
    "flags": (int, ""),
    "flags_names": (str, "flags name"),
    "type": (str, "type of pool"),
    "size": (int, "pool size"),
    "min_size": (int, ""),
    "crush_rule": (str, ""),
    "object_hash": (int, ""),
    "pg_autoscale_mode": (str, ""),
    "pg_num": (int, ""),
    "pg_placement_num": (int, ""),
    "pg_placement_num_target": (int, ""),
    "pg_num_target": (int, ""),
    "pg_num_pending": (int, ""),
    "last_pg_merge_meta": ({
        "ready_epoch": (int, ""),
        "last_epoch_started": (int, ""),
        "last_epoch_clean": (int, ""),
        "source_pgid": (str, ""),
        "source_version": (str, ""),
        "target_version": (str, ""),
    }, ""),
    "auid": (int, ""),
    "snap_mode": (str, ""),
    "snap_seq": (int, ""),
    "snap_epoch": (int, ""),
    "pool_snaps": ([str], ""),
    "quota_max_bytes": (int, ""),
    "quota_max_objects": (int, ""),
    "tiers": ([str], ""),
    "tier_of": (int, ""),
    "read_tier": (int, ""),
    "write_tier": (int, ""),
    "cache_mode": (str, ""),
    "target_max_bytes": (int, ""),
    "target_max_objects": (int, ""),
    "cache_target_dirty_ratio_micro": (int, ""),
    "cache_target_dirty_high_ratio_micro": (int, ""),
    "cache_target_full_ratio_micro": (int, ""),
    "cache_min_flush_age": (int, ""),
    "cache_min_evict_age": (int, ""),
    "erasure_code_profile": (str, ""),
    "hit_set_params": ({
        "type": (str, "")
    }, ""),
    "hit_set_period": (int, ""),
    "hit_set_count": (int, ""),
    "use_gmt_hitset": (bool, ""),
    "min_read_recency_for_promote": (int, ""),
    "min_write_recency_for_promote": (int, ""),
    "hit_set_grade_decay_rate": (int, ""),
    "hit_set_search_last_n": (int, ""),
    "grade_table": ([str], ""),
    "stripe_width": (int, ""),
    "expected_num_objects": (int, ""),
    "fast_read": (bool, ""),
    "options": ({
        "pg_num_min": (int, ""),
        "pg_num_max": (int, "")
    }, ""),
    "application_metadata": ([str], ""),
    "create_time": (str, ""),
    "last_change": (str, ""),
    "last_force_op_resend": (str, ""),
    "last_force_op_resend_prenautilus": (str, ""),
    "last_force_op_resend_preluminous": (str, ""),
    "removed_snaps": ([str], "")
}])


def pool_task(name, metadata, wait_for=2.0):
    return Task("pool/{}".format(name), metadata, wait_for)


@APIRouter('/pool', Scope.POOL)
@APIDoc("Get pool details by pool name", "Pool")
class Pool(RESTController):

    @staticmethod
    def _serialize_pool(pool, attrs):
        if not attrs or not isinstance(attrs, list):
            attrs = pool.keys()

        crush_rules = {r['rule_id']: r["rule_name"] for r in mgr.get('osd_map_crush')['rules']}

        res: Dict[Union[int, str], Union[str, List[Any], Dict[str, Any]]] = {}
        for attr in attrs:
            if attr not in pool:
                continue
            if attr == 'type':
                res[attr] = {1: 'replicated', 3: 'erasure'}[pool[attr]]
            elif attr == 'crush_rule':
                res[attr] = crush_rules[pool[attr]]
            elif attr == 'application_metadata':
                res[attr] = list(pool[attr].keys())
            # handle infinity values
            elif attr == 'read_balance' and isinstance(pool[attr], dict):
                read_balance: Dict[str, Any] = {}
                for key, value in pool[attr].items():
                    if isinstance(value, float) and math.isinf(value):
                        read_balance[key] = "Infinity"
                    else:
                        read_balance[key] = value
                res[attr] = read_balance
            else:
                res[attr] = pool[attr]

        # pool_name is mandatory
        res['pool_name'] = pool['pool_name']
        return res

    @classmethod
    def _pool_list(cls, attrs=None, stats=False):
        if attrs:
            attrs = attrs.split(',')

        if str_to_bool(stats):
            pools = CephService.get_pool_list_with_stats()
        else:
            pools = CephService.get_pool_list()

        return [cls._serialize_pool(pool, attrs) for pool in pools]

    @EndpointDoc("Display Pool List",
                 parameters={
                     'attrs': (str, 'Pool Attributes'),
                     'stats': (bool, 'Pool Stats')
                 },
                 responses={200: POOL_SCHEMA})
    def list(self, attrs=None, stats=False):
        return self._pool_list(attrs, stats)

    @classmethod
    def _get(cls, pool_name: str, attrs: Optional[str] = None, stats: bool = False) -> dict:
        pools = cls._pool_list(attrs, stats)
        pool = [p for p in pools if p['pool_name'] == pool_name]
        if not pool:
            raise cherrypy.NotFound('No such pool')
        return pool[0]

    def get(self, pool_name: str, attrs: Optional[str] = None, stats: bool = False) -> dict:
        pool = self._get(pool_name, attrs, stats)
        pool['configuration'] = RbdConfiguration(pool_name).list()
        return pool

    @pool_task('delete', ['{pool_name}'])
    @handle_send_command_error('pool')
    def delete(self, pool_name):
        return CephService.send_command('mon', 'osd pool delete', pool=pool_name, pool2=pool_name,
                                        yes_i_really_really_mean_it=True)

    @pool_task('edit', ['{pool_name}'])
    def set(self, pool_name, flags=None, application_metadata=None, configuration=None,
            rbd_mirroring=None, **kwargs):
        self._set_pool_values(pool_name, application_metadata, flags, True, kwargs)
        if kwargs.get('pool'):
            pool_name = kwargs['pool']
        RbdConfiguration(pool_name).set_configuration(configuration)
        if rbd_mirroring is not None:
            self._set_mirroring_mode(rbd_mirroring, pool_name)
        self._wait_for_pgs(pool_name)

    @pool_task('create', {'pool_name': '{pool}'})
    @handle_send_command_error('pool')
    def create(self, pool, pg_num, pool_type, erasure_code_profile=None, flags=None,
               application_metadata=None, rule_name=None, configuration=None,
               rbd_mirroring=None, **kwargs):
        ecp = erasure_code_profile if erasure_code_profile else None
        CephService.send_command('mon', 'osd pool create', pool=pool, pg_num=int(pg_num),
                                 pgp_num=int(pg_num), pool_type=pool_type, erasure_code_profile=ecp,
                                 rule=rule_name)
        self._set_pool_values(pool, application_metadata, flags, False, kwargs)
        RbdConfiguration(pool).set_configuration(configuration)
        if rbd_mirroring is not None:
            self._set_mirroring_mode(rbd_mirroring, pool)
        self._wait_for_pgs(pool)

    def _set_mirroring_mode(self, mirroring_enabled, pool):
        rbd_mirroring = RbdMirroringPoolMode()
        if str_to_bool(mirroring_enabled):
            rbd_mirroring.set_pool_mirror_mode(pool, 'pool')
        else:
            rbd_mirroring.set_pool_mirror_mode(pool, 'disabled')

    def _set_pool_values(self, pool, application_metadata, flags, update_existing, kwargs):
        current_pool = self._get(pool)
        if update_existing and kwargs.get('compression_mode') == 'unset':
            self._prepare_compression_removal(current_pool.get('options'), kwargs)
        if flags and 'ec_overwrites' in flags:
            CephService.send_command('mon', 'osd pool set', pool=pool, var='allow_ec_overwrites',
                                     val='true')
        if application_metadata is not None:
            def set_app(app_metadata, set_app_what):
                for app in app_metadata:
                    CephService.send_command('mon', 'osd pool application ' + set_app_what,
                                             pool=pool, app=app, yes_i_really_mean_it=True)

            if update_existing:
                original_app_metadata = set(
                    cast(Iterable[Any], current_pool.get('application_metadata')))
            else:
                original_app_metadata = set()

            set_app(original_app_metadata - set(application_metadata), 'disable')
            set_app(set(application_metadata) - original_app_metadata, 'enable')

        quotas = {}
        quotas['max_objects'] = kwargs.pop('quota_max_objects', None)
        quotas['max_bytes'] = kwargs.pop('quota_max_bytes', None)
        self._set_quotas(pool, quotas)
        self._set_pool_keys(pool, kwargs)

    def _set_pool_keys(self, pool, pool_items):
        def set_key(key, value):
            CephService.send_command('mon', 'osd pool set', pool=pool, var=key, val=str(value))

        update_name = False
        for key, value in pool_items.items():
            if key == 'pool':
                update_name = True
                destpool = value
            else:
                set_key(key, value)
                if key == 'pg_num':
                    set_key('pgp_num', value)
        if update_name:
            CephService.send_command('mon', 'osd pool rename', srcpool=pool, destpool=destpool)

    def _set_quotas(self, pool, quotas):
        for field, value in quotas.items():
            if value is not None:
                CephService.send_command('mon', 'osd pool set-quota',
                                         pool=pool, field=field, val=str(value))

    def _prepare_compression_removal(self, options, kwargs):
        """
        Presets payload with values to remove compression attributes in case they are not
        needed anymore.

        In case compression is not needed the dashboard will send 'compression_mode' with the
        value 'unset'.

        :param options: All set options for the current pool.
        :param kwargs: Payload of the PUT / POST call
        """
        if options is not None:
            def reset_arg(arg, value):
                if options.get(arg):
                    kwargs[arg] = value
            for arg in ['compression_min_blob_size', 'compression_max_blob_size',
                        'compression_required_ratio']:
                reset_arg(arg, '0')
            reset_arg('compression_algorithm', 'unset')

    @classmethod
    def _wait_for_pgs(cls, pool_name):
        """
        Keep the task waiting for until all pg changes are complete
        :param pool_name: The name of the pool.
        :type pool_name: string
        """
        current_pool = cls._get(pool_name)
        initial_pgs = int(current_pool['pg_placement_num']) + int(current_pool['pg_num'])
        cls._pg_wait_loop(current_pool, initial_pgs)

    @classmethod
    def _pg_wait_loop(cls, pool, initial_pgs):
        """
        Compares if all pg changes are completed, if not it will call itself
        until all changes are completed.
        :param pool: The dict that represents a pool.
        :type pool: dict
        :param initial_pgs: The pg and pg_num count before any change happened.
        :type initial_pgs: int
        """
        if 'pg_num_target' in pool:
            target = int(pool['pg_num_target']) + int(pool['pg_placement_num_target'])
            current = int(pool['pg_placement_num']) + int(pool['pg_num'])
            if current != target:
                max_diff = abs(target - initial_pgs)
                diff = max_diff - abs(target - current)
                percentage = int(round(diff / float(max_diff) * 100))
                TaskManager.current_task().set_progress(percentage)
                time.sleep(4)
                cls._pg_wait_loop(cls._get(pool['pool_name']), initial_pgs)

    @RESTController.Resource()
    @ReadPermission
    def configuration(self, pool_name):
        return RbdConfiguration(pool_name).list()


@UIRouter('/pool', Scope.POOL)
@APIDoc("Dashboard UI helper function; not part of the public API", "PoolUi")
class PoolUi(Pool):
    @Endpoint()
    @ReadPermission
    def info(self):
        """Used by the create-pool dialog"""
        osd_map_crush = mgr.get('osd_map_crush')
        options = mgr.get('config_options')['options']

        def rules(pool_type):
            return [r
                    for r in osd_map_crush['rules']
                    if r['type'] == pool_type]

        def all_bluestore():
            return all(o['osd_objectstore'] == 'bluestore'
                       for o in mgr.get('osd_metadata').values())

        def get_config_option_enum(conf_name):
            return [[v for v in o['enum_values'] if len(v) > 0]
                    for o in options
                    if o['name'] == conf_name][0]

        profiles = CephService.get_erasure_code_profiles()
        used_rules: Dict[str, List[str]] = {}
        used_profiles: Dict[str, List[str]] = {}
        pool_names = []
        for p in self._pool_list():
            name = p['pool_name']
            pool_names.append(name)
            rule = p['crush_rule']
            if rule in used_rules:
                used_rules[rule].append(name)
            else:
                used_rules[rule] = [name]
            profile = p['erasure_code_profile']
            if profile in used_profiles:
                used_profiles[profile].append(name)
            else:
                used_profiles[profile] = [name]

        mgr_config = mgr.get('config')
        return {
            "pool_names": pool_names,
            "crush_rules_replicated": rules(1),
            "crush_rules_erasure": rules(3),
            "is_all_bluestore": all_bluestore(),
            "osd_count": len(mgr.get('osd_map')['osds']),
            "bluestore_compression_algorithm": mgr_config['bluestore_compression_algorithm'],
            "compression_algorithms": get_config_option_enum('bluestore_compression_algorithm'),
            "compression_modes": get_config_option_enum('bluestore_compression_mode'),
            "pg_autoscale_default_mode": mgr_config['osd_pool_default_pg_autoscale_mode'],
            "pg_autoscale_modes": get_config_option_enum('osd_pool_default_pg_autoscale_mode'),
            "erasure_code_profiles": profiles,
            "used_rules": used_rules,
            "used_profiles": used_profiles,
            'nodes': mgr.get('osd_map_tree')['nodes']
        }


class RBDPool(Pool):
    def create(self, pool='rbd-mirror'):  # pylint: disable=arguments-differ
        super().create(pool, pg_num=1, pool_type='replicated',
                       rule_name='replicated_rule', application_metadata=['rbd'])
