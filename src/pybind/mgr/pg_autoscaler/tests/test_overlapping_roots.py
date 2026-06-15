# python unit test
import unittest
from tests import mock
import pytest
import json
from collections import defaultdict
from pg_autoscaler import module
from pg_autoscaler.module import CrushRootResourceStatus


class OSDMAP:
    def __init__(self, pools):
        self.pools = pools

    def get_pools(self):
        return self.pools

    def pool_raw_used_rate(pool_id):
        return 1


class CRUSH:
    def __init__(self, rules, osd_dic):
        self.rules = rules
        self.osd_dic = osd_dic

    def get_rule_by_id(self, rule_id):
        for rule in self.rules:
            if rule['rule_id'] == rule_id:
                return rule

        return None

    def get_rule_root(self, rule_name):
        for rule in self.rules:
            if rule['rule_name'] == rule_name:
                return rule['root_id']

        return None

    def get_osds_under(self, root_id):
        return self.osd_dic[root_id]


class TestPgAutoscaler(object):

    def setup_method(self):
        # a bunch of attributes for testing.
        self.autoscaler = module.PgAutoscaler('module_name', 0, 0)
        self.autoscaler.mon_target_pg_per_osd = 100

    def helper_test(self, osd_dic, rules, pools, expected_result):
        osdmap = OSDMAP(pools)
        crush = CRUSH(rules, osd_dic)
        result = self.autoscaler.get_subtree_resource_status(
            osdmap, pools, crush
        )
        for root_id in result:
            assert result[root_id].pg_target == expected_result[root_id].pg_target

    def test_subtrees_and_overlaps(self):
        osd_dic = {
            -1: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            -40: [11, 12, 13, 14, 15],
            -5: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        }

        rules = [
            {
                "rule_id": 0,
                "rule_name": "data",
                "ruleset": 0,
                "type": 1,
                "min_size": 1,
                "max_size": 10,
                "root_id": -1,
            },
            {
                "rule_id": 1,
                "rule_name": "teuthology-data-ec",
                "ruleset": 1,
                "type": 3,
                "min_size": 3,
                "max_size": 6,
                "root_id": -5,
            },
            {
                "rule_id": 4,
                "rule_name": "rep-ssd",
                "ruleset": 4,
                "type": 1,
                "min_size": 1,
                "max_size": 10,
                "root_id": -40,
            },
        ]
        pools = {
            "data": {
                "pool": 0,
                "pool_name": "data",
                "pg_num_target": 1024,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0.1624,
                "options": {
                    "pg_num_min": 1024,
                },
                "expected_final_pg_target": 1024,
            },
            "metadata": {
                "pool": 1,
                "pool_name": "metadata",
                "pg_num_target": 64,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0.0144,
                "options": {
                    "pg_num_min": 64,
                },
                "expected_final_pg_target": 64,
            },
            "libvirt-pool": {
                "pool": 4,
                "pool_name": "libvirt-pool",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0.0001,
                "options": {},
                "expected_final_pg_target": 128,
            },
            ".rgw.root": {
                "pool": 93,
                "pool_name": ".rgw.root",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "default.rgw.control": {
                "pool": 94,
                "pool_name": "default.rgw.control",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "default.rgw.meta": {
                "pool": 95,
                "pool_name": "default.rgw.meta",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "default.rgw.log": {
                "pool": 96,
                "pool_name": "default.rgw.log",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "default.rgw.buckets.index": {
                "pool": 97,
                "pool_name": "default.rgw.buckets.index",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0.0002,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "default.rgw.buckets.data": {
                "pool": 98,
                "pool_name": "default.rgw.buckets.data",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0.0457,
                "options": {},
                "expected_final_pg_target": 128,
            },
            "default.rgw.buckets.non-ec": {
                "pool": 99,
                "pool_name": "default.rgw.buckets.non-ec",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "device_health_metrics": {
                "pool": 100,
                "pool_name": "device_health_metrics",
                "pg_num_target": 1,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0,
                "options": {
                    "pg_num_min": 1
                },
                "expected_final_pg_target": 1,
            },
            "cephfs.teuthology.meta": {
                "pool": 113,
                "pool_name": "cephfs.teuthology.meta",
                "pg_num_target": 64,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0.1389,
                "options": {
                    "pg_autoscale_bias": 4,
                    "pg_num_min": 64,
                },
                "expected_final_pg_target": 512,
            },
            "cephfs.teuthology.data": {
                "pool": 114,
                "pool_name": "cephfs.teuthology.data",
                "pg_num_target": 256,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0.0006,
                "options": {
                    "pg_num_min": 128,
                },
                "expected_final_pg_target": 1024,
                "expected_final_pg_target": 256,
            },
            "cephfs.scratch.meta": {
                "pool": 117,
                "pool_name": "cephfs.scratch.meta",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0.0027,
                "options": {
                    "pg_autoscale_bias": 4,
                    "pg_num_min": 16,
                },
                "expected_final_pg_target": 64,
            },
            "cephfs.scratch.data": {
                "pool": 118,
                "pool_name": "cephfs.scratch.data",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0.0027,
                "options": {},
                "expected_final_pg_target": 128,
            },
            "cephfs.teuthology.data-ec": {
                "pool": 119,
                "pool_name": "cephfs.teuthology.data-ec",
                "pg_num_target": 1024,
                "size": 6,
                "crush_rule": 1,
                "capacity_ratio": 0.8490,
                "options": {
                    "pg_num_min": 1024
                },
                "expected_final_pg_target": 1024,
            },
            "cephsqlite": {
                "pool": 121,
                "pool_name": "cephsqlite",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 128,
            },
        }
        expected_result = defaultdict(CrushRootResourceStatus)
        for root in osd_dic:
            expected_result[root] = CrushRootResourceStatus()
        expected_result[-1].pg_target = 550
        expected_result[-40].pg_target = 250
        expected_result[-5].pg_target = 800
        self.helper_test(osd_dic, rules, pools, expected_result)

    def test_no_overlaps(self):
        osd_dic = {
            -1: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            -40: [11, 12, 13, 14, 15],
            -5: [16, 17, 18],
        }

        rules = [
            {
                "rule_id": 0,
                "rule_name": "data",
                "ruleset": 0,
                "type": 1,
                "min_size": 1,
                "max_size": 10,
                "root_id": -1,
            },
            {
                "rule_id": 1,
                "rule_name": "teuthology-data-ec",
                "ruleset": 1,
                "type": 3,
                "min_size": 3,
                "max_size": 6,
                "root_id": -5,
            },
            {
                "rule_id": 4,
                "rule_name": "rep-ssd",
                "ruleset": 4,
                "type": 1,
                "min_size": 1,
                "max_size": 10,
                "root_id": -40,
            },
        ]
        pools = {
            "data": {
                "pool": 0,
                "pool_name": "data",
                "pg_num_target": 1024,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0.1624,
                "options": {
                    "pg_num_min": 1024,
                },
                "expected_final_pg_target": 1024,
            },
            "metadata": {
                "pool": 1,
                "pool_name": "metadata",
                "pg_num_target": 64,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0.0144,
                "options": {
                    "pg_num_min": 64,
                },
                "expected_final_pg_target": 64,
            },
            "libvirt-pool": {
                "pool": 4,
                "pool_name": "libvirt-pool",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0.0001,
                "options": {},
                "expected_final_pg_target": 128,
            },
            ".rgw.root": {
                "pool": 93,
                "pool_name": ".rgw.root",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "default.rgw.control": {
                "pool": 94,
                "pool_name": "default.rgw.control",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "default.rgw.meta": {
                "pool": 95,
                "pool_name": "default.rgw.meta",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "default.rgw.log": {
                "pool": 96,
                "pool_name": "default.rgw.log",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "default.rgw.buckets.index": {
                "pool": 97,
                "pool_name": "default.rgw.buckets.index",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0.0002,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "default.rgw.buckets.data": {
                "pool": 98,
                "pool_name": "default.rgw.buckets.data",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0.0457,
                "options": {},
                "expected_final_pg_target": 128,
            },
            "default.rgw.buckets.non-ec": {
                "pool": 99,
                "pool_name": "default.rgw.buckets.non-ec",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 32,
            },
            "device_health_metrics": {
                "pool": 100,
                "pool_name": "device_health_metrics",
                "pg_num_target": 1,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0,
                "options": {
                    "pg_num_min": 1
                },
                "expected_final_pg_target": 1,
            },
            "cephfs.teuthology.meta": {
                "pool": 113,
                "pool_name": "cephfs.teuthology.meta",
                "pg_num_target": 64,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0.1389,
                "options": {
                    "pg_autoscale_bias": 4,
                    "pg_num_min": 64,
                },
                "expected_final_pg_target": 512,
            },
            "cephfs.teuthology.data": {
                "pool": 114,
                "pool_name": "cephfs.teuthology.data",
                "pg_num_target": 256,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0.0006,
                "options": {
                    "pg_num_min": 128,
                },
                "expected_final_pg_target": 1024,
                "expected_final_pg_target": 256,
            },
            "cephfs.scratch.meta": {
                "pool": 117,
                "pool_name": "cephfs.scratch.meta",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0.0027,
                "options": {
                    "pg_autoscale_bias": 4,
                    "pg_num_min": 16,
                },
                "expected_final_pg_target": 64,
            },
            "cephfs.scratch.data": {
                "pool": 118,
                "pool_name": "cephfs.scratch.data",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 0,
                "capacity_ratio": 0.0027,
                "options": {},
                "expected_final_pg_target": 128,
            },
            "cephfs.teuthology.data-ec": {
                "pool": 119,
                "pool_name": "cephfs.teuthology.data-ec",
                "pg_num_target": 1024,
                "size": 6,
                "crush_rule": 1,
                "capacity_ratio": 0.8490,
                "options": {
                    "pg_num_min": 1024
                },
                "expected_final_pg_target": 1024,
            },
            "cephsqlite": {
                "pool": 121,
                "pool_name": "cephsqlite",
                "pg_num_target": 32,
                "size": 3,
                "crush_rule": 4,
                "capacity_ratio": 0,
                "options": {},
                "expected_final_pg_target": 128,
            },
        }
        expected_result = defaultdict(CrushRootResourceStatus)
        for root in osd_dic:
            expected_result[root] = CrushRootResourceStatus()
        expected_result[-1].pg_target = 1100
        expected_result[-40].pg_target = 500
        expected_result[-5].pg_target = 300
        self.helper_test(osd_dic, rules, pools, expected_result)
