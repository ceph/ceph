# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import (DashboardTestCase, JAny, JLeaf, JList, JObj,
                     addrvec_schema, module_options_schema)


class HealthTest(DashboardTestCase):
    CEPHFS = True

    __pg_info_schema = JObj({
        'object_stats': JObj({
            'num_objects': int,
            'num_object_copies': int,
            'num_objects_degraded': int,
            'num_objects_misplaced': int,
            'num_objects_unfound': int
        }),
        'pgs_per_osd': float,
        'statuses': JObj({}, allow_unknown=True, unknown_schema=int)
    })

    __mdsmap_schema = JObj({
        'session_autoclose': int,
        'balancer': str,
        'bal_rank_mask': str,
        'up': JObj({}, allow_unknown=True),
        'last_failure_osd_epoch': int,
        'in': JList(int),
        'last_failure': int,
        'max_file_size': int,
        'max_xattr_size': int,
        'explicitly_allowed_features': int,
        'damaged': JList(int),
        'tableserver': int,
        'failed': JList(int),
        'metadata_pool': int,
        'epoch': int,
        'stopped': JList(int),
        'max_mds': int,
        'compat': JObj({
            'compat': JObj({}, allow_unknown=True),
            'ro_compat': JObj({}, allow_unknown=True),
            'incompat': JObj({}, allow_unknown=True)
        }),
        'required_client_features': JObj({}, allow_unknown=True),
        'data_pools': JList(int),
        'info': JObj({}, allow_unknown=True),
        'fs_name': str,
        'created': str,
        'standby_count_wanted': int,
        'enabled': bool,
        'modified': str,
        'session_timeout': int,
        'flags': int,
        'flags_state': JObj({
            'joinable': bool,
            'allow_snaps': bool,
            'allow_multimds_snaps': bool,
            'allow_standby_replay': bool,
            'refuse_client_session': bool,
            'refuse_standby_for_another_fs': bool,
            'balance_automate': bool,
        }),
        'ever_allowed_features': int,
        'root': int,
        'qdb_leader': int,
        'qdb_cluster': JList(int)
    })

    def test_minimal_health(self):
        data = self._get('/api/health/minimal')
        self.assertStatus(200)
        schema = JObj({
            'client_perf': JObj({
                'read_bytes_sec': int,
                'read_op_per_sec': int,
                'recovering_bytes_per_sec': int,
                'write_bytes_sec': int,
                'write_op_per_sec': int
            }),
            'df': JObj({
                'stats': JObj({
                    'total_avail_bytes': int,
                    'total_bytes': int,
                    'total_used_raw_bytes': int,
                })
            }),
            'fs_map': JObj({
                'filesystems': JList(
                    JObj({
                        'mdsmap': self.__mdsmap_schema
                    }),
                ),
                'standbys': JList(JObj({}, allow_unknown=True)),
            }),
            'health': JObj({
                'checks': JList(JObj({}, allow_unknown=True)),
                'mutes': JList(JObj({}, allow_unknown=True)),
                'status': str,
            }),
            'hosts': int,
            'iscsi_daemons': JObj({
                'up': int,
                'down': int
            }),
            'mgr_map': JObj({
                'active_name': str,
                'standbys': JList(JLeaf(dict))
            }),
            'mon_status': JObj({
                'monmap': JObj({
                    'mons': JList(JLeaf(dict)),
                }),
                'quorum': JList(int)
            }),
            'osd_map': JObj({
                'osds': JList(
                    JObj({
                        'in': int,
                        'up': int,
                        'state': JList(str)
                    })),
            }),
            'pg_info': self.__pg_info_schema,
            'pools': JList(JLeaf(dict)),
            'rgw': int,
            'scrub_status': str
        })
        self.assertSchema(data, schema)

    def test_full_health(self):
        data = self._get('/api/health/full')
        self.assertStatus(200)
        module_info_schema = JObj({
            'can_run': bool,
            'error_string': str,
            'name': str,
            'module_options': module_options_schema
        })
        schema = JObj({
            'client_perf': JObj({
                'read_bytes_sec': int,
                'read_op_per_sec': int,
                'recovering_bytes_per_sec': int,
                'write_bytes_sec': int,
                'write_op_per_sec': int
            }),
            'df': JObj({
                'pools': JList(JObj({
                    'stats': JObj({
                        'stored': int,
                        'stored_data': int,
                        'stored_omap': int,
                        'objects': int,
                        'kb_used': int,
                        'bytes_used': int,
                        'data_bytes_used': int,
                        'omap_bytes_used': int,
                        'percent_used': float,
                        'max_avail': int,
                        'quota_objects': int,
                        'quota_bytes': int,
                        'dirty': int,
                        'rd': int,
                        'rd_bytes': int,
                        'wr': int,
                        'wr_bytes': int,
                        'compress_bytes_used': int,
                        'compress_under_bytes': int,
                        'stored_raw': int,
                        'avail_raw': int
                    }),
                    'name': str,
                    'id': int
                })),
                'stats': JObj({
                    'total_avail_bytes': int,
                    'total_bytes': int,
                    'total_used_bytes': int,
                    'total_used_raw_bytes': int,
                    'total_used_raw_ratio': float,
                    'num_osds': int,
                    'num_per_pool_osds': int,
                    'num_per_pool_omap_osds': int
                })
            }),
            'fs_map': JObj({
                'btime': str,
                'compat': JObj({
                    'compat': JObj({}, allow_unknown=True, unknown_schema=str),
                    'incompat': JObj(
                        {}, allow_unknown=True, unknown_schema=str),
                    'ro_compat': JObj(
                        {}, allow_unknown=True, unknown_schema=str)
                }),
                'default_fscid': int,
                'epoch': int,
                'feature_flags': JObj(
                    {}, allow_unknown=True, unknown_schema=bool),
                'filesystems': JList(
                    JObj({
                        'id': int,
                        'mdsmap': self.__mdsmap_schema
                    }),
                ),
                'standbys': JList(JObj({}, allow_unknown=True)),
            }),
            'health': JObj({
                'checks': JList(JObj({}, allow_unknown=True)),
                'mutes': JList(JObj({}, allow_unknown=True)),
                'status': str,
            }),
            'hosts': int,
            'iscsi_daemons': JObj({
                'up': int,
                'down': int
            }),
            'mgr_map': JObj({
                'active_addr': str,
                'active_addrs': JObj({
                    'addrvec': addrvec_schema
                }),
                'active_change': str,  # timestamp
                'active_mgr_features': int,
                'active_gid': int,
                'active_name': str,
                'always_on_modules': JObj({}, allow_unknown=True),
                'available': bool,
                'available_modules': JList(module_info_schema),
                'epoch': int,
                'modules': JList(str),
                'services': JObj(
                    {'dashboard': str},  # This module should always be present
                    allow_unknown=True, unknown_schema=str
                ),
                'standbys': JList(JObj({
                    'available_modules': JList(module_info_schema),
                    'gid': int,
                    'name': str,
                    'mgr_features': int
                }, allow_unknown=True))
            }, allow_unknown=True),
            'mon_status': JObj({
                'election_epoch': int,
                'extra_probe_peers': JList(JAny(none=True)),
                'feature_map': JObj(
                    {}, allow_unknown=True, unknown_schema=JList(JObj({
                        'features': str,
                        'num': int,
                        'release': str
                    }))
                ),
                'features': JObj({
                    'quorum_con': str,
                    'quorum_mon': JList(str),
                    'required_con': str,
                    'required_mon': JList(str)
                }),
                'monmap': JObj({
                    # @TODO: expand on monmap schema
                    'mons': JList(JLeaf(dict)),
                }, allow_unknown=True),
                'name': str,
                'outside_quorum': JList(int),
                'quorum': JList(int),
                'quorum_age': int,
                'rank': int,
                'state': str,
                # @TODO: What type should be expected here?
                'sync_provider': JList(JAny(none=True)),
                'stretch_mode': bool,
                'uptime': int,
            }),
            'osd_map': JObj({
                # @TODO: define schema for crush map and osd_metadata, among
                # others
                'osds': JList(
                    JObj({
                        'in': int,
                        'up': int,
                    }, allow_unknown=True)),
            }, allow_unknown=True),
            'pg_info': self.__pg_info_schema,
            'pools': JList(JLeaf(dict)),
            'rgw': int,
            'scrub_status': str
        })
        self.assertSchema(data, schema)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data['pools']))
        for pool in data['pools']:
            self.assertIn(pool['pool_name'], cluster_pools)

    @DashboardTestCase.RunAs('test', 'test', ['pool-manager'])
    def test_health_permissions(self):
        data = self._get('/api/health/full')
        self.assertStatus(200)

        schema = JObj({
            'client_perf': JObj({}, allow_unknown=True),
            'df': JObj({}, allow_unknown=True),
            'health': JObj({
                'checks': JList(JObj({}, allow_unknown=True)),
                'mutes': JList(JObj({}, allow_unknown=True)),
                'status': str
            }),
            'pools': JList(JLeaf(dict)),
        })
        self.assertSchema(data, schema)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data['pools']))
        for pool in data['pools']:
            self.assertIn(pool['pool_name'], cluster_pools)
