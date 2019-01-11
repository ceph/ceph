# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase, JAny, JLeaf, JList, JObj


class HealthTest(DashboardTestCase):
    CEPHFS = True

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
                    'total_objects': int,
                    'total_used_raw_bytes': int,
                })
            }),
            'fs_map': JObj({
                'filesystems': JList(
                    JObj({
                        'mdsmap': JObj({
                            'info': JObj(
                                {},
                                allow_unknown=True,
                                unknown_schema=JObj({
                                    'state': str
                                })
                            )
                        })
                    }),
                ),
                'standbys': JList(JObj({})),
            }),
            'health': JObj({
                'checks': JList(str),
                'status': str,
            }),
            'hosts': int,
            'iscsi_daemons': int,
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
                    })),
            }),
            'pg_info': JObj({
                'pgs_per_osd': float,
                'statuses': JObj({}, allow_unknown=True, unknown_schema=int)
            }),
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
            'module_options': JObj(
                {},
                allow_unknown=True,
                unknown_schema=JObj({
                    'name': str,
                    'type': str,
                    'level': str,
                    'flags': int,
                    'default_value': str,
                    'min': str,
                    'max': str,
                    'enum_allowed': JList(str),
                    'see_also': JList(str),
                    'desc': str,
                    'long_desc': str,
                    'tags': JList(str),
                })),
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
                        'objects': int,
                        'kb_used': int,
                        'bytes_used': int,
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
                        'stored_raw': int
                    }),
                    'name': str,
                    'id': int
                })),
                'stats': JObj({
                    'total_avail_bytes': int,
                    'total_bytes': int,
                    'total_objects': int,
                    'total_used_bytes': int,
                    'total_used_raw_bytes': int,
                    'total_used_raw_ratio': float
                })
            }),
            'fs_map': JObj({
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
                        'mdsmap': JObj({
                            # TODO: Expand mdsmap schema
                            'info': JObj(
                                {},
                                allow_unknown=True,
                                unknown_schema=JObj({
                                    'state': str
                                }, allow_unknown=True)
                            )
                        }, allow_unknown=True)
                    }),
                ),
                'standbys': JList(JObj({}, allow_unknown=True)),
            }),
            'health': JObj({
                'checks': JList(str),
                'status': str,
            }),
            'hosts': int,
            'iscsi_daemons': int,
            'mgr_map': JObj({
                'active_addr': str,
                'active_addrs': JObj({
                    'addrvec': JList(JObj({
                        'addr': str,
                        'nonce': int,
                        'type': str
                    }))
                }),
                'active_change': str,  # timestamp
                'active_gid': int,
                'active_name': str,
                'always_on_modules': JObj(
                    {},
                    allow_unknown=True, unknown_schema=JList(str)
                ),
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
                    'name': str
                }))
            }),
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
                    # TODO: expand on monmap schema
                    'mons': JList(JLeaf(dict)),
                }, allow_unknown=True),
                'name': str,
                'outside_quorum': JList(int),
                'quorum': JList(int),
                'quorum_age': int,
                'rank': int,
                'state': str,
                # TODO: What type should be expected here?
                'sync_provider': JList(JAny(none=True))
            }),
            'osd_map': JObj({
                # TODO: define schema for crush map and osd_metadata, among
                # others
                'osds': JList(
                    JObj({
                        'in': int,
                        'up': int,
                    }, allow_unknown=True)),
            }, allow_unknown=True),
            'pg_info': JObj({
                'pgs_per_osd': float,
                'statuses': JObj({}, allow_unknown=True, unknown_schema=int)
            }),
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
                'checks': JList(str),
                'status': str
            }),
            'pools': JList(JLeaf(dict)),
        })
        self.assertSchema(data, schema)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data['pools']))
        for pool in data['pools']:
            self.assertIn(pool['pool_name'], cluster_pools)
