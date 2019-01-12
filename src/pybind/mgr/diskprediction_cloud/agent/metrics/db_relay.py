from __future__ import absolute_import

import re
import socket

from . import MetricsAgent, MetricsField
from ...common.clusterdata import ClusterAPI
from ...common.cypher import CypherOP, NodeInfo


class BaseDP(object):
    """ basic diskprediction structure """
    _fields = []

    def __init__(self, *args, **kwargs):
        if len(args) > len(self._fields):
            raise TypeError('Expected {} arguments'.format(len(self._fields)))

        for name, value in zip(self._fields, args):
            setattr(self, name, value)

        for name in self._fields[len(args):]:
            setattr(self, name, kwargs.pop(name))

        if kwargs:
            raise TypeError('Invalid argument(s): {}'.format(','.join(kwargs)))


class MGRDpCeph(BaseDP):
    _fields = [
        'fsid', 'health', 'max_osd', 'size',
        'avail_size', 'raw_used', 'raw_used_percent'
    ]


class MGRDpHost(BaseDP):
    _fields = ['fsid', 'host', 'ipaddr']


class MGRDpMon(BaseDP):
    _fields = ['fsid', 'host', 'ipaddr']


class MGRDpOsd(BaseDP):
    _fields = [
        'fsid', 'host', '_id', 'uuid', 'up', '_in', 'weight', 'public_addr',
        'cluster_addr', 'state', 'ceph_release', 'osd_devices', 'rotational'
    ]


class MGRDpMds(BaseDP):
    _fields = ['fsid', 'host', 'ipaddr']


class MGRDpPool(BaseDP):
    _fields = [
        'fsid', 'size', 'pool_name', 'pool_id', 'type', 'min_size',
        'pg_num', 'pgp_num', 'created_time', 'pgids', 'osd_ids', 'tiers', 'cache_mode',
        'erasure_code_profile', 'tier_of'
    ]


class MGRDpRBD(BaseDP):
    _fields = ['fsid', '_id', 'name', 'pool_name', 'pool_id']


class MGRDpFS(BaseDP):
    _fields = ['fsid', '_id', 'name', 'metadata_pool', 'data_pools', 'mds_nodes']


class MGRDpPG(BaseDP):
    _fields = [
        'fsid', 'pgid', 'up_osds', 'acting_osds', 'state',
        'objects', 'degraded', 'misplaced', 'unfound'
    ]


class MGRDpDisk(BaseDP):
    _fields = ['host_domain_id', 'host', 'fs_journal_osd', 'bs_db_osd', 'bs_wal_osd', 'data_osd', 'osd_ids']


class DBRelay(MetricsField):
    """ DB Relay structure """
    measurement = 'db_relay'

    def __init__(self):
        super(DBRelay, self).__init__()
        self.fields['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.tags['dc_tag'] = 'na'
        self.tags['host'] = None
        self.fields['cmd'] = None


class DBRelayAgent(MetricsAgent):
    measurement = 'db_relay'

    def __init__(self, *args, **kwargs):
        super(DBRelayAgent, self).__init__(*args, **kwargs)
        self._cluster_node = None
        self._cluster_id = None
        self._ceph = ClusterAPI(self._module_inst)
        self._osd_maps = self._ceph.module.get('osd_map')
        self._mon_maps = self._ceph.module.get('mon_map')
        self._fs_maps = self._ceph.module.get('fs_map')
        self._osd_metadata = self._ceph.module.get('osd_metadata')
        self._host_nodes = dict()
        self._osd_nodes = dict()
        self._mon_nodes = dict()
        self._mds_nodes = dict()
        self._dev_nodes = dict()
        self._pool_nodes = dict()
        self._rbd_nodes = dict()
        self._fs_nodes = dict()
        # initial ceph all node states
        self._init_cluster_node()
        self._init_hosts()
        self._init_mons()
        self._init_mds()
        self._init_osds()
        self._init_devices()
        self._init_pools()
        self._init_rbds()
        self._init_fs()

    def _init_hosts(self):
        hosts = set()
        # Add host from osd
        osd_data = self._osd_maps.get('osds', [])
        for _data in osd_data:
            osd_id = _data['osd']
            if not _data.get('in'):
                continue
            osd_addr = _data['public_addr'].split(':')[0]
            osd_metadata = self._ceph.get_osd_metadata(osd_id)
            if osd_metadata:
                osd_host = osd_metadata['hostname']
                hosts.add((osd_host, osd_addr))

        # Add host from mon
        mons = self._mon_maps.get('mons', [])
        for _data in mons:
            mon_host = _data['name']
            mon_addr = _data['public_addr'].split(':')[0]
            if mon_host:
                hosts.add((mon_host, mon_addr))

        # Add host from mds
        file_systems = self._fs_maps.get('filesystems', [])
        for _data in file_systems:
            mds_info = _data.get('mdsmap').get('info')
            for _gid in mds_info:
                mds_data = mds_info[_gid]
                mds_addr = mds_data.get('addr').split(':')[0]
                mds_host = mds_data.get('name')
                if mds_host:
                    hosts.add((mds_host, mds_addr))
        for tp in hosts:
            host = tp[0]
            self._host_nodes[host] = None

            host_node = NodeInfo(
                label='VMHost',
                domain_id='{}_{}'.format(self._cluster_id, host),
                name=host,
                meta={}
            )
            self._host_nodes[host] = host_node

    def _init_mons(self):
        cluster_id = self._cluster_id
        mons = self._mon_maps.get('mons')
        for mon in mons:
            mon_name = mon.get('name', '')
            mon_addr = mon.get('addr', '').split(':')[0]
            if mon_name not in self._host_nodes.keys():
                continue

            dp_mon = MGRDpMon(
                fsid=cluster_id,
                host=mon_name,
                ipaddr=mon_addr
            )

            # create mon node
            mon_node = NodeInfo(
                label='CephMon',
                domain_id='{}.mon.{}'.format(cluster_id, mon_name),
                name=mon_name,
                meta=dp_mon.__dict__
            )
            self._mon_nodes[mon_name] = mon_node

    def _init_mds(self):
        cluster_id = self._cluster_id
        file_systems = self._fs_maps.get('filesystems', [])
        for _data in file_systems:
            mds_info = _data.get('mdsmap').get('info')
            for _gid in mds_info:
                mds_data = mds_info[_gid]
                mds_addr = mds_data.get('addr').split(':')[0]
                mds_host = mds_data.get('name')
                mds_gid = mds_data.get('gid')

                if mds_host not in self._host_nodes:
                    continue

                dp_mds = MGRDpMds(
                    fsid=cluster_id,
                    host=mds_host,
                    ipaddr=mds_addr
                )

                # create osd node
                mds_node = NodeInfo(
                    label='CephMds',
                    domain_id='{}.mds.{}'.format(cluster_id, mds_gid),
                    name='MDS.{}'.format(mds_gid),
                    meta=dp_mds.__dict__
                )
                self._mds_nodes[mds_host] = mds_node

    def _init_osds(self):
        for osd in self._osd_maps.get('osds', []):
            osd_id = osd.get('osd', -1)
            meta = self._osd_metadata.get(str(osd_id), {})
            osd_host = meta['hostname']
            osd_ceph_version = meta['ceph_version']
            osd_rotational = meta['rotational']
            osd_devices = meta['devices'].split(',')

            # filter 'dm' device.
            devices = []
            for devname in osd_devices:
                if 'dm' in devname:
                    continue
                devices.append(devname)

            if osd_host not in self._host_nodes.keys():
                continue
            self._osd_nodes[str(osd_id)] = None
            public_addr = []
            cluster_addr = []
            for addr in osd.get('public_addrs', {}).get('addrvec', []):
                public_addr.append(addr.get('addr'))
            for addr in osd.get('cluster_addrs', {}).get('addrvec', []):
                cluster_addr.append(addr.get('addr'))
            dp_osd = MGRDpOsd(
                fsid=self._cluster_id,
                host=osd_host,
                _id=osd_id,
                uuid=osd.get('uuid'),
                up=osd.get('up'),
                _in=osd.get('in'),
                weight=osd.get('weight'),
                public_addr=','.join(public_addr),
                cluster_addr=','.join(cluster_addr),
                state=','.join(osd.get('state', [])),
                ceph_release=osd_ceph_version,
                osd_devices=','.join(devices),
                rotational=osd_rotational)
            for k, v in meta.items():
                setattr(dp_osd, k, v)

            # create osd node
            osd_node = NodeInfo(
                label='CephOsd',
                domain_id='{}.osd.{}'.format(self._cluster_id, osd_id),
                name='OSD.{}'.format(osd_id),
                meta=dp_osd.__dict__
            )
            self._osd_nodes[str(osd_id)] = osd_node

    def _init_devices(self):
        r = re.compile('[^/dev]\D+')
        for osdid, o_val in self._osd_nodes.items():
            o_devs = o_val.meta.get('device_ids', '').split(',')
            # fs_store
            journal_devs = o_val.meta.get('backend_filestore_journal_dev_node', '').split(',')
            # bs_store
            bs_db_devs = o_val.meta.get('bluefs_db_dev_node', '').split(',')
            bs_wal_devs = o_val.meta.get('bluefs_wal_dev_node', '').split(',')

            for dev in o_devs:
                fs_journal = []
                bs_db = []
                bs_wal = []
                data = []
                if len(dev.split('=')) != 2:
                    continue
                dev_name = dev.split('=')[0]
                dev_id = dev.split('=')[1]
                if not dev_id:
                    continue

                for j_dev in journal_devs:
                    if dev_name == ''.join(r.findall(j_dev)):
                        fs_journal.append(osdid)
                for db_dev in bs_db_devs:
                    if dev_name == ''.join(r.findall(db_dev)):
                        bs_db.append(osdid)
                for wal_dev in bs_wal_devs:
                    if dev_name == ''.join(r.findall(wal_dev)):
                        bs_wal.append(osdid)

                if not fs_journal and not bs_db and not bs_wal:
                    data.append(osdid)

                disk_domain_id = dev_id
                if disk_domain_id not in self._dev_nodes.keys():
                    dp_disk = MGRDpDisk(
                        host_domain_id='{}_{}'.format(self._cluster_id, o_val.meta.get('host')),
                        host=o_val.meta.get('host'),
                        osd_ids=osdid,
                        fs_journal_osd=','.join(str(x) for x in fs_journal) if fs_journal else '',
                        bs_db_osd=','.join(str(x) for x in bs_db) if bs_db else '',
                        bs_wal_osd=','.join(str(x) for x in bs_wal) if bs_wal else '',
                        data_osd=','.join(str(x) for x in data) if data else ''
                    )
                    # create disk node
                    disk_node = NodeInfo(
                        label='VMDisk',
                        domain_id=disk_domain_id,
                        name=dev_name,
                        meta=dp_disk.__dict__
                    )
                    self._dev_nodes[disk_domain_id] = disk_node
                else:
                    dev_node = self._dev_nodes[disk_domain_id]
                    osd_ids = dev_node.meta.get('osd_ids', '')
                    if osdid not in osd_ids.split(','):
                        arr_value = osd_ids.split(',')
                        arr_value.append(str(osdid))
                        dev_node.meta['osd_ids'] = ','.join(arr_value)
                    if fs_journal:
                        arr_value = None
                        for t in fs_journal:
                            value = dev_node.meta.get('fs_journal_osd', '')
                            if value:
                                arr_value = value.split(',')
                            else:
                                arr_value = []
                            if t not in arr_value:
                                arr_value.append(t)
                        if arr_value:
                            dev_node.meta['fs_journal_osd'] = ','.join(str(x) for x in arr_value)
                    if bs_db:
                        arr_value = None
                        for t in bs_db:
                            value = dev_node.meta.get('bs_db_osd', '')
                            if value:
                                arr_value = value.split(',')
                            else:
                                arr_value = []
                            if t not in arr_value:
                                arr_value.append(t)
                        if arr_value:
                            dev_node.meta['bs_db_osd'] = ','.join(str(x) for x in arr_value)
                    if bs_wal:
                        arr_value = None
                        for t in bs_wal:
                            value = dev_node.meta.get('bs_wal_osd', '')
                            if value:
                                arr_value = value.split(',')
                            else:
                                arr_value = []
                            if t not in arr_value:
                                arr_value.append(t)
                        if arr_value:
                            dev_node.meta['bs_wal_osd'] = ','.join(str(x) for x in arr_value)
                    if data:
                        arr_value = None
                        for t in data:
                            value = dev_node.meta.get('data_osd', '')
                            if value:
                                arr_value = value.split(',')
                            else:
                                arr_value = []
                            if t not in arr_value:
                                arr_value.append(t)
                        if arr_value:
                            dev_node.meta['data_osd'] = ','.join(str(x) for x in arr_value)

    def _init_cluster_node(self):
        cluster_id = self._ceph.get_cluster_id()
        ceph_df_stat = self._ceph.get_ceph_df_state()
        dp_cluster = MGRDpCeph(
            fsid=cluster_id,
            health=self._ceph.get_health_status(),
            max_osd=len(self._ceph.get_osds()),
            size=ceph_df_stat.get('total_size'),
            avail_size=ceph_df_stat.get('avail_size'),
            raw_used=ceph_df_stat.get('raw_used_size'),
            raw_used_percent=ceph_df_stat.get('used_percent')
        )
        cluster_name = cluster_id[-12:]
        cluster_node = NodeInfo(
            label='CephCluster',
            domain_id=cluster_id,
            name='cluster-{}'.format(cluster_name),
            meta=dp_cluster.__dict__
        )
        self._cluster_id = cluster_id
        self._cluster_node = cluster_node

    def _init_pools(self):
        pools = self._osd_maps.get('pools', [])
        cluster_id = self._cluster_id
        for pool in pools:
            osds = []
            pgs = self._ceph.get_pgs_up_by_poolid(int(pool.get('pool', -1)))
            for pg_id, osd_id in pgs.items():
                for o_id in osd_id:
                    if o_id not in osds:
                        osds.append(str(o_id))
            dp_pool = MGRDpPool(
                fsid=cluster_id,
                size=pool.get('size'),
                pool_name=pool.get('pool_name'),
                pool_id=pool.get('pool'),
                type=pool.get('type'),
                min_size=pool.get('min_szie'),
                pg_num=pool.get('pg_num'),
                pgp_num=pool.get('pg_placement_num'),
                created_time=pool.get('create_time'),
                pgids=','.join(pgs.keys()),
                osd_ids=','.join(osds),
                tiers=','.join(str(x) for x in pool.get('tiers', [])),
                cache_mode=pool.get('cache_mode', ''),
                erasure_code_profile=str(pool.get('erasure_code_profile', '')),
                tier_of=str(pool.get('tier_of', -1)))
            # create pool node
            pool_node = NodeInfo(
                label='CephPool',
                domain_id='{}_pool_{}'.format(cluster_id, pool.get('pool')),
                name=pool.get('pool_name'),
                meta=dp_pool.__dict__
            )
            self._pool_nodes[str(pool.get('pool'))] = pool_node

    def _init_rbds(self):
        cluster_id = self._cluster_id
        for p_id, p_node in self._pool_nodes.items():
            rbds = self._ceph.get_rbd_list(p_node.name)
            self._rbd_nodes[str(p_id)] = []
            for rbd in rbds:
                dp_rbd = MGRDpRBD(
                    fsid=cluster_id,
                    _id=rbd['id'],
                    name=rbd['name'],
                    pool_name=rbd['pool_name'],
                    pool_id=p_id,
                )
                # create pool node
                rbd_node = NodeInfo(
                    label='CephRBD',
                    domain_id='{}_rbd_{}'.format(cluster_id, rbd['id']),
                    name=rbd['name'],
                    meta=dp_rbd.__dict__,
                )
                self._rbd_nodes[str(p_id)].append(rbd_node)

    def _init_fs(self):
        # _fields = ['fsid', '_id', 'name', 'metadata_pool', 'data_pool', 'mds_nodes']
        cluster_id = self._cluster_id
        file_systems = self._fs_maps.get('filesystems', [])
        for fs in file_systems:
            mdsmap = fs.get('mdsmap', {})
            mds_hostnames = []
            for m, md in mdsmap.get('info', {}).items():
                if md.get('name') not in mds_hostnames:
                    mds_hostnames.append(md.get('name'))
            dp_fs = MGRDpFS(
                fsid=cluster_id,
                _id=fs.get('id'),
                name=mdsmap.get('fs_name'),
                metadata_pool=str(mdsmap.get('metadata_pool', -1)),
                data_pools=','.join(str(i) for i in mdsmap.get('data_pools', [])),
                mds_nodes=','.join(mds_hostnames),
            )
            fs_node = NodeInfo(
                label='CephFS',
                domain_id='{}_fs_{}'.format(cluster_id, fs.get('id')),
                name=mdsmap.get('fs_name'),
                meta=dp_fs.__dict__,
            )
            self._fs_nodes[str(fs.get('id'))] = fs_node

    def _cluster_contains_host(self):
        cluster_id = self._cluster_id
        cluster_node = self._cluster_node

        # create node relation
        for h_id, h_node in self._host_nodes.items():
            data = DBRelay()
            # add osd node relationship
            cypher_cmd = CypherOP.add_link(
                cluster_node,
                h_node,
                'CephClusterContainsHost'
            )
            cluster_host = socket.gethostname()
            data.fields['agenthost'] = cluster_host
            data.tags['agenthost_domain_id'] = cluster_id
            data.tags['host'] = cluster_host
            data.fields['cmd'] = str(cypher_cmd)
            self.data.append(data)

    def _host_contains_mon(self):
        for m_name, m_node in self._mon_nodes.items():
            host_node = self._host_nodes.get(m_name)
            if not host_node:
                continue
            data = DBRelay()
            # add mon node relationship
            cypher_cmd = CypherOP.add_link(
                host_node,
                m_node,
                'HostContainsMon'
            )
            cluster_host = socket.gethostname()
            data.fields['agenthost'] = cluster_host
            data.tags['agenthost_domain_id'] = self._cluster_id
            data.tags['host'] = cluster_host
            data.fields['cmd'] = str(cypher_cmd)
            self.data.append(data)

    def _host_contains_osd(self):
        cluster_id = self._cluster_id
        for o_id, o_node in self._osd_nodes.items():
            host_node = self._host_nodes.get(o_node.meta.get('host'))
            if not host_node:
                continue
            data = DBRelay()
            # add osd node relationship
            cypher_cmd = CypherOP.add_link(
                host_node,
                o_node,
                'HostContainsOsd'
            )
            cluster_host = socket.gethostname()
            data.fields['agenthost'] = cluster_host
            data.tags['agenthost_domain_id'] = cluster_id, data
            data.tags['host'] = cluster_host
            data.fields['cmd'] = str(cypher_cmd)
            self.data.append(data)

    def _host_contains_mds(self):
        cluster_id = self._cluster_id
        for m_name, mds_node in self._mds_nodes.items():
            data = DBRelay()
            host_node = self._host_nodes.get(mds_node.meta.get('host'))
            if not host_node:
                continue
            # add osd node relationship
            cypher_cmd = CypherOP.add_link(
                host_node,
                mds_node,
                'HostContainsMds'
            )
            cluster_host = socket.gethostname()
            data.fields['agenthost'] = cluster_host
            data.tags['agenthost_domain_id'] = cluster_id
            data.tags['host'] = cluster_host
            data.fields['cmd'] = str(cypher_cmd)
            self.data.append(data)

    def _osd_contains_disk(self):
        cluster_id = self._cluster_id
        cluster_host = socket.gethostname()
        for d_name, d_node in self._dev_nodes.items():
            keys = {'data_osd': 'DataDiskOfOSD',
                    'fs_journal_osd': 'FsJournalDiskOfOSD',
                    'bs_db_osd': 'BsDBDiskOfOSD',
                    'bs_wal_osd': 'BsWalDiskOfOSD'}
            for k, v in keys.items():
                if not d_node.meta.get(k):
                    continue
                for osdid in d_node.meta.get(k, '').split(','):
                    data = DBRelay()
                    osd_node = self._osd_nodes.get(str(osdid))
                    if not osd_node:
                        continue
                    # add disk node relationship
                    cypher_cmd = CypherOP.add_link(
                        osd_node,
                        d_node,
                        v)
                    data.fields['agenthost'] = cluster_host
                    data.tags['agenthost_domain_id'] = cluster_id
                    data.tags['host'] = cluster_host
                    data.fields['cmd'] = str(cypher_cmd)
                    self.data.append(data)

            hostname = d_node.meta.get('host', '')
            if not hostname:
                continue
            host_node = self._host_nodes.get(hostname)
            if not host_node:
                continue
            # add osd node relationship
            data = DBRelay()
            cypher_cmd = CypherOP.add_link(
                host_node,
                d_node,
                'VmHostContainsVmDisk'
            )
            data.fields['agenthost'] = cluster_host
            data.tags['agenthost_domain_id'] = cluster_id
            data.tags['host'] = cluster_host
            data.fields['cmd'] = str(cypher_cmd)
            self.data.append(data)

    def _pool_contains_osd(self):
        cluster_id = self._cluster_id
        cluster_host = socket.gethostname()
        for p_id, p_node in self._pool_nodes.items():
            for o_id in p_node.meta.get('osd_ids', '').split(','):
                osd_node = self._osd_nodes.get(str(o_id))
                if not osd_node:
                    continue
                data = DBRelay()
                cypher_cmd = CypherOP.add_link(
                    osd_node,
                    p_node,
                    'OsdContainsPool'
                )
                data.fields['agenthost'] = cluster_host
                data.tags['agenthost_domain_id'] = cluster_id
                data.tags['host'] = cluster_host
                data.fields['cmd'] = str(cypher_cmd)
                self.data.append(data)

    def _pool_contains_rbd(self):
        cluster_id = self._cluster_id
        cluster_host = socket.gethostname()
        for p_id, p_node in self._pool_nodes.items():
            for rbd_node in self._rbd_nodes.get(str(p_id), []):
                if not rbd_node:
                    continue
                data = DBRelay()
                cypher_cmd = CypherOP.add_link(
                    p_node,
                    rbd_node,
                    'PoolContainsRBD'
                )
                data.fields['agenthost'] = cluster_host
                data.tags['agenthost_domain_id'] = cluster_id
                data.tags['host'] = cluster_host
                data.fields['cmd'] = str(cypher_cmd)
                self.data.append(data)

    def _pool_contains_fs(self):
        cluster_id = self._cluster_id
        cluster_host = socket.gethostname()
        for fs_id, fs_node in self._fs_nodes.items():
            pool_attrs = ['metadata_pool', 'data_pools']
            for p_attr in pool_attrs:
                pools_id = fs_node.meta.get(p_attr).split(',')
                for p_id in pools_id:
                    p_node = self._pool_nodes.get(str(p_id))
                    if p_node:
                        data = DBRelay()
                        cypher_cmd = CypherOP.add_link(
                            p_node,
                            fs_node,
                            'MetadataPoolContainsFS' if p_attr == 'metadata_pool' else 'DataPoolContainsFS'
                        )
                        data.fields['agenthost'] = cluster_host
                        data.tags['agenthost_domain_id'] = cluster_id
                        data.tags['host'] = cluster_host
                        data.fields['cmd'] = str(cypher_cmd)
                        self.data.append(data)
            for mds_name in fs_node.meta.get('mds_nodes', '').split(','):
                mds_node = self._mds_nodes.get(mds_name)
                if not mds_node:
                    continue
                data = DBRelay()
                cypher_cmd = CypherOP.add_link(
                    mds_node,
                    fs_node,
                    'MDSContainsFS'
                )
                data.fields['agenthost'] = cluster_host
                data.tags['agenthost_domain_id'] = cluster_id
                data.tags['host'] = cluster_host
                data.fields['cmd'] = str(cypher_cmd)
                self.data.append(data)

    def _collect_data(self):
        if not self._module_inst:
            return
        job_name = ['cluster_contains_host', 'host_contains_mon', 'host_contains_mds', 'host_contains_osd', 'osd_contains_disk',
                    'pool_contains_osd', 'pool_contains_rbd', 'pool_contains_fs']
        for job in job_name:
            fn = getattr(self, '_%s' % job)
            if not fn:
                continue
            try:
                fn()
            except Exception as e:
                self._module_inst.log.error('dbrelay - execute function {} fail, due to {}'.format(job, str(e)))
                continue
