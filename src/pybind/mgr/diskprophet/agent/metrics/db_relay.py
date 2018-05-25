from __future__ import absolute_import

import socket
import time

from . import MetricsAgent

from ...common.cypher import CypherOP, NodeInfo
from ...common.db import DB_API
from ...models.metrics.dp import DB_Relay
from ...models.relation.mgrdp import MGRDpCeph, MGRDpMon, MGRDpOsd, MGRDpHost, \
                                     MGRDpMds, MGRDpPG, MGRDpDisk, MGRDpRBD


class DB_RelayAgent(MetricsAgent):
    measurement = 'db_relay'

    def __init__(self, *args, **kwargs):
        super(DB_RelayAgent, self).__init__(*args, **kwargs)
        self._cluster_node = self._get_cluster_node()
        self._cluster_id = self._cluster_node.domain_id
        self._host_nodes = dict()
        self._osd_nodes = dict()

    def _get_cluster_node(self):
        db = DB_API(self._ceph_context)
        cluster_id = db.get_cluster_id()
        dp_cluster = MGRDpCeph(
            fsid=cluster_id,
            health=db.get_health_status(),
            max_osd=db.get_max_osd(),
            size=db.get_global_total_size(),
            avail_size=db.get_global_avail_size(),
            raw_used=db.get_global_raw_used_size(),
            raw_used_percent=db.get_global_raw_used_percent()
        )
        cluster_id = db.get_cluster_id()
        cluster_name = cluster_id[-12:]
        cluster_node = NodeInfo(
            label='CephCluster',
            domain_id=cluster_id,
            name='cluster-{}'.format(cluster_name),
            meta=dp_cluster.__dict__
        )
        return cluster_node

    def _cluster_contains_host(self):
        cluster_id = self._cluster_id
        cluster_node = self._cluster_node

        db = DB_API(self._ceph_context)

        hosts = set()

        # Add host from osd
        osd_data = db.get_osds()
        for _data in osd_data:
            osd_id = _data['osd']
            osd_addr = _data['public_addr'].split(':')[0]
            osd_metadata = db.get_osd_metadata(osd_id)
            if osd_metadata:
                osd_host = osd_metadata['hostname']
                hosts.add((osd_host, osd_addr))

        # Add host from mon
        mons = db.get_mons()
        for _data in mons:
            mon_host = _data['name']
            mon_addr = _data['public_addr'].split(':')[0]
            if mon_host:
                hosts.add((mon_host, mon_addr))

        # Add host from mds
        file_systems = db.get_file_systems()
        for _data in file_systems:
            mds_info = _data.get('mdsmap').get('info')
            for _gid in mds_info:
                mds_data = mds_info[_gid]
                mds_addr = mds_data.get('addr').split(':')[0]
                mds_host = mds_data.get('name')
                if mds_host:
                    hosts.add((mds_host, mds_addr))

        # create node relation
        for tp in hosts:
            data = DB_Relay()
            host = tp[0]
            ipaddr = tp[1]
            self._host_nodes[host] = None
            dp_host = MGRDpHost(
                fsid=cluster_id,
                host=host,
                ipaddr=ipaddr
            )

            host_node = NodeInfo(
                label='CephHost',
                domain_id="{}.{}".format(cluster_id, host),
                name=ipaddr,
                meta=dp_host.__dict__
            )

            # add osd node relationship
            timestamp = int(time.time())
            cypher_cmd = CypherOP.add_link(
                cluster_node,
                host_node,
                'CephClusterContainsHost',
                timestamp
            )
            cluster_host = socket.gethostname()
            data.tags['agenthost'] = cluster_host
            data.tags['agenthost_domain_id'] = cluster_id + cluster_host
            data.tags['host'] = cluster_host
            data.fields['cmd'] = str(cypher_cmd)
            self._host_nodes[host] = host_node
            self.data.append(data)

    def _host_contains_mon(self):
        cluster_id = self._cluster_id

        db = DB_API(self._ceph_context)
        mons = db.get_mons()
        for mon in mons:
            mon_name = mon.get('name', '')
            mon_addr = mon.get('addr', '').split(':')[0]
            for hostname in self._host_nodes:
                if hostname != mon_name:
                    continue

                host_node = self._host_nodes[hostname]
                data = DB_Relay()
                dp_mon = MGRDpMon(
                    fsid=cluster_id,
                    host=mon_name,
                    ipaddr=mon_addr
                )

                # create mon node
                mon_node = NodeInfo(
                    label='CephMon',
                    domain_id="{}.mon.{}".format(cluster_id, mon_name),
                    name=mon_name,
                    meta=dp_mon.__dict__
                )

                # add mon node relationship
                timestamp = int(time.time())
                cypher_cmd = CypherOP.add_link(
                    host_node,
                    mon_node,
                    'HostContainsMon',
                    timestamp
                )
                cluster_host = socket.gethostname()
                data.tags['agenthost'] = cluster_host
                data.tags['agenthost_domain_id'] = cluster_id + cluster_host
                data.tags['host'] = cluster_host
                data.fields['cmd'] = str(cypher_cmd)
                self.data.append(data)

    def _host_contains_osd(self):
        cluster_id = self._cluster_id

        db = DB_API(self._ceph_context)
        osd_data = db.get_osd_data()
        osd_journal = db.get_osd_journal()
        for _data in db.get_osds():
            osd_id = _data['osd']
            osd_uuid = _data['uuid']
            osd_up = _data['up']
            osd_in = _data['in']
            osd_weight = _data['weight']
            osd_public_addr = _data['public_addr']
            osd_cluster_addr = _data['cluster_addr']
            osd_heartbeat_back_addr = _data['heartbeat_back_addr']
            osd_heartbeat_front_addr = _data['heartbeat_front_addr']
            osd_state = _data['state']
            osd_metadata = db.get_osd_metadata(osd_id)
            if osd_metadata:
                data = DB_Relay()
                osd_host = osd_metadata['hostname']
                osd_ceph_version = osd_metadata['ceph_version']
                osd_rotational = osd_metadata['rotational']
                osd_devices = osd_metadata['devices'].split(',')

                # filter 'dm' device.
                devices = []
                for devname in osd_devices:
                    if 'dm' in devname:
                        continue
                    devices.append(devname)

                for hostname in self._host_nodes:
                    if hostname != osd_host:
                        continue

                    self._osd_nodes[str(osd_id)] = None
                    host_node = self._host_nodes[hostname]
                    osd_dev_node = None
                    for dev_node in ['backend_filestore_dev_node',
                                     'bluestore_bdev_dev_node']:
                        val = osd_metadata.get(dev_node)
                        if val and val.lower() != 'unknown':
                            osd_dev_node = val
                            break

                    osd_dev_path = None
                    for dev_path in ['backend_filestore_partition_path',
                                     'bluestore_bdev_partition_path']:
                        val = osd_metadata.get(dev_path)
                        if val and val.lower() != 'unknown':
                            osd_dev_path = val
                            break

                    dp_osd = MGRDpOsd(
                        fsid=cluster_id,
                        host=osd_host,
                        _id=osd_id,
                        uuid=osd_uuid,
                        up=osd_up,
                        _in=osd_in,
                        weight=osd_weight,
                        public_addr=osd_public_addr,
                        cluster_addr=osd_cluster_addr,
                        heartbeat_back_addr=osd_heartbeat_back_addr,
                        heartbeat_front_addr=osd_heartbeat_front_addr,
                        state=','.join(osd_state),
                        backend_filestore_dev_node=osd_dev_node,
                        backend_filestore_partition_path=osd_dev_path,
                        ceph_release=osd_ceph_version,
                        osd_data=osd_data,
                        osd_journal=osd_journal,
                        devices=','.join(devices),
                        rotational=osd_rotational)

                    # create osd node
                    osd_node = NodeInfo(
                        label='CephOsd',
                        domain_id="{}.osd.{}".format(cluster_id, osd_id),
                        name='OSD.{}'.format(osd_id),
                        meta=dp_osd.__dict__
                    )
                    # add osd node relationship
                    timestamp = int(time.time())
                    cypher_cmd = CypherOP.add_link(
                        host_node,
                        osd_node,
                        'HostContainsOsd',
                        timestamp
                    )
                    cluster_host = socket.gethostname()
                    data.tags['agenthost'] = cluster_host
                    data.tags['agenthost_domain_id'] = cluster_id + cluster_host
                    data.tags['host'] = cluster_host
                    data.fields['cmd'] = str(cypher_cmd)
                    self._osd_nodes[str(osd_id)] = osd_node
                    self.data.append(data)

    def _host_contains_mds(self):
        cluster_id = self._cluster_id

        db = DB_API(self._ceph_context)
        file_systems = db.get_file_systems()

        for _data in file_systems:
            mds_info = _data.get('mdsmap').get('info')
            for _gid in mds_info:
                mds_data = mds_info[_gid]
                mds_addr = mds_data.get('addr').split(':')[0]
                mds_host = mds_data.get('name')
                mds_gid = mds_data.get('gid')

                for hostname in self._host_nodes:
                    if hostname != mds_host:
                        continue

                    data = DB_Relay()
                    host_node = self._host_nodes[hostname]
                    dp_mds = MGRDpMds(
                        fsid=cluster_id,
                        host=mds_host,
                        ipaddr=mds_addr
                    )

                    # create osd node
                    mds_node = NodeInfo(
                        label='CephMds',
                        domain_id="{}.mds.{}".format(cluster_id, mds_gid),
                        name='MDS.{}'.format(mds_gid),
                        meta=dp_mds.__dict__
                    )
                    # add osd node relationship
                    timestamp = int(time.time())
                    cypher_cmd = CypherOP.add_link(
                        host_node,
                        mds_node,
                        'HostContainsMds',
                        timestamp
                    )
                    cluster_host = socket.gethostname()
                    data.tags['agenthost'] = cluster_host
                    data.tags['agenthost_domain_id'] = cluster_id + cluster_host
                    data.tags['host'] = cluster_host
                    data.fields['cmd'] = str(cypher_cmd)
                    self.data.append(data)

    def _osd_contains_pg(self):
        cluster_id = self._cluster_id
        db = DB_API(self._ceph_context)

        pg_stats = db.get_pg_stats()
        for osd_data in db.get_osds():
            osd_id = osd_data['osd']
            for _data in pg_stats:
                state = _data.get('state')
                up = _data.get('up')
                acting = _data.get('acting')
                pgid = _data.get('pgid')
                stat_sum = _data.get('stat_sum', {})
                num_objects = stat_sum.get('num_objects')
                num_objects_degraded = stat_sum.get('num_objects_degraded')
                num_objects_misplaced = stat_sum.get('num_objects_misplaced')
                num_objects_unfound = stat_sum.get('num_objects_unfound')
                if osd_id in up:
                    if str(osd_id) not in self._osd_nodes:
                        continue
                    osd_node = self._osd_nodes[str(osd_id)]
                    data = DB_Relay()
                    dp_pg = MGRDpPG(
                        fsid=cluster_id,
                        pgid=pgid,
                        up_osds=','.join(str(x) for x in up),
                        acting_osds=','.join(str(x) for x in acting),
                        state=state,
                        objects=num_objects,
                        degraded=num_objects_degraded,
                        misplaced=num_objects_misplaced,
                        unfound=num_objects_unfound
                    )

                    # create pg node
                    pg_node = NodeInfo(
                        label='CephPG',
                        domain_id="{}.osd.{}.pg.{}".format(cluster_id, osd_id, pgid),
                        name='PG.{}'.format(pgid),
                        meta=dp_pg.__dict__
                    )

                    # add pg node relationship
                    timestamp = int(time.time())
                    cypher_cmd = CypherOP.add_link(
                        osd_node,
                        pg_node,
                        'OsdContainsPg',
                        timestamp
                    )
                    cluster_host = socket.gethostname()
                    data.tags['agenthost'] = cluster_host
                    data.tags['agenthost_domain_id'] = cluster_id + cluster_host
                    data.tags['host'] = cluster_host
                    data.fields['cmd'] = str(cypher_cmd)
                    self.data.append(data)

    def _osd_contains_disk(self):
        cluster_id = self._cluster_id
        db = DB_API(self._ceph_context)

        osd_metadata = db.get_osd_metadata()
        for osd_id in osd_metadata:
            osds_smart = db.get_osd_smart(osd_id)
            if not osds_smart:
                continue

            if str(osd_id) not in self._osd_nodes:
                continue

            osd_node = self._osd_nodes[str(osd_id)]
            for dev_name, s_val in osds_smart.iteritems():
                data = DB_Relay()
                s_val.get("serial_number")
                dp_disk = MGRDpDisk(
                    fsid=cluster_id,
                    osd_id=osd_id,
                    serial_number=s_val.get("serial_number"),
                    disk_name=dev_name
                )

                # create disk node
                disk_node = NodeInfo(
                    label='CephDisk',
                    domain_id="{}.osd.{}.{}".format(cluster_id, osd_id, dev_name),
                    name=dev_name,
                    meta=dp_disk.__dict__
                )

                # add disk node relationship
                timestamp = int(time.time())
                cypher_cmd = CypherOP.add_link(
                    osd_node,
                    disk_node,
                    'DiskOfOsd',
                    timestamp
                )
                cluster_host = socket.gethostname()
                data.tags['agenthost'] = cluster_host
                data.tags['agenthost_domain_id'] = cluster_id + cluster_host
                data.tags['host'] = cluster_host
                data.fields['cmd'] = str(cypher_cmd)
                self.data.append(data)

    def _rbd_contains_pg(self):
        cluster_id = self._cluster_id
        db = DB_API(self._ceph_context)

        pg_stats = db.get_pg_stats()
        pools = db.get_osd_pools()
        for pool_data in pools:
            pool_name = pool_data.get('pool_name')
            rbd_list = db.get_rbd_list(pool_name=pool_name)
            for rbd_data in rbd_list:
                image_name = rbd_data.get('name')
                rbd_info = db.get_rbd_info(pool_name, image_name)
                rbd_id = rbd_info.get('id')
                rbd_size = rbd_info.get('size')
                rbd_pgids = rbd_info.get('pgs', [])
                pgids = []
                for _data in rbd_pgids:
                    pgid = _data.get('pgid')
                    if pgid:
                        pgids.append(pgid)

                for _data in pg_stats:
                    pgid = _data.get('pgid')
                    if pgid not in pgids:
                        continue

                    state = _data.get('state')
                    up = _data.get('up')
                    acting = _data.get('acting')
                    stat_sum = _data.get('stat_sum', {})
                    num_objects = stat_sum.get('num_objects')
                    num_objects_degraded = stat_sum.get('num_objects_degraded')
                    num_objects_misplaced = stat_sum.get('num_objects_misplaced')
                    num_objects_unfound = stat_sum.get('num_objects_unfound')

                    data = DB_Relay()
                    dp_rbd = MGRDpRBD(
                        fsid=cluster_id,
                        _id=rbd_id,
                        name=image_name,
                        pool_name=pool_name,
                        size=rbd_size,
                        pgids=','.join(pgids)
                    )

                    # create rbd node
                    rbd_node = NodeInfo(
                        label='CephRBD',
                        domain_id="{}.rbd.{}".format(cluster_id, image_name),
                        name=image_name,
                        meta=dp_rbd.__dict__
                    )

                    dp_pg = MGRDpPG(
                        fsid=cluster_id,
                        pgid=pgid,
                        up_osds=','.join(str(x) for x in up),
                        acting_osds=','.join(str(x) for x in acting),
                        state=state,
                        objects=num_objects,
                        degraded=num_objects_degraded,
                        misplaced=num_objects_misplaced,
                        unfound=num_objects_unfound
                    )

                    # create pg node
                    pg_node = NodeInfo(
                        label='CephPG',
                        domain_id="{}.rbd.{}.pg.{}".format(cluster_id, image_name, pgid),
                        name='PG.{}'.format(pgid),
                        meta=dp_pg.__dict__
                    )

                    # add rbd node relationship
                    timestamp = int(time.time())
                    cypher_cmd = CypherOP.add_link(
                        rbd_node,
                        pg_node,
                        'RbdContainsPg',
                        timestamp
                    )
                    cluster_host = socket.gethostname()
                    data.tags['agenthost'] = cluster_host
                    data.tags['agenthost_domain_id'] = cluster_id + cluster_host
                    data.tags['host'] = cluster_host
                    data.fields['cmd'] = str(cypher_cmd)
                    self.data.append(data)


    def _collect_data(self):
        if not self._ceph_context:
            return

        self._cluster_contains_host()
        self._host_contains_osd()
        self._host_contains_mon()
        self._host_contains_mds()
        self._osd_contains_pg()
        self._osd_contains_disk()
        self._rbd_contains_pg()
