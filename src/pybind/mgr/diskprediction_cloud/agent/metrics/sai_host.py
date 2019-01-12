from __future__ import absolute_import

import socket

from . import AGENT_VERSION, MetricsAgent, MetricsField
from ...common.clusterdata import ClusterAPI


class SAIHostFields(MetricsField):
    """ SAI Host structure """
    measurement = 'sai_host'

    def __init__(self):
        super(SAIHostFields, self).__init__()
        self.tags['domain_id'] = None
        self.fields['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.fields['cluster_domain_id'] = None
        self.fields['name'] = None
        self.fields['host_ip'] = None
        self.fields['host_ipv6'] = None
        self.fields['host_uuid'] = None
        self.fields['os_type'] = str('ceph')
        self.fields['os_name'] = None
        self.fields['os_version'] = None
        self.fields['agent_version'] = str(AGENT_VERSION)


class SAIHostAgent(MetricsAgent):
    measurement = 'sai_host'

    def _collect_data(self):
        db = ClusterAPI(self._module_inst)
        cluster_id = db.get_cluster_id()

        hosts = set()

        # Parse osd's host
        osd_data = db.get_osds()
        for _data in osd_data:
            osd_id = _data['osd']
            if not _data.get('in'):
                continue
            osd_addr = _data['public_addr'].split(':')[0]
            osd_metadata = db.get_osd_metadata(osd_id)
            if osd_metadata:
                osd_host = osd_metadata.get('hostname', 'None')
                if osd_host not in hosts:
                    data = SAIHostFields()
                    data.fields['agenthost'] = str(socket.gethostname())
                    data.tags['agenthost_domain_id'] = cluster_id
                    data.tags['domain_id'] = \
                        str('%s_%s' % (cluster_id, osd_host))
                    data.fields['cluster_domain_id'] = str(cluster_id)
                    data.fields['host_ip'] = osd_addr
                    data.fields['host_uuid'] = \
                        str('%s_%s' % (cluster_id, osd_host))
                    data.fields['os_name'] = \
                        osd_metadata.get('ceph_release', '')
                    data.fields['os_version'] = \
                        osd_metadata.get('ceph_version_short', '')
                    data.fields['name'] = 'osd_{}'.format(osd_host)
                    hosts.add(osd_host)
                    self.data.append(data)

        # Parse mon node host
        mons = db.get_mons()
        for _data in mons:
            mon_host = _data['name']
            mon_addr = _data['public_addr'].split(':')[0]
            if mon_host not in hosts:
                data = SAIHostFields()
                data.fields['agenthost'] = str(socket.gethostname())
                data.tags['agenthost_domain_id'] = cluster_id
                data.tags['domain_id'] = \
                    str('%s_%s' % (cluster_id, mon_host))
                data.fields['cluster_domain_id'] = str(cluster_id)
                data.fields['host_ip'] = mon_addr
                data.fields['host_uuid'] = \
                    str('%s_%s' % (cluster_id, mon_host))
                data.fields['name'] = 'mon_{}'.format(mon_host)
                hosts.add((mon_host, mon_addr))
                self.data.append(data)

        # Parse fs host
        file_systems = db.get_file_systems()
        for _data in file_systems:
            mds_info = _data.get('mdsmap').get('info')
            for _gid in mds_info:
                mds_data = mds_info[_gid]
                mds_addr = mds_data.get('addr').split(':')[0]
                mds_host = mds_data.get('name')
                if mds_host not in hosts:
                    data = SAIHostFields()
                    data.fields['agenthost'] = str(socket.gethostname())
                    data.tags['agenthost_domain_id'] = cluster_id
                    data.tags['domain_id'] = \
                        str('%s_%s' % (cluster_id, mds_host))
                    data.fields['cluster_domain_id'] = str(cluster_id)
                    data.fields['host_ip'] = mds_addr
                    data.fields['host_uuid'] = \
                        str('%s_%s' % (cluster_id, mds_host))
                    data.fields['name'] = 'mds_{}'.format(mds_host)
                    hosts.add((mds_host, mds_addr))
                    self.data.append(data)
