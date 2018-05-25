from __future__ import absolute_import

import socket

from . import MetricsAgent
from ...common.db import DB_API
from ...models.metrics.dp import SAI_Host


class SAI_HostAgent(MetricsAgent):
    measurement = 'sai_host'

    def _collect_data(self):
        db = DB_API(self._ceph_context)
        cluster_id = db.get_cluster_id()

        hosts = set()

        osd_data = db.get_osds()
        for _data in osd_data:
            osd_id = _data['osd']
            osd_addr = _data['public_addr'].split(':')[0]
            osd_metadata = db.get_osd_metadata(osd_id)
            if osd_metadata:
                osd_host = osd_metadata.get("hostname", "None")
                if osd_host not in hosts:
                    data = SAI_Host()
                    data.tags['agenthost'] = str(socket.gethostname())
                    data.tags['agenthost_domain_id'] = \
                        str("%s_%s" % (cluster_id, data.tags['agenthost']))
                    data.tags['domain_id'] = \
                        str("%s_%s" % (cluster_id, osd_host))
                    data.fields['cluster_domain_id'] = str(cluster_id)
                    data.fields['host_ip'] = osd_addr
                    data.fields['host_uuid'] = \
                        str("%s_%s" % (cluster_id, osd_host))
                    data.fields['os_name'] = \
                        osd_metadata.get('ceph_release', '')
                    data.fields['os_version'] = \
                        osd_metadata.get('ceph_version_short', '')
                    data.fields['name'] = osd_host
                    hosts.add(osd_host)
                    self.data.append(data)

        mons = db.get_mons()
        for _data in mons:
            mon_host = _data['name']
            mon_addr = _data['public_addr'].split(':')[0]
            if mon_host not in hosts:
                data = SAI_Host()
                data.tags['agenthost'] = str(socket.gethostname())
                data.tags['agenthost_domain_id'] = \
                    str("%s_%s" % (cluster_id, data.tags['agenthost']))
                data.tags['domain_id'] = \
                    str("%s_%s" % (cluster_id, mon_host))
                data.fields['cluster_domain_id'] = str(cluster_id)
                data.fields['host_ip'] = mon_addr
                data.fields['host_uuid'] = \
                    str("%s_%s" % (cluster_id, mon_host))
                data.fields['name'] = mon_host
                hosts.add((mon_host, mon_addr))
                self.data.append(data)

        file_systems = db.get_file_systems()
        for _data in file_systems:
            mds_info = _data.get('mdsmap').get('info')
            for _gid in mds_info:
                mds_data = mds_info[_gid]
                mds_addr = mds_data.get('addr').split(':')[0]
                mds_host = mds_data.get('name')
                if mds_host not in hosts:
                    data = SAI_Host()
                    data.tags['agenthost'] = str(socket.gethostname())
                    data.tags['agenthost_domain_id'] = \
                        str("%s_%s" % (cluster_id, data.tags['agenthost']))
                    data.tags['domain_id'] = \
                        str("%s_%s" % (cluster_id, mds_host))
                    data.fields['cluster_domain_id'] = str(cluster_id)
                    data.fields['host_ip'] = mds_addr
                    data.fields['host_uuid'] = \
                        str("%s_%s" % (cluster_id, mds_host))
                    data.fields['name'] = mds_host
                    hosts.add((mds_host, mds_addr))
                    self.data.append(data)
