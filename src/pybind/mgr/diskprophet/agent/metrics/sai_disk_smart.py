from __future__ import absolute_import

import socket

from . import MetricsAgent
from ...common.db import DB_API
from ...models.metrics.dp import SAI_Disk_Smart


class SAI_DiskSmartAgent(MetricsAgent):
    measurement = 'sai_disk_smart'

    def _collect_data(self):
        # process data and save to 'self.data'
        obj_api = DB_API(self._ceph_context)
        cluster_id = obj_api.get_cluster_id()
        osds = obj_api.get_osds()
        for osd in osds:
            if osd.get('osd') is None:
                continue
            osds_meta = obj_api.get_osd_metadata(osd.get('osd'))
            if not osds_meta:
                continue
            osds_smart = obj_api.get_osd_smart(osd.get('osd'))
            if not osds_smart:
                continue
            for dev_name, s_val in osds_smart.iteritems():
                smart_data = SAI_Disk_Smart()
                smart_data.tags['disk_name'] = str(dev_name)
                smart_data.fields['cluster_domain_id'] = str(cluster_id)
                smart_data.fields['host_domain_id'] = \
                    str('%s_%s'
                        % (cluster_id, osds_meta.get('hostname', 'None')))
                smart_data.tags['agenthost'] = str(socket.gethostname())
                smart_data.tags['agenthost_domain_id'] = \
                    str('%s_%s' % (cluster_id, smart_data.tags['agenthost']))
                # parse attributes
                ata_smart = s_val.get('ata_smart_attributes', {})
                for attr in ata_smart.get('attrs', []):
                    smart_data.fields['%s_raw' % attr.get('id')] = \
                        attr.get('raw', {}).get('value', 0)
                if s_val.get('temperature', {}).get('current'):
                    smart_data.fields['CurrentDriveTemperature_raw'] = \
                        int(s_val['temperature']['current'])
                serial_number = s_val.get('serial_number')
                wwn = s_val.get("wwn", {}).get("id")
                if wwn:
                    smart_data.tags['disk_domain_id'] = str(wwn)
                    smart_data.tags['disk_wwn'] = str(wwn)
                    if serial_number:
                        smart_data.tags['serial_number'] = str(serial_number)
                    else:
                        smart_data.tags['serial_number'] = str(wwn)
                elif serial_number:
                    smart_data.tags['disk_domain_id'] = str(serial_number)
                    smart_data.fields['serial_number'] = str(serial_number)
                    if wwn:
                        smart_data.tags['disk_wwn'] = str(wwn)
                    else:
                        smart_data.tags['disk_wwn'] = str(serial_number)
                else:
                    smart_data.tags['disk_domain_id'] = str(dev_name)
                    smart_data.tags['disk_wwn'] = str(dev_name)
                    smart_data.fields['serial_number'] = str(dev_name)

                smart_data.tags['primary_key'] = \
                    str('%s%s%s' % (cluster_id,
                                smart_data.fields['host_domain_id'],
                                smart_data.tags['disk_domain_id']))
                self.data.append(smart_data)
