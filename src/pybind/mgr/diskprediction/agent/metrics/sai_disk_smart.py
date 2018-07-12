from __future__ import absolute_import

import socket

from . import MetricsAgent, AGENT_VERSION
from ...common.db import DB_API
from ...models.metrics.dp import SAI_Disk_Smart


class SAI_DiskSmartAgent(MetricsAgent):
    measurement = 'sai_disk_smart'

    def _collect_data(self):
        # process data and save to 'self.data'
        obj_api = DB_API(self._ceph_context)
        if self._ceph_context.get_configuration('diskprediction_smart_relied_device_health').lower() == 'true':
            rely_device_health = True
        else:
            rely_device_health = False
        cluster_id = obj_api.get_cluster_id()
        osds = obj_api.get_osds()
        for osd in osds:
            osds_smart = None
            if osd.get('osd') is None:
                continue
            if not osd.get('in'):
                continue
            osds_meta = obj_api.get_osd_metadata(osd.get('osd'))
            if not osds_meta:
                continue
            devs_info = obj_api.get_osd_device_id(osd.get('osd'))
            if devs_info:
                for dev_name, dev_info in devs_info.iteritems():
                    if rely_device_health:
                        osds_smart = obj_api.get_device_health(dev_info['dev_id'])
                    if not osds_smart:
                        osds_smart = obj_api.get_osd_smart(osd.get('osd'), dev_info['dev_id'])
                    if not osds_smart:
                        continue
                    for dev_identify, s_val in osds_smart.iteritems():
                        smart_data = SAI_Disk_Smart()
                        smart_data.tags['disk_name'] = str(dev_name)
                        smart_data.fields['cluster_domain_id'] = str(cluster_id)
                        smart_data.tags['host_domain_id'] = \
                            str('%s_%s'
                                % (cluster_id, osds_meta.get('hostname', 'None')))
                        smart_data.fields['agenthost'] = str(socket.gethostname())
                        smart_data.tags['agenthost_domain_id'] = \
                            str('%s_%s' % (cluster_id, smart_data.fields['agenthost']))
                        # parse attributes
                        ata_smart = s_val.get('ata_smart_attributes', {})
                        for attr in ata_smart.get('table', []):
                            if attr.get('raw', {}).get('string'):
                                if str(attr.get('raw', {}).get('string', "0")).isdigit():
                                    smart_data.fields['%s_raw' % attr.get('id')] = \
                                        int(attr.get('raw', {}).get('string', "0"))
                                else:
                                    if str(attr.get('raw', {}).get('string', "0")).split(" ")[0].isdigit():
                                        smart_data.fields['%s_raw' % attr.get('id')] = \
                                            int(attr.get('raw', {}).get('string', "0").split(" ")[0])
                                    else:
                                        smart_data.fields['%s_raw' % attr.get('id')] = \
                                            attr.get('raw', {}).get('value', 0)

                        if s_val.get('temperature', {}).get('current'):
                            smart_data.fields['CurrentDriveTemperature_raw'] = \
                                int(s_val['temperature']['current'])
                        serial_number = s_val.get('serial_number')
                        wwn = s_val.get("wwn", {})
                        wwpn = ''
                        if wwn:
                            wwpn = '%06X%X' % (wwn.get('oui', 0), wwn.get('id', 0))
                            for k in wwn.keys():
                                if k in ['naa', 't10', 'eui', 'iqn']:
                                    wwpn = ("%X%s" % (wwn[k], wwpn)).lower()
                                    break
                        if wwpn:
                            smart_data.tags['disk_domain_id'] = str(wwpn)
                            smart_data.tags['disk_wwn'] = str(wwpn)
                            if serial_number:
                                smart_data.fields['serial_number'] = str(serial_number)
                            else:
                                smart_data.fields['serial_number'] = str(wwpn)
                        elif serial_number:
                            smart_data.tags['disk_domain_id'] = str(serial_number)
                            smart_data.fields['serial_number'] = str(serial_number)
                            if wwpn:
                                smart_data.tags['disk_wwn'] = str(wwpn)
                            else:
                                smart_data.tags['disk_wwn'] = str(serial_number)
                        else:
                            smart_data.tags['disk_domain_id'] = str(dev_name)
                            smart_data.tags['disk_wwn'] = str(dev_name)
                            smart_data.fields['serial_number'] = str(dev_name)
                        smart_data.fields['agent_version'] = AGENT_VERSION
                        smart_data.tags['primary_key'] = \
                            str('%s%s%s'
                                % (cluster_id,
                                   smart_data.tags['host_domain_id'],
                                   smart_data.tags['disk_domain_id']))
                        self.data.append(smart_data)
