from __future__ import absolute_import

import socket

from . import MetricsAgent, AGENT_VERSION
from ...common.db import DB_API
from ...models.metrics.dp import SAI_Disk


def get_human_readable(size, precision=2):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    suffix_index = 0
    while size > 1024 and suffix_index < 4:
        # increment the index of the suffix
        suffix_index += 1
        # apply the division
        size = size/1000.0
    return "%.*d %s" % (precision, size, suffixes[suffix_index])


class SAI_DiskAgent(MetricsAgent):
    measurement = 'sai_disk'

    @staticmethod
    def _get_disk_type(is_ssd, vendor, model):
        """ return type:
            0: "Unknown", 1: "HDD",
            2: "SSD",     3: "SSD NVME",
            4: "SSD SAS", 5: "SSD SATA",
            6: "HDD SAS", 7: "HDD SATA"
        """
        if is_ssd:
            if vendor:
                disk_type = 4
            elif model:
                disk_type = 5
            else:
                disk_type = 2
        else:
            if vendor:
                disk_type = 6
            elif model:
                disk_type = 7
            else:
                disk_type = 1
        return disk_type

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
                d_data = SAI_Disk()
                d_data.tags['disk_name'] = str(dev_name)
                d_data.fields['cluster_domain_id'] = str(cluster_id)
                d_data.fields['host_domain_id'] = \
                    str("%s_%s"
                        % (cluster_id, osds_meta.get("hostname", "None")))
                d_data.tags['agenthost'] = str(socket.gethostname())
                d_data.tags['agenthost_domain_id'] = \
                    str("%s_%s" % (cluster_id, d_data.tags['agenthost']))
                serial_number = s_val.get("serial_number")
                wwn = s_val.get("wwn", {})
                wwpn = ''
                if wwn:
                    wwpn = '%06X%X' % (wwn.get('oui', 0), wwn.get('id', 0))
                    for k in wwn.keys():
                        if k in ['naa', 't10', 'eui', 'iqn']:
                            wwpn = ("%X%s" % (wwn[k], wwpn)).lower()
                            break

                if wwpn:
                    d_data.tags['disk_domain_id'] = str(wwpn)
                    d_data.tags['disk_wwn'] = str(wwpn)
                    if serial_number:
                        d_data.fields['serial_number'] = str(serial_number)
                    else:
                        d_data.fields['serial_number'] = str(wwpn)
                elif serial_number:
                    d_data.tags['disk_domain_id'] = str(serial_number)
                    d_data.fields['serial_number'] = str(serial_number)
                    if wwpn:
                        d_data.tags['disk_wwn'] = str(wwpn)
                    else:
                        d_data.tags['disk_wwn'] = str(serial_number)
                else:
                    d_data.tags['disk_domain_id'] = str(dev_name)
                    d_data.tags['disk_wwn'] = str(dev_name)
                    d_data.fields['serial_number'] = str(dev_name)
                d_data.tags['primary_key'] = \
                    str("%s%s%s"
                        % (cluster_id, d_data.fields['host_domain_id'],
                           d_data.tags['disk_domain_id']))
                d_data.fields['disk_status'] = int(1)
                is_ssd = True if s_val.get('rotation_rate') == 0 else False
                vendor = s_val.get('vendor', None)
                model = s_val.get('model_name', None)
                d_data.fields['disk_type'] = \
                    self._get_disk_type(is_ssd, vendor, model)
                d_data.fields['firmware_version'] = \
                    str(s_val.get('firmware_version'))
                if model:
                    d_data.fields['model'] = str(model)
                if vendor:
                    d_data.fields['vendor'] = str(vendor)
                if s_val.get('sata_version', {}).get('string'):
                    d_data.fields['sata_version'] = \
                        str(s_val['sata_version']['string'])
                if s_val.get('logical_block_size'):
                    d_data.fields['sector_size'] = \
                        str(str(s_val['logical_block_size']))
                d_data.fields['transport_protocol'] = str('')
                d_data.fields['vendor'] = \
                    str(s_val.get('model_family', '')).replace("\"", "'")
                d_data.fields['agent_version'] = str(AGENT_VERSION)
                d_data.fields['size'] = \
                    get_human_readable(
                        int(s_val.get('user_capacity', '0')), 0)
                if s_val.get('smart_status', {}).get('passed'):
                    d_data.fields['smart_health_status'] = 'PASSED'
                else:
                    d_data.fields['smart_health_status'] = 'FAILED'
                self.data.append(d_data)
