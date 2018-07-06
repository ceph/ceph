from __future__ import absolute_import

import socket

from . import MetricsAgent, AGENT_VERSION
from ...common.db import DB_API
from ...models.metrics.dp import SAI_Disk


def get_human_readable(size, precision=2):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    suffix_index = 0
    while size > 1000 and suffix_index < 4:
        # increment the index of the suffix
        suffix_index += 1
        # apply the division
        size = size/1000.0
    return "%.*d %s" % (precision, size, suffixes[suffix_index])


class SAI_DiskAgent(MetricsAgent):
    measurement = 'sai_disk'

    @staticmethod
    def _convert_disk_type(is_ssd, sata_version, protocol):
        """ return type:
            0: "Unknown", 1: "HDD",
            2: "SSD",     3: "SSD NVME",
            4: "SSD SAS", 5: "SSD SATA",
            6: "HDD SAS", 7: "HDD SATA"
        """
        disk_type = 0
        if is_ssd:
            if sata_version and not protocol:
                disk_type = 5
            elif 'SCSI' in protocol:
                disk_type = 4
            elif 'NVMe' in protocol:
                disk_type = 3
            else:
                disk_type = 2
        else:
            if sata_version and not protocol:
                disk_type = 7
            elif 'SCSI' in protocol:
                disk_type = 6
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
            if not osd.get('in'):
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
                d_data.tags['host_domain_id'] = \
                    str("%s_%s"
                        % (cluster_id, osds_meta.get("hostname", "None")))
                d_data.fields['agenthost'] = str(socket.gethostname())
                d_data.tags['agenthost_domain_id'] = \
                    str("%s_%s" % (cluster_id, d_data.fields['agenthost']))
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
                        % (cluster_id, d_data.tags['host_domain_id'],
                           d_data.tags['disk_domain_id']))
                d_data.fields['disk_status'] = int(1)
                is_ssd = True if s_val.get('rotation_rate') == 0 else False
                vendor = s_val.get('vendor', None)
                model = s_val.get('model_name', None)
                if s_val.get('sata_version', {}).get('string'):
                    sata_version = s_val['sata_version']['string']
                else:
                    sata_version = ''
                if s_val.get('device', {}).get('protocol'):
                    protocol = s_val['device']['protocol']
                else:
                    protocol = ''
                d_data.fields['disk_type'] = \
                    self._convert_disk_type(is_ssd, sata_version, protocol)
                d_data.fields['firmware_version'] = \
                    str(s_val.get('firmware_version'))
                if model:
                    d_data.fields['model'] = str(model)
                if vendor:
                    d_data.fields['vendor'] = str(vendor)
                if sata_version:
                    d_data.fields['sata_version'] = str(sata_version)
                if s_val.get('logical_block_size'):
                    d_data.fields['sector_size'] = \
                        str(str(s_val['logical_block_size']))
                d_data.fields['transport_protocol'] = str('')
                d_data.fields['vendor'] = \
                    str(s_val.get('model_family', '')).replace("\"", "'")
                d_data.fields['agent_version'] = str(AGENT_VERSION)
                try:
                    if isinstance(s_val.get('user_capacity'), dict):
                        user_capacity = \
                            s_val['user_capacity'].get('bytes', {}).get('n', 0)
                    else:
                        user_capacity = s_val.get('user_capacity', 0)
                except ValueError:
                    user_capacity = 0
                d_data.fields['size'] = \
                    get_human_readable(int(user_capacity), 0)

                if s_val.get('smart_status', {}).get('passed'):
                    d_data.fields['smart_health_status'] = 'PASSED'
                else:
                    d_data.fields['smart_health_status'] = 'FAILED'
                self.data.append(d_data)
