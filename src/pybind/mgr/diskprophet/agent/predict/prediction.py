from __future__ import absolute_import

import json
import os
import socket
import sys
import time

from .. import BaseAgent
from ...common.db import DB_API


PREDICTION_FILE = '/var/tmp/disk_prediction.json'

test_json = '{"results":[{"statement_id":0,"series":[{"name":"sai_disk_prediction","columns":["time","cluster_domain_id","confidence","disk_domain_id","disk_model","disk_name","disk_serial_number","disk_type","disk_vendor","host_domain_id","life_expectancy","life_expectancy_day","near_failure","predicted","primary_key"],"values":[["2018-05-25T01:26:18.231490725Z","dpCluster",100,"55cd2e404b7ee6d3","INTEL SSDSC2BP480G4","MegaraidDisk-0","BTJR516601GW480BGN","5","","da24c5fac654244dccffeb6b564b139b",24,730,"Good",1527211578228188894,"dpCluster-da24c5fac654244dccffeb6b564b139b-55cd2e404b7ee6d3"]]}]}]}'

class Prediction_Agent(BaseAgent):
    measurement = 'sai_disk_prediction'

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

    def _read_prediction_file(self):
        self._logger.info("Read prediction file.")
        data = {}
        if os.path.isfile(PREDICTION_FILE):
            try:
                with open(PREDICTION_FILE, 'r') as f:
                    data = json.load(f)
            except Exception as e:
                self._logger.error(str(e))
        return data

    def _write_prediction_file(self, result):
        self._logger.info("Write prediction file.")
        try:
            with open(PREDICTION_FILE, 'w') as f:
                json.dump(result, f)
        except Exception as e:
            self._logger.error(str(e))

    def _parse_prediction_data(self, host_domain_id, dev_name):
        result = {}

        sql = "SELECT * FROM \"%s\" WHERE (%s) ORDER BY time DESC LIMIT 1"
        where = "host_domain_id=\'%s\' AND disk_name=\'%s\'" % (host_domain_id, dev_name)
        try:
            query_info = self._command.query_info(sql % (self.__class__.measurement, where))
            status_code = query_info.status_code
            if status_code >= 200 and status_code < 300:
                resp = query_info.json()
                rc = resp.get('results', [])
                if rc:
                    series = rc[0].get('series', [])
                    if series:
                        values = series[0].get('values', [])
                        if not values:
                            return result

                        disk_id_idx = 0
                        columns = series[0].get('columns', [])
                        for _idx, _val in enumerate(columns):
                            if _val == 'disk_domain_id':
                                disk_id_idx = _idx

                        if not disk_id_idx:
                            return result

                        for item in values:
                            # get prediction key and value from server.
                            for name, value in zip(columns, item):
                                # process prediction data
                                result[name] = value
            else:
                resp = query_info.json()
                if resp.get('error'):
                    self._logger.error(str(resp['error']))
        except Exception as e:
            self._logger.error(str(e))
        return result

    def _effected_resource(self, osd_id):
        result = {'pool': [], 'rbd': []}

        obj_api = DB_API(self._ceph_context)
        pools = obj_api.get_osd_pools()
        effected_pool = set()
        effected_rbd = set()
        for pool_data in pools:
            pool_name = pool_data.get('pool_name')
            rbd_list = obj_api.get_rbd_list(pool_name=pool_name)
            for rbd_data in rbd_list:
                image_name = rbd_data.get('name')
                rbd_info = obj_api.get_rbd_info(pool_name, image_name)
                rbd_pgids = rbd_info.get('pgs', [])
                for _data in rbd_pgids:
                    acting = _data.get('acting', [])
                    if osd_id in acting:
                        effected_pool.add(pool_name)
                        effected_rbd.add(image_name)

        result['pool'] = map(lambda x: x, effected_pool)
        result['rbd'] = map(lambda x: x, effected_rbd)
        return result

    def _fetch_prediction_result(self):
        obj_api = DB_API(self._ceph_context)
        cluster_id =  obj_api.get_cluster_id()

        result = {cluster_id: {}}
        osds = obj_api.get_osds()
        for osd in osds:
            osd_id = osd.get('osd')
            osd_uuid = osd.get('uuid')
            if osd_id is None:
                continue
            osds_meta = obj_api.get_osd_metadata(osd_id)
            if not osds_meta:
                continue
            osds_smart = obj_api.get_osd_smart(osd_id)
            if not osds_smart:
                continue

            osd_key = 'osd.{}'.format(osd_id)
            hostname = osds_meta.get("hostname", "None")
            host_domain_id = "%s_%s" % (cluster_id, hostname)
            result[cluster_id].update({
                hostname: {'osd': {}}
            })
            collect_data = result[cluster_id][hostname]['osd']

            info_list = []
            for dev_name, s_val in osds_smart.iteritems():
                is_ssd = True if s_val.get('rotation_rate') == 0 else False
                vendor = s_val.get('vendor', '')
                model = s_val.get('model_name', '')
                disk_type = self._get_disk_type(is_ssd, vendor, model)
                serial_number = s_val.get("serial_number")
                wwn = s_val.get("wwn", {}).get("id")
                tmp = {}
                if wwn:
                    tmp['disk_domain_id'] = wwn
                    tmp['disk_wwn'] = wwn
                    if serial_number:
                        tmp['serial_number'] = serial_number
                    else:
                        tmp['serial_number'] = wwn
                elif serial_number:
                    tmp['disk_domain_id'] = serial_number
                    tmp['serial_number'] = serial_number
                    if wwn:
                        tmp['disk_wwn'] = wwn
                    else:
                        tmp['disk_wwn'] = serial_number
                else:
                    tmp['disk_domain_id'] = dev_name
                    tmp['disk_wwn'] = dev_name
                    tmp['serial_number'] = dev_name

                if s_val.get('smart_status', {}).get("passed"):
                    tmp['smart_health_status'] = 'OK'
                else:
                    tmp['smart_health_status'] = 'FAIL'

                tmp['sata_version'] = s_val.get('sata_version', {}).get('string', '')
                tmp['sector_size'] = str(s_val.get('logical_block_size', ''))
                disk_info = {
                    'diskName': dev_name,
                    'diskType': str(disk_type),
                    'diskStatus': '1',
                    'diskWWN': tmp['disk_wwn'],
                    'dpDiskId': tmp['disk_domain_id'],
                    'serialNumber': tmp['serial_number'],
                    'vendor': vendor,
                    'sataVersion': tmp['sata_version'],
                    'smartHealthStatus': tmp['smart_health_status'],
                    'sectorSize': tmp['sector_size'],
                    'size': str(s_val.get('user_capacity', '0')),
                    'prediction': self._parse_prediction_data(host_domain_id, dev_name)
                }
                info_list.append(disk_info)

            effected = self._effected_resource(osd_id)
            collect_data.update({
                'host': hostname,
                'last_time': int(time.time()),
                osd_key: {
                    'id': osd_id,
                    'uuid': osd_uuid,
                    'physicalDisks': info_list,
                    'effectedRBD': effected['rbd'],
                    'effectedPool': effected['pool']}
            })

        mons = obj_api.get_mons()
        for mon in mons:
            mon_host = mon.get('name', '')
            mon_key = 'mon.{}'.format(mon_host)
            if result[cluster_id].get(mon_host, {}):
                result[cluster_id][mon_host].update({
                    'mon': {mon_key: {}}
                })
            else:
                result[cluster_id].update({
                    mon_host: {
                        'mon': {mon_key: {}}
                    }
                })

        file_systems = obj_api.get_file_systems()
        for _data in file_systems:
            mds_info = _data.get('mdsmap').get('info')
            for _gid in mds_info:
                mds_data = mds_info[_gid]
                mds_host = mds_data.get('name')
                mds_key = 'mds.{}'.format(_gid)
                if result[cluster_id].get(mds_host, {}):
                    result[cluster_id][mds_host].update({
                        'mds': {mds_key: {}}
                    })
                else:
                    result[cluster_id].update({
                        mds_host: {
                            'mds': {mds_key: {}}
                        }
                    })

        return result


    def run(self):
        result = self._fetch_prediction_result()
        if result:
            self._write_prediction_file(result)
