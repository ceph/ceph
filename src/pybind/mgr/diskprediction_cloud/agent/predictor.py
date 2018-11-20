from __future__ import absolute_import


class PredictAgent(object):

    measurement = 'predictor'

    def __init__(self, mgr_module, obj_sender, timeout=30):
        self.data = []
        self._client = None
        self._client = obj_sender
        self._logger = mgr_module.log
        self._module_inst = mgr_module
        self._timeout = timeout

    def __nonzero__(self):
        if not self._module_inst:
            return False
        else:
            return True

    def run(self):
        result = self._module_inst.get('devices')
        cluster_id = self._module_inst.get('mon_map').get('fsid')
        if not result:
            return -1, '', 'unable to get all devices for prediction'
        for dev in result.get('devices', []):
            for location in dev.get('location', []):
                host = location.get('host')
                host_domain_id = '{}_{}'.format(cluster_id, host)
                prediction_data = self._get_cloud_prediction_result(host_domain_id, dev.get('devid'))
                if prediction_data:
                    self._module_inst.prediction_result[dev.get('devid')] = prediction_data

    def _get_cloud_prediction_result(self, host_domain_id, disk_domain_id):
        result = {}
        try:
            query_info = self._client.query_info(host_domain_id, disk_domain_id, 'sai_disk_prediction')
            status_code = query_info.status_code
            if status_code == 200:
                result = query_info.json()
            else:
                resp = query_info.json()
                if resp.get('error'):
                    self._logger.error(str(resp['error']))
        except Exception as e:
            self._logger.error('failed to get %s prediction result %s' % (disk_domain_id, str(e)))
        return result
