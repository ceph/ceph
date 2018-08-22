# vim: tabstop=4 shiftwidth=4 softtabstop=4
# from __future__ import absolute_import
from __future__ import absolute_import
from logging import getLogger
import time

from . import DummyResonse
from .clusterdata import ClusterAPI
from ..predictor.DiskFailurePredictor import DiskFailurePredictor, get_diskfailurepredictor_path


def gen_configuration(**kwargs):
    configuration = {
        'mgr_inst': kwargs.get('mgr_inst', None)}
    return configuration


class LocalPredictor:

    def __init__(self, configuration):
        self.mgr_inst = configuration.get('mgr_inst')
        if self.mgr_inst:
            self._logger = self.mgr_inst.log
        else:
            self._logger = getLogger()

    def __nonzero__(self):
        if self.mgr_inst:
            return True
        else:
            return False

    def test_connection(self):
        resp = DummyResonse()
        resp.status_code = 200
        resp.content = ''
        return resp

    def send_info(self, data, measurement):
        status_info = dict()
        status_info['measurement'] = measurement
        status_info['success_count'] = 0
        status_info['failure_count'] = 0
        for dp_data in data:
            try:
                resp = self._send_info(data=dp_data, measurement=measurement)
                status_code = resp.status_code
                if 200 <= status_code < 300:
                    self._logger.debug(
                        '%s send diskprediction api success(ret: %s)'
                        % (measurement, status_code))
                    status_info['success_count'] += 1
                else:
                    self._logger.error(
                        'return code: %s, content: %s, data: %s' % (
                            status_code, resp.content, data))
                    status_info['failure_count'] += 1
            except Exception as e:
                status_info['failure_count'] += 1
                self._logger.error(str(e))
        return status_info

    def _send_info(self, data, measurement):
        resp = DummyResonse()
        resp.status_code = 200
        resp.content = ''
        return resp

    def _local_predict(self, smart_datas):
        obj_predictor = DiskFailurePredictor()
        predictor_path = get_diskfailurepredictor_path()
        models_path = "{}/models".format(predictor_path)
        obj_predictor.initialize(models_path)
        return obj_predictor.predict(smart_datas)

    def query_info(self, host_domain_id, disk_domain_id, measurement):
        predict_datas = list()
        obj_api = ClusterAPI(self.mgr_inst)
        predicted_result = 'Unknown'
        smart_datas = obj_api.get_device_health(disk_domain_id)
        if len(smart_datas) >= 6:
            o_keys = sorted(smart_datas.iterkeys(), reverse=True)
            for o_key in o_keys:
                dev_smart = {}
                s_val = smart_datas[o_key]
                ata_smart = s_val.get('ata_smart_attributes', {})
                for attr in ata_smart.get('table', []):
                    if attr.get('raw', {}).get('string'):
                        if str(attr.get('raw', {}).get('string', '0')).isdigit():
                            dev_smart['smart_%s_raw' % attr.get('id')] = \
                                int(attr.get('raw', {}).get('string', '0'))
                        else:
                            if str(attr.get('raw', {}).get('string', '0')).split(' ')[0].isdigit():
                                dev_smart['smart_%s_raw' % attr.get('id')] = \
                                    int(attr.get('raw', {}).get('string',
                                                                '0').split(' ')[0])
                            else:
                                dev_smart['smart_%s_raw' % attr.get('id')] = \
                                    attr.get('raw', {}).get('value', 0)
                if s_val.get('hours_powered_up') is not None:
                    dev_smart['smart_9_raw'] = int(s_val['hours_powered_up'])
                if dev_smart:
                    predict_datas.append(dev_smart)

            if predict_datas:
                predicted_result = self._local_predict(predict_datas)
            resp = DummyResonse()
            resp.status_code = 200
            resp.resp_json = {
                "disk_domain_id": disk_domain_id,
                "near_failure": predicted_result,
                "predicted": int(time.time() * (1000 ** 3))}
            return resp
        else:
            resp = DummyResonse()
            resp.status_code = 400
            resp.content = '\'predict\' need least 6 pieces disk smart data'
            resp.resp_json = \
                {'error': '\'predict\' need least 6 pieces disk smart data'}
        return resp
