# vim: tabstop=4 shiftwidth=4 softtabstop=4
# from __future__ import absolute_import
from __future__ import absolute_import
from logging import getLogger
import time

from . import DummyResonse
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
        for dev_id in self.mgr_inst.history_smart.keys():
            if dev_id not in [x.tags['disk_domain_id'] for x in data]:
                del self.mgr_inst.history_smart[dev_id]
        return status_info

    def _send_info(self, data, measurement):
        resp = DummyResonse()
        resp.status_code = 200
        resp.content = ''
        # Ignore none disk smart table.
        if measurement != 'sai_disk_smart':
            return resp
        dev_id = data.tags['disk_domain_id']
        if dev_id not in self.mgr_inst.history_smart.keys():
            self.mgr_inst.history_smart[dev_id] = list()

        if data.timestamp not in [x.timestamp for x in self.mgr_inst.history_smart[dev_id]]:
            self.mgr_inst.history_smart[dev_id].append(data)
            # Maximum keep data 10
            if len(self.mgr_inst.history_smart[dev_id]) > 10:
                self.mgr_inst.history_smart[dev_id].pop(0)
        return resp

    def _local_predict(self, smart_datas):
        obj_predictor = DiskFailurePredictor()
        predictor_path = get_diskfailurepredictor_path()
        models_path = "{}/models".format(predictor_path)
        obj_predictor.initialize(models_path)
        return obj_predictor.predict(smart_datas)

    def query_info(self, host_domain_id, disk_domain_id, measurement):
        predicted_result = 'Unknown'
        smart_datas = self.mgr_inst.history_smart.get(disk_domain_id, [])
        if len(smart_datas) >= 6:
            predict_datas = list()
            for s_data in smart_datas:
                predict_data = {}
                for field in s_data.fields:
                    if '_raw' == field[-4:] and str(field[0:-4]).isdigit():
                        predict_data['smart_{}'.format(field)] = s_data.fields[field]
                if predict_data:
                    predict_datas.append(predict_data)
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
            resp.resp_json = \
                {'error': '\'predict\' need least 6 pieces disk smart data'}
        return resp
