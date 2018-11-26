# vim: tabstop=4 shiftwidth=4 softtabstop=4
import grpc
import json
from logging import getLogger
import time

from . import DummyResonse
from . import client_pb2
from . import client_pb2_grpc


def gen_configuration(**kwargs):
    configuration = {
        'host': kwargs.get('host', 'api.diskprophet.com'),
        'user': kwargs.get('user'),
        'password': kwargs.get('password'),
        'port': kwargs.get('port', 31400),
        'mgr_inst': kwargs.get('mgr_inst', None),
        'cert_context': kwargs.get('cert_context'),
        'ssl_target_name': kwargs.get('ssl_target_name', 'api.diskprophet.com'),
        'default_authority': kwargs.get('default_authority', 'api.diskprophet.com')}
    return configuration


class GRPcClient(object):

    def __init__(self, configuration):
        self.auth = None
        self.host = configuration.get('host')
        self.port = configuration.get('port')
        if configuration.get('user') and configuration.get('password'):
            self.auth = (
                ('account', configuration.get('user')),
                ('password', configuration.get('password')))
        self.cert_context = configuration.get('cert_context')
        self.ssl_target_name = configuration.get('ssl_target_name')
        self.default_authority = configuration.get('default_authority')
        self.mgr_inst = configuration.get('mgr_inst')
        if self.mgr_inst:
            self._logger = self.mgr_inst.log
        else:
            self._logger = getLogger()
        self._client = self._get_channel()

    def close(self):
        if self._client:
            self._client.close()

    @staticmethod
    def connectivity_update(connectivity):
        pass

    def _get_channel(self):
        try:
            creds = grpc.ssl_channel_credentials(
                root_certificates=self.cert_context)
            channel = \
                grpc.secure_channel('{}:{}'.format(
                    self.host, self.port), creds,
                    options=(('grpc.ssl_target_name_override', self.ssl_target_name,),
                             ('grpc.default_authority', self.default_authority),))
            channel.subscribe(self.connectivity_update, try_to_connect=True)
            return channel
        except Exception as e:
            self._logger.error(
                'failed to create connection exception: {}'.format(
                    ';'.join(str(e).split('\n\t'))))
            return None

    def test_connection(self):
        try:
            stub_accout = client_pb2_grpc.AccountStub(self._client)
            result = stub_accout.AccountHeartbeat(client_pb2.Empty())
            self._logger.debug('text connection result: {}'.format(str(result)))
            if result and "is alive" in str(result.message):
                return True
            else:
                return False
        except Exception as e:
            self._logger.error(
                'failed to test connection exception: {}'.format(
                    ';'.join(str(e).split('\n\t'))))
            return False

    def _send_metrics(self, data, measurement):
        status_info = dict()
        status_info['measurement'] = None
        status_info['success_count'] = 0
        status_info['failure_count'] = 0
        for dp_data in data:
            d_measurement = dp_data.measurement
            if not d_measurement:
                status_info['measurement'] = measurement
            else:
                status_info['measurement'] = d_measurement
            tag_list = []
            field_list = []
            for name in dp_data.tags:
                tag = '{}={}'.format(name, dp_data.tags[name])
                tag_list.append(tag)
            for name in dp_data.fields:
                if dp_data.fields[name] is None:
                    continue
                if isinstance(dp_data.fields[name], str):
                    field = '{}=\"{}\"'.format(name, dp_data.fields[name])
                elif isinstance(dp_data.fields[name], bool):
                    field = '{}={}'.format(name,
                                           str(dp_data.fields[name]).lower())
                elif (isinstance(dp_data.fields[name], int) or
                      isinstance(dp_data.fields[name], long)):
                    field = '{}={}i'.format(name, dp_data.fields[name])
                else:
                    field = '{}={}'.format(name, dp_data.fields[name])
                field_list.append(field)
            data = '{},{} {} {}'.format(
                status_info['measurement'],
                ','.join(tag_list),
                ','.join(field_list),
                int(time.time() * 1000 * 1000 * 1000))
            try:
                resp = self._send_info(data=[data], measurement=status_info['measurement'])
                status_code = resp.status_code
                if 200 <= status_code < 300:
                    self._logger.debug(
                        '{} send diskprediction api success(ret: {})'.format(
                            status_info['measurement'], status_code))
                    status_info['success_count'] += 1
                else:
                    self._logger.error(
                        'return code: {}, content: {}'.format(
                            status_code, resp.content))
                    status_info['failure_count'] += 1
            except Exception as e:
                status_info['failure_count'] += 1
                self._logger.error(str(e))
        return status_info

    def _send_db_relay(self, data, measurement):
        status_info = dict()
        status_info['measurement'] = measurement
        status_info['success_count'] = 0
        status_info['failure_count'] = 0
        for dp_data in data:
            try:
                resp = self._send_info(
                    data=[dp_data.fields['cmd']], measurement=measurement)
                status_code = resp.status_code
                if 200 <= status_code < 300:
                    self._logger.debug(
                        '{} send diskprediction api success(ret: {})'.format(
                            measurement, status_code))
                    status_info['success_count'] += 1
                else:
                    self._logger.error(
                        'return code: {}, content: {}'.format(
                            status_code, resp.content))
                    status_info['failure_count'] += 1
            except Exception as e:
                status_info['failure_count'] += 1
                self._logger.error(str(e))
        return status_info

    def send_info(self, data, measurement):
        """
        :param data: data structure
        :param measurement: data measurement class name
        :return:
            status_info = {
                'success_count': <count>,
                'failure_count': <count>
            }
        """
        if measurement == 'db_relay':
            return self._send_db_relay(data, measurement)
        else:
            return self._send_metrics(data, measurement)

    def _send_info(self, data, measurement):
        resp = DummyResonse()
        try:
            stub_collection = client_pb2_grpc.CollectionStub(self._client)
            if measurement == 'db_relay':
                result = stub_collection.PostDBRelay(
                    client_pb2.PostDBRelayInput(cmds=data), metadata=self.auth)
            else:
                result = stub_collection.PostMetrics(
                    client_pb2.PostMetricsInput(points=data), metadata=self.auth)
            if result and 'success' in str(result.message).lower():
                resp.status_code = 200
                resp.content = ''
            else:
                resp.status_code = 400
                resp.content = ';'.join(str(result).split('\n\t'))
                self._logger.error(
                    'failed to send info: {}'.format(resp.content))
        except Exception as e:
            resp.status_code = 400
            resp.content = ';'.join(str(e).split('\n\t'))
            self._logger.error(
                'failed to send info exception: {}'.format(resp.content))
        return resp

    def query_info(self, host_domain_id, disk_domain_id, measurement):
        resp = DummyResonse()
        try:
            stub_dp = client_pb2_grpc.DiskprophetStub(self._client)
            predicted = stub_dp.DPGetDisksPrediction(
                client_pb2.DPGetDisksPredictionInput(
                    physicalDiskIds=disk_domain_id),
                metadata=self.auth)
            if predicted and hasattr(predicted, 'data'):
                resp.status_code = 200
                resp.content = ''
                resp_json = json.loads(predicted.data)
                rc = resp_json.get('results', [])
                if rc:
                    series = rc[0].get('series', [])
                    if series:
                        values = series[0].get('values', [])
                        if not values:
                            resp.resp_json = {}
                        else:
                            columns = series[0].get('columns', [])
                            for item in values:
                                # get prediction key and value from server.
                                for name, value in zip(columns, item):
                                    # process prediction data
                                    resp.resp_json[name] = value
                self._logger.debug("query {}:{} result:{}".format(host_domain_id, disk_domain_id, resp))
                return resp
            else:
                resp.status_code = 400
                resp.content = ''
                resp.resp_json = {'error': ';'.join(str(predicted).split('\n\t'))}
                self._logger.debug("query {}:{} result:{}".format(host_domain_id, disk_domain_id, resp))
                return resp
        except Exception as e:
            resp.status_code = 400
            resp.content = ';'.join(str(e).split('\n\t'))
            resp.resp_json = {'error': resp.content}
            self._logger.debug("query {}:{} result:{}".format(host_domain_id, disk_domain_id, resp))
            return resp
