"""
diskprediction with cloud predictor
"""
from __future__ import absolute_import

import base64
from datetime import datetime
import errno
import json
from mgr_module import MgrModule
import os
from string import maketrans
from threading import Event

from .common import DP_MGR_STAT_ENABLED, DP_MGR_STAT_DISABLED
from .task import MetricsRunner, SmartRunner, PredictRunner, TestRunner

TIME_DAYS = 24*60*60
TIME_WEEK = TIME_DAYS * 7
DP_AGENTS = [MetricsRunner, SmartRunner, PredictRunner]
CUSTOMER_ALPHABET = "ABCDEFG&HIJKLMN@OQRS.TUV(WXYZabcd)efghijlmn-opqrstu*vwxyz0123=45"
ORIGIN_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"


def encode_string(value):
    transtable = maketrans(ORIGIN_ALPHABET, CUSTOMER_ALPHABET)
    e = base64.b64encode(value)
    return e.translate(transtable)[:-1]


def decode_string(value):
    transtable = maketrans(CUSTOMER_ALPHABET, ORIGIN_ALPHABET)
    e = str(value).translate(transtable) + "="
    return base64.b64decode(e)


class Module(MgrModule):

    MODULE_OPTIONS = [
        {
            'name': 'diskprediction_server',
            'default': ''
        },
        {
            'name': 'diskprediction_port',
            'default': '31400'
        },
        {
            'name': 'diskprediction_user',
            'default': ''
        },
        {
            'name': 'diskprediction_password',
            'default': ''
        },
        {
            'name': 'diskprediction_upload_metrics_interval',
            'default': '600'
        },
        {
            'name': 'diskprediction_upload_smart_interval',
            'default': '43200'
        },
        {
            'name': 'diskprediction_retrieve_prediction_interval',
            'default': '43200'
        },
        {
            'name': 'diskprediction_cert_context',
            'default': ''
        },
        {
            'name': 'diskprediction_ssl_target_name_override',
            'default': 'api.diskprophet.com'
        },
        {
            'name': 'diskprediction_default_authority',
            'default': 'api.diskprophet.com'
        },
        {
            'name': 'sleep_interval',
            'default': str(600),
        }
    ]

    COMMANDS = [
        {
            'cmd': 'device show-prediction-config',
            'desc': 'Prints diskprediction configuration',
            'perm': 'r'
        },
        {
            'cmd': 'device set-cloud-prediction-config '
                   'name=server,type=CephString,req=true '
                   'name=user,type=CephString,req=true '
                   'name=password,type=CephString,req=true '
                   'name=certfile,type=CephString,req=true '
                   'name=port,type=CephString,req=false ',
            'desc': 'Configure Disk Prediction service',
            'perm': 'rw'
        },
        {
            'cmd': 'device debug metrics-forced',
            'desc': 'Run metrics agent forced',
            'perm': 'r'
        },
        {
            'cmd': 'device debug smart-forced',
            'desc': 'Run smart agent forced',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction_cloud status',
            'desc': 'Check diskprediction_cloud status',
            'perm': 'r'
        }
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.status = {'status': DP_MGR_STAT_DISABLED}
        self._event = Event()
        self._predict_event = Event()
        self._agents = []
        self._activated_cloud = False
        self.prediction_result = {}
        self.config = dict()
        self._run = True

    def config_notify(self):
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']) or opt['default'])
            self.log.debug(' %s = %s', opt['name'], getattr(self, opt['name']))
        if not self._activated_cloud and self.get_ceph_option('device_failure_prediction_mode') == 'cloud':
            self._event.set()
        if self._activated_cloud and self.get_ceph_option('device_failure_prediction_mode') != 'cloud':
            self._event.set()

    @property
    def config_keys(self):
        return dict((o['name'], o.get('default', None)) for o in self.MODULE_OPTIONS)

    def set_config_option(self, option, value):
        if option not in self.config_keys.keys():
            raise RuntimeError('{0} is a unknown configuration '
                               'option'.format(option))

        if option in ['diskprediction_port',
                      'diskprediction_upload_metrics_interval',
                      'diskprediction_upload_smart_interval',
                      'diskprediction_retrieve_prediction_interval']:
            if not str(value).isdigit():
                raise RuntimeError('invalid {} configured. Please specify '
                                   'a valid integer {}'.format(option, value))

        self.log.debug('Setting in-memory config option %s to: %s', option,
                       value)
        self.set_module_option(option, value)
        self.config[option] = value

        return True

    def get_configuration(self, key):
        return self.get_module_option(key, self.config_keys[key])

    @staticmethod
    def _convert_timestamp(predicted_timestamp, life_expectancy_day):
        """
        :param predicted_timestamp: unit is nanoseconds
        :param life_expectancy_day: unit is seconds
        :return:
            date format '%Y-%m-%d' ex. 2018-01-01
        """
        return datetime.fromtimestamp(
            predicted_timestamp / (1000 ** 3) + life_expectancy_day).strftime('%Y-%m-%d')

    def _show_prediction_config(self, cmd):
        self.show_module_config()
        return 0, json.dumps(self.config, indent=4), ''

    def _set_ssl_target_name(self, cmd):
        str_ssl_target = cmd.get('ssl_target_name', '')
        try:
            self.set_module_option('diskprediction_ssl_target_name_override', str_ssl_target)
            return (0,
                    'success to config ssl target name', 0)
        except Exception as e:
            return -errno.EINVAL, '', str(e)

    def _set_ssl_default_authority(self, cmd):
        str_ssl_authority = cmd.get('ssl_authority', '')
        try:
            self.set_module_option('diskprediction_default_authority', str_ssl_authority)
            return 0, 'success to config ssl default authority', 0
        except Exception as e:
            return -errno.EINVAL, '', str(e)

    def _set_cloud_prediction_config(self, cmd):
        str_cert_path = cmd.get('certfile', '')
        if os.path.exists(str_cert_path):
            with open(str_cert_path, 'rb') as f:
                trusted_certs = f.read()
            self.set_config_option(
                'diskprediction_cert_context', trusted_certs)
            for _agent in self._agents:
                _agent.event.set()
            self.set_module_option('diskprediction_server', cmd['server'])
            self.set_module_option('diskprediction_user', cmd['user'])
            self.set_module_option('diskprediction_password', encode_string(cmd['password']))
            if cmd.get('port'):
                self.set_module_option('diskprediction_port', cmd['port'])
            return 0, 'succeed to config cloud mode connection', ''
        else:
            return -errno.EINVAL, '', 'certification file not existed'

    def _debug_metrics_forced(self, cmd):
        msg = ''
        for _agent in self._agents:
            if isinstance(_agent, MetricsRunner):
                msg = 'run metrics agent successfully'
                _agent.event.set()
        return 0, msg, ''

    def _debug_smart_forced(self, cmd):
        msg = ' '
        for _agent in self._agents:
            if isinstance(_agent, SmartRunner):
                msg = 'run smart agent successfully'
                _agent.event.set()
        return 0, msg, ''

    def refresh_config(self):
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']) or opt['default'])
            self.log.debug(' %s = %s', opt['name'], getattr(self, opt['name']))

    def _status(self,  cmd):
        return 0, json.dumps(self.status), ''

    def _refresh_cloud_prediction_result(self):
        for _agent in self._agents:
            if isinstance(_agent, PredictRunner):
                self._predict_event.clear()
                _agent.event.set()
                self._predict_event.wait(300)
                if self._predict_event.is_set():
                    self._predict_event.clear()
                break

    def predict_life_expectancy(self, devid):
        assert devid
        result = self.get('device {}'.format(devid))
        if not result:
            return -1, '', 'device {} not found'.format(devid)
        dev_info = result.get('device', {})
        if not dev_info:
            return -1, '', 'device {} not found'.format(devid)
        self._refresh_cloud_prediction_result()
        prediction_data = self.prediction_result.get(devid)
        if not prediction_data:
            return -1, '', 'device {} prediction data not ready'.format(devid)
        elif prediction_data.get('near_failure', '').lower() == 'good':
            return 0, '>6w', ''
        elif prediction_data.get('near_failure', '').lower() == 'warning':
            return 0, '>=2w and <=6w', ''
        elif prediction_data.get('near_failure', '').lower() == 'bad':
            return 0, '<2w', ''
        else:
            return 0, 'unknown', ''

    def _update_device_life_expectancy_day(self, devid, prediction):
        # Update osd life-expectancy
        from .common.clusterdata import ClusterAPI
        predicted = None
        life_expectancy_day_min = None
        life_expectancy_day_max = None
        if prediction.get('predicted'):
            predicted = int(prediction['predicted'])
        if prediction.get('near_failure'):
            if prediction['near_failure'].lower() == 'good':
                life_expectancy_day_min = (TIME_WEEK * 6) + TIME_DAYS
                life_expectancy_day_max = None
            elif prediction['near_failure'].lower() == 'warning':
                life_expectancy_day_min = (TIME_WEEK * 2)
                life_expectancy_day_max = (TIME_WEEK * 6)
            elif prediction['near_failure'].lower() == 'bad':
                life_expectancy_day_min = 0
                life_expectancy_day_max = (TIME_WEEK * 2) - TIME_DAYS
            else:
                # Near failure state is unknown.
                predicted = None
                life_expectancy_day_min = None
                life_expectancy_day_max = None

        obj_api = ClusterAPI(self)
        if predicted and devid and life_expectancy_day_min is not None:
            from_date = None
            to_date = None
            try:
                if life_expectancy_day_min is not None:
                    from_date = self._convert_timestamp(predicted, life_expectancy_day_min)

                if life_expectancy_day_max is not None:
                    to_date = self._convert_timestamp(predicted, life_expectancy_day_max)

                obj_api.set_device_life_expectancy(devid, from_date, to_date)
                self.log.info(
                    'succeed to set device {} life expectancy from: {}, to: {}'.format(
                        devid, from_date, to_date))
            except Exception as e:
                self.log.error(
                    'failed to set device {} life expectancy from: {}, to: {}, {}'.format(
                        devid, from_date, to_date, str(e)))
        else:
            obj_api.reset_device_life_expectancy(devid)

    def predict_all_devices(self):
        if not self._activated_cloud:
            return -1, '', 'diskprecition_cloud not ready'
        self.refresh_config()
        result = self.get('devices')
        if not result:
            return -1, '', 'unable to get all devices for prediction'
        self._refresh_cloud_prediction_result()
        for dev in result.get('devices', []):
            devid = dev.get('devid')
            if not devid:
                continue
            prediction_data = self.prediction_result.get(devid)
            if prediction_data:
                break
            if not prediction_data:
                return -1, '', 'device {} prediction data not ready'.format(dev.get('devid'))
            else:
                self._update_device_life_expectancy_day(dev.get('devid'), prediction_data)
        return 0, '', ''

    def handle_command(self, _, cmd):
        for o_cmd in self.COMMANDS:
            if cmd['prefix'] == o_cmd['cmd'][:len(cmd['prefix'])]:
                fun_name = ''
                avgs = o_cmd['cmd'].split(' ')
                for avg in avgs:
                    if avg.lower() == 'diskprediction_cloud':
                        continue
                    if avg.lower() == 'device':
                        continue
                    if '=' in avg or ',' in avg or not avg:
                        continue
                    fun_name += '_%s' % avg.replace('-', '_')
                if fun_name:
                    fun = getattr(
                        self, fun_name)
                    if fun:
                        return fun(cmd)
        return -errno.EINVAL, '', 'cmd not found'

    def show_module_config(self):
        for key, default in self.config_keys.items():
            self.set_config_option(key, self.get_module_option(key, default))

    def serve(self):
        self.log.info('Starting diskprediction module')
        self.config_notify()
        self.status = {'status': DP_MGR_STAT_ENABLED}

        while self._run:
            self.refresh_config()
            mode = self.get_ceph_option('device_failure_prediction_mode')
            if mode == 'cloud':
                if not self._activated_cloud:
                    self.start_cloud_disk_prediction()
            else:
                if self._activated_cloud:
                    self.stop_disk_prediction()

            # Check agent hang is?
            restart_agent = False
            try:
                for dp_agent in self._agents:
                    if dp_agent.is_timeout():
                        self.log.error('agent name: {] timeout'.format(dp_agent.task_name))
                        restart_agent = True
                        break
            except Exception as IOError:
                self.log.error('disk prediction plugin faield to started and try to restart')
                restart_agent = True

            if restart_agent:
                self.stop_disk_prediction()
            else:
                sleep_interval = int(self.sleep_interval) or 60
                self._event.wait(sleep_interval)
                self._event.clear()
        self.stop_disk_prediction()

    def _agent_call_back(self):
        self.log.debug('notify refresh devices prediction result')
        self._predict_event.set()

    def start_cloud_disk_prediction(self):
        assert not self._activated_cloud
        for dp_agent in DP_AGENTS:
            if dp_agent == PredictRunner:
                obj_agent = dp_agent(self, 300, self._agent_call_back)
            else:
                obj_agent = dp_agent(self, 300)
            if obj_agent:
                obj_agent.start()
            else:
                raise Exception('failed to start task %s' % obj_agent.task_name)
            self._agents.append(obj_agent)
        self._activated_cloud = True
        self.log.info('start cloud disk prediction')

    def stop_disk_prediction(self):
        assert self._activated_cloud
        try:
            self.status = {'status': DP_MGR_STAT_DISABLED}
            while self._agents:
                dp_agent = self._agents.pop()
                self.log.info('agent name: {}'.format(dp_agent.task_name))
                dp_agent.terminate()
                dp_agent.join(5)
                del dp_agent
            self._activated_cloud = False
            self.log.info('stop disk prediction')
        except Exception as IOError:
            self.log.error('failed to stop disk prediction clould plugin')

    def shutdown(self):
        self._run = False
        self._event.set()
        super(Module, self).shutdown()

    def self_test(self):
        obj_test = TestRunner(self)
        obj_test.run()
        self.log.info('self test completed')
