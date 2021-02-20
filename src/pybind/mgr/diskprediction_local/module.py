"""
diskprediction with local predictor
"""
import json
import datetime
import _strptime
from threading import Event
import time

from mgr_module import MgrModule, CommandResult

# Importing scipy early appears to avoid a future deadlock when
# we try to do
#
#  from .predictor import get_diskfailurepredictor_path
#
# in a command thread.  See https://tracker.ceph.com/issues/42764
import scipy


TIME_FORMAT = '%Y%m%d-%H%M%S'
TIME_DAYS = 24*60*60
TIME_WEEK = TIME_DAYS * 7


class Module(MgrModule):
    MODULE_OPTIONS = [
        {
            'name': 'sleep_interval',
            'default': str(600),
        },
        {
            'name': 'predict_interval',
            'default': str(86400),
        },
        {
            'name': 'predictor_model',
            'default': 'prophetstor',
        },
    ]

    COMMANDS = []

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        # options
        for opt in self.MODULE_OPTIONS:
            setattr(self, opt['name'], opt['default'])
        # other
        self._run = True
        self._event = Event()

    def config_notify(self):
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' %s = %s', opt['name'], getattr(self, opt['name']))
        if self.get_ceph_option('device_failure_prediction_mode') == 'local':
            self._event.set()

    def refresh_config(self):
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' %s = %s', opt['name'], getattr(self, opt['name']))

    def handle_command(self, _, cmd):
        self.log.debug('handle_command cmd: %s', cmd)
        raise NotImplementedError(cmd['prefix'])

    def self_test(self):
        self.log.debug('self_test enter')
        ret, out, err = self.predict_all_devices()
        assert ret == 0
        return 0, 'self test succeed', ''

    def serve(self):
        self.log.info('Starting diskprediction local module')
        self.config_notify()
        last_predicted = None
        ls = self.get_store('last_predicted')
        if ls:
            try:
                last_predicted = datetime.datetime.strptime(ls, TIME_FORMAT)
            except ValueError:
                pass
        self.log.debug('Last predicted %s', last_predicted)

        while self._run:
            self.refresh_config()
            mode = self.get_ceph_option('device_failure_prediction_mode')
            if mode == 'local':
                now = datetime.datetime.utcnow()
                if not last_predicted:
                    next_predicted = now
                else:
                    predicted_frequency = int(self.predict_interval) or 86400
                    seconds = (last_predicted - datetime.datetime.utcfromtimestamp(0)).total_seconds()
                    seconds -= seconds % predicted_frequency
                    seconds += predicted_frequency
                    next_predicted = datetime.datetime.utcfromtimestamp(seconds)
                    if last_predicted:
                        self.log.debug('Last scrape %s, next scrape due %s',
                                       last_predicted.strftime(TIME_FORMAT),
                                       next_predicted.strftime(TIME_FORMAT))
                    else:
                        self.log.debug('Last scrape never, next scrape due %s',
                                       next_predicted.strftime(TIME_FORMAT))
                if now >= next_predicted:
                    self.predict_all_devices()
                    last_predicted = now
                    self.set_store('last_predicted', last_predicted.strftime(TIME_FORMAT))

            sleep_interval = int(self.sleep_interval) or 60
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            self._event.wait(sleep_interval)
            self._event.clear()

    def shutdown(self):
        self.log.info('Stopping')
        self._run = False
        self._event.set()

    @staticmethod
    def _convert_timestamp(predicted_timestamp, life_expectancy_day):
        """
        :param predicted_timestamp: unit is nanoseconds
        :param life_expectancy_day: unit is seconds
        :return:
            date format '%Y-%m-%d' ex. 2018-01-01
        """
        return datetime.datetime.fromtimestamp(
            predicted_timestamp / (1000 ** 3) + life_expectancy_day).strftime('%Y-%m-%d')

    def _predict_life_expentancy(self, devid):
        predicted_result = ''
        health_data = {}
        predict_datas = []
        try:
            r, outb, outs = self.remote('devicehealth', 'show_device_metrics', devid=devid, sample='')
            if r != 0:
                self.log.error('failed to get device %s health', devid)
                health_data = {}
            else:
                health_data = json.loads(outb)
        except Exception as e:
            self.log.error('failed to get device %s health data due to %s', devid, str(e))

        # initialize appropriate disk failure predictor model
        from .predictor import get_diskfailurepredictor_path
        if self.predictor_model == 'prophetstor':
            from .predictor import PSDiskFailurePredictor
            obj_predictor = PSDiskFailurePredictor()
            ret = obj_predictor.initialize("{}/models/{}".format(get_diskfailurepredictor_path(), self.predictor_model))
            if ret is not None:
                self.log.error('Error initializing predictor')
                return predicted_result
        elif self.predictor_model == 'redhat':
            from .predictor import RHDiskFailurePredictor
            obj_predictor = RHDiskFailurePredictor()
            ret = obj_predictor.initialize("{}/models/{}".format(get_diskfailurepredictor_path(), self.predictor_model))
            if ret is not None:
                self.log.error('Error initializing predictor')
                return predicted_result
        else:
            self.log.error('invalid value received for MODULE_OPTIONS.predictor_model')
            return predicted_result

        if len(health_data) >= 6:
            o_keys = sorted(health_data.keys(), reverse=True)
            for o_key in o_keys:
                # get values for current day (?)
                dev_smart = {}
                s_val = health_data[o_key]

                # add all smart attributes
                ata_smart = s_val.get('ata_smart_attributes', {})
                for attr in ata_smart.get('table', []):
                    # get raw smart values
                    if attr.get('raw', {}).get('string') is not None:
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
                    # get normalized smart values
                    if attr.get('value') is not None:
                        dev_smart['smart_%s_normalized' % attr.get('id')] = \
                                    attr.get('value')
                # add power on hours manually if not available in smart attributes
                if s_val.get('power_on_time', {}).get('hours') is not None:
                    dev_smart['smart_9_raw'] = int(s_val['power_on_time']['hours'])
                # add device capacity
                if s_val.get('user_capacity') is not None:
                    if s_val.get('user_capacity').get('bytes') is not None:
                        dev_smart['user_capacity'] = s_val.get('user_capacity').get('bytes')
                    else:
                        self.log.debug('user_capacity not found in smart attributes list')
                # add device model
                if s_val.get('model_name') is not None:
                    dev_smart['model_name'] = s_val.get('model_name')
                # add vendor
                if s_val.get('vendor') is not None:
                    dev_smart['vendor'] = s_val.get('vendor')
                # if smart data was found, then add that to list
                if dev_smart:
                    predict_datas.append(dev_smart)
                if len(predict_datas) >= 12:
                    break
        else:
            self.log.error('unable to predict device due to health data records less than 6 days')

        if len(predict_datas) >= 6:
            predicted_result = obj_predictor.predict(predict_datas)
        return predicted_result

    def predict_life_expectancy(self, devid):
        result = self._predict_life_expentancy(devid)
        if result.lower() == 'good':
            return 0, '>6w', ''
        elif result.lower() == 'warning':
            return 0, '>=2w and <=6w', ''
        elif result.lower() == 'bad':
            return 0, '<2w', ''
        else:
            return 0, 'unknown', ''

    def _reset_device_life_expectancy(self, device_id):
        result = CommandResult('')
        self.send_command(result, 'mon', '', json.dumps({
            'prefix': 'device rm-life-expectancy',
            'devid': device_id
        }), '')
        ret, _, outs = result.wait()
        if ret != 0:
            self.log.error(
                'failed to reset device life expectancy, %s' % outs)
        return ret

    def _set_device_life_expectancy(self, device_id, from_date, to_date=None):
        result = CommandResult('')

        if to_date is None:
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'device set-life-expectancy',
                'devid': device_id,
                'from': from_date
            }), '')
        else:
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'device set-life-expectancy',
                'devid': device_id,
                'from': from_date,
                'to': to_date
            }), '')
        ret, _, outs = result.wait()
        if ret != 0:
            self.log.error(
                'failed to set device life expectancy, %s' % outs)
        return ret

    def predict_all_devices(self):
        self.log.debug('predict_all_devices')
        devices = self.get('devices').get('devices', [])
        for devInfo in devices:
            if not devInfo.get('daemons'):
                continue
            if not devInfo.get('devid'):
                continue
            self.log.debug('%s' % devInfo)
            result = self._predict_life_expentancy(devInfo['devid'])
            if result == 'unknown':
                self._reset_device_life_expectancy(devInfo['devid'])
                continue
            predicted = int(time.time() * (1000 ** 3))

            if result.lower() == 'good':
                life_expectancy_day_min = (TIME_WEEK * 6) + TIME_DAYS
                life_expectancy_day_max = None
            elif result.lower() == 'warning':
                life_expectancy_day_min = (TIME_WEEK * 2)
                life_expectancy_day_max = (TIME_WEEK * 6)
            elif result.lower() == 'bad':
                life_expectancy_day_min = 0
                life_expectancy_day_max = (TIME_WEEK * 2) - TIME_DAYS
            else:
                predicted = None
                life_expectancy_day_min = None
                life_expectancy_day_max = None

            if predicted and devInfo['devid'] and life_expectancy_day_min:
                from_date = None
                to_date = None
                try:
                    if life_expectancy_day_min:
                        from_date = self._convert_timestamp(predicted, life_expectancy_day_min)

                    if life_expectancy_day_max:
                        to_date = self._convert_timestamp(predicted, life_expectancy_day_max)

                    self._set_device_life_expectancy(devInfo['devid'], from_date, to_date)
                    self._logger.info(
                        'succeed to set device {} life expectancy from: {}, to: {}'.format(
                            devInfo['devid'], from_date, to_date))
                except Exception as e:
                    self._logger.error(
                        'failed to set device {} life expectancy from: {}, to: {}, {}'.format(
                            devInfo['devid'], from_date, to_date, str(e)))
            else:
                self._reset_device_life_expectancy(devInfo['devid'])
        return 0, 'succeed to predicted all devices', ''
