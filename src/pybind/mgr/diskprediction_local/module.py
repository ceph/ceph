"""
diskprediction with local predictor
"""
import json
import datetime
from threading import Event
import time
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING
from mgr_module import CommandResult, MgrModule, Option
# Importing scipy early appears to avoid a future deadlock when
# we try to do
#
#  from .predictor import get_diskfailurepredictor_path
#
# in a command thread.  See https://tracker.ceph.com/issues/42764
import scipy  # noqa: ignore=F401
from .predictor import DevSmartT, Predictor, get_diskfailurepredictor_path


TIME_FORMAT = '%Y%m%d-%H%M%S'
TIME_DAYS = 24 * 60 * 60
TIME_WEEK = TIME_DAYS * 7


class Module(MgrModule):
    MODULE_OPTIONS = [
        Option(name='sleep_interval',
               default=600),
        Option(name='predict_interval',
               default=86400),
        Option(name='predictor_model',
               default='prophetstor')
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        # options
        for opt in self.MODULE_OPTIONS:
            setattr(self, opt['name'], opt['default'])
        # other
        self._run = True
        self._event = Event()
        # for mypy which does not run the code
        if TYPE_CHECKING:
            self.sleep_interval = 0
            self.predict_interval = 0
            self.predictor_model = ''

    def config_notify(self) -> None:
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' %s = %s', opt['name'], getattr(self, opt['name']))
        if self.get_ceph_option('device_failure_prediction_mode') == 'local':
            self._event.set()

    def refresh_config(self) -> None:
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' %s = %s', opt['name'], getattr(self, opt['name']))

    def self_test(self) -> None:
        self.log.debug('self_test enter')
        ret, out, err = self.predict_all_devices()
        assert ret == 0

    def serve(self) -> None:
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
                    predicted_frequency = self.predict_interval or 86400
                    seconds = (last_predicted - datetime.datetime.utcfromtimestamp(0)).total_seconds()
                    seconds -= seconds % predicted_frequency
                    seconds += predicted_frequency
                    next_predicted = datetime.datetime.utcfromtimestamp(seconds)
                    self.log.debug('Last scrape %s, next scrape due %s',
                                   last_predicted.strftime(TIME_FORMAT),
                                   next_predicted.strftime(TIME_FORMAT))
                if now >= next_predicted:
                    self.predict_all_devices()
                    last_predicted = now
                    self.set_store('last_predicted', last_predicted.strftime(TIME_FORMAT))

            sleep_interval = self.sleep_interval or 60
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            self._event.wait(sleep_interval)
            self._event.clear()

    def shutdown(self) -> None:
        self.log.info('Stopping')
        self._run = False
        self._event.set()

    @staticmethod
    def _convert_timestamp(predicted_timestamp: int, life_expectancy_day: int) -> str:
        """
        :param predicted_timestamp: unit is nanoseconds
        :param life_expectancy_day: unit is seconds
        :return:
            date format '%Y-%m-%d' ex. 2018-01-01
        """
        return datetime.datetime.fromtimestamp(
            predicted_timestamp / (1000 ** 3) + life_expectancy_day).strftime('%Y-%m-%d')

    def _predict_life_expentancy(self, devid: str) -> str:
        predicted_result = ''
        health_data: Dict[str, Dict[str, Any]] = {}
        predict_datas: List[DevSmartT] = []
        try:
            r, outb, outs = self.remote(
                'devicehealth', 'show_device_metrics', devid=devid, sample='')
            if r != 0:
                self.log.error('failed to get device %s health', devid)
                health_data = {}
            else:
                health_data = json.loads(outb)
        except Exception as e:
            self.log.error('failed to get device %s health data due to %s', devid, str(e))

        # initialize appropriate disk failure predictor model
        obj_predictor = Predictor.create(self.predictor_model)
        if obj_predictor is None:
            self.log.error('invalid value received for MODULE_OPTIONS.predictor_model')
            return predicted_result
        try:
            obj_predictor.initialize(
                "{}/models/{}".format(get_diskfailurepredictor_path(), self.predictor_model))
        except Exception as e:
            self.log.error('Error initializing predictor: %s', e)
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
                power_on_time = s_val.get('power_on_time', {}).get('hours')
                if power_on_time is not None:
                    dev_smart['smart_9_raw'] = int(power_on_time)
                # add device capacity
                user_capacity = s_val.get('user_capacity', {}).get('bytes')
                if user_capacity is not None:
                    dev_smart['user_capacity'] = user_capacity
                else:
                    self.log.debug('user_capacity not found in smart attributes list')
                # add device model
                model_name = s_val.get('model_name')
                if model_name is not None:
                    dev_smart['model_name'] = model_name
                # add vendor
                vendor = s_val.get('vendor')
                if vendor is not None:
                    dev_smart['vendor'] = vendor
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

    def predict_life_expectancy(self, devid: str) -> Tuple[int, str, str]:
        result = self._predict_life_expentancy(devid)
        if result.lower() == 'good':
            return 0, '>6w', ''
        elif result.lower() == 'warning':
            return 0, '>=2w and <=6w', ''
        elif result.lower() == 'bad':
            return 0, '<2w', ''
        else:
            return 0, 'unknown', ''

    def _reset_device_life_expectancy(self, device_id: str) -> int:
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

    def _set_device_life_expectancy(self,
                                    device_id: str,
                                    from_date: str,
                                    to_date: Optional[str] = None) -> int:
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

    def predict_all_devices(self) -> Tuple[int, str, str]:
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
                life_expectancy_day_max = 0
            elif result.lower() == 'warning':
                life_expectancy_day_min = (TIME_WEEK * 2)
                life_expectancy_day_max = (TIME_WEEK * 6)
            elif result.lower() == 'bad':
                life_expectancy_day_min = 0
                life_expectancy_day_max = (TIME_WEEK * 2) - TIME_DAYS
            else:
                predicted = 0
                life_expectancy_day_min = 0
                life_expectancy_day_max = 0

            if predicted and devInfo['devid'] and life_expectancy_day_min:
                from_date = None
                to_date = None
                try:
                    assert life_expectancy_day_min
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
