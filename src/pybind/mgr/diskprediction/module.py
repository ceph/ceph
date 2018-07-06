"""
A diskprediction module
"""
from __future__ import absolute_import

from datetime import datetime
import json
from mgr_module import MgrModule
from threading import Event

from .task import MetricsTask, PredictionTask, SmartTask, \
    DP_MGR_STAT_ENABLED, DP_MGR_STAT_DISABLED

DP_TASK = [MetricsTask, PredictionTask, SmartTask]


class Module(MgrModule):

    config = dict()

    OPTIONS = [
        {
            'name': 'diskprediction_config_mode',
            'default': 'cloud'
        },
        {
            'name': 'diskprediction_server',
            'default': None
        },
        {
            'name': 'diskprediction_port',
            'default': 8086
        },
        {
            'name': 'diskprediction_database',
            'default': 'telegraf'
        },
        {
            'name': 'diskprediction_user',
            'default': None
        },
        {
            'name': 'diskprediction_password',
            'default': None
        },
        {
            'name': 'diskprediction_cluster_domain_id',
            'default': None
        },
        {
            'name': 'diskprediction_upload_metrics_interval',
            'default': 600
        },
        {
            'name': 'diskprediction_upload_smart_interval',
            'default': 43200
        },
        {
            'name': 'diskprediction_retrieve_prediction_interval',
            'default': 43200
        },
        {
            'name': 'diskprediction_smart_relied_device_health',
            'default': 'true'
        }
    ]

    COMMANDS = [
        {
            'cmd': 'diskprediction config-mode '
                   'name=mode,type=CephString,req=true',
            'desc': 'config disk prediction mode [\"cloud\"|\"onpremise\"|\"local\"]',
            'perm': 'rw'
        },
        {
            'cmd': 'diskprediction set-smart-relied-device-health '
                   'name=relied,type=CephString,req=true',
            'desc': 'Set device health data from device health plugin',
            'perm': 'rw'
        },
        {
            'cmd': 'diskprediction config-show',
            'desc': 'Prints diskprediction configuration',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction config-set '
                   'name=server,type=CephString,req=true '
                   'name=user,type=CephString,req=true '
                   'name=password,type=CephString,req=true '
                   'name=port,type=CephInt,req=false ',
            'desc': 'Configure Disk Prediction service',
            'perm': 'rw'
        },
        {
            'cmd': 'diskprediction get-predicted-status '
                   'name=osd_id,type=CephString,req=true',
            'desc': 'Get osd physical disk predicted result',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction run-metrics-forced',
            'desc': 'Run metrics agent forced',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction run-prediction-forced',
            'desc': 'Run prediction agent forced',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction run-smart-forced',
            'desc': 'Run smart agent forced',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction self-test',
            'desc': 'Prints hello world to mgr.x.log',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction status',
            'desc': 'Check diskprediction status',
            'perm': 'r'
        }
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.status = DP_MGR_STAT_DISABLED
        self.event = Event()
        self.run = True
        self._tasks = []
        self._activate_cloud = False
        self._prediction_result = {}

    @property
    def config_keys(self):
        return dict((o['name'], o.get('default', None)) for o in self.OPTIONS)

    def set_config_option(self, option, value):
        if option not in self.config_keys.keys():
            raise RuntimeError('{0} is a unknown configuration '
                               'option'.format(option))

        if option in ['diskprediction_port',
                      'diskprediction_upload_metrics_interval',
                      'diskprediction_upload_smart_interval',
                      'diskprediction_retrieve_prediction_interval']:
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise RuntimeError('invalid {0} configured. Please specify '
                                   'a valid integer'.format(option))

        self.log.debug('Setting in-memory config option %s to: %s', option,
                       value)
        self.config[option] = value
        return True

    def get_configuration(self, key):
        return self.get_config(key, self.config_keys[key])

    def _config_show(self, inbuf, cmd):
        self.show_module_config()
        return 0, json.dumps(self.config, indent=4), ''

    def _config_mode(self, inbuf, cmd):
        str_mode = cmd.get('mode', 'cloud')
        if str_mode.lower() not in ['cloud', 'onpremise', 'local']:
            return 0, 'invalid configuration, enable=[cloud|onpremise|local]', ''
        try:
            self.set_config('diskprediction_config_mode', str_mode)
            return (0,
                    'success to config disk prediction mode: %s'
                    % str_mode.lower(), 0)
        except Exception as e:
            return 0, str(e), ''

    def _set_smart_relied_device_health(self, inbuf, cmd):
        str_relied = cmd.get('relied', 'false')
        if str_relied.lower() not in ['false', 'true']:
            return 0, 'invalid configuration, enable=[true|false]', ''
        try:
            self.set_config('diskprediction_smart_relied_device_health', str_relied)
            return (0,
                    'success to config device data %s on the device plugin'
                    % 'not relied' if str_relied.lower() == 'false' else 'relied', 0)
        except Exception as e:
            return 0, 'command failed, %s' % str(e), ''

    def _self_test(self, inbuf, cmd):
        return 0, 'self-test completed', ''

    def _get_predicted_status(self, inbuf, cmd):
        osd_data = dict()
        physical_data = dict()
        try:
            if not self._prediction_result:
                for _task in self._tasks:
                    if isinstance(_task, PredictionTask):
                        _task.event.set()
                        break
            pre_data = self._prediction_result
            fsid = self.get('mon_map')['fsid']
            for d_host, d_val in pre_data.get(fsid, {}).iteritems():
                if "osd.%s" % cmd['osd_id'] in d_val.get("osd", {}).keys():
                    s_data = d_val['osd']['osd.%s' % cmd['osd_id']]
                    for dev in s_data.get("physicalDisks", []):
                        p_data = dev.get('prediction', {})
                        if not p_data.get('predicted'):
                            predicted = ''
                        else:
                            predicted = datetime.fromtimestamp(int(
                                p_data.get('predicted')) / (1000 ** 3))
                        d_data = {
                            'device': dev.get("diskName"),
                            'near_failure': p_data.get('near_failure'),
                            'predicted': str(predicted),
                            'serial_number': dev.get('serialNumber'),
                            'disk_wwn': dev.get('diskWWN'),
                            'attachment': p_data.get('disk_name', '')
                        }
                        physical_data[dev.get('diskName')] = d_data
                    osd_data['osd.%s' % cmd['osd_id']] = dict(
                        prediction=physical_data)
                    break
            if not osd_data:
                msg = 'not found osd %s predicted data' % cmd['osd_id']
            else:
                msg = json.dumps(osd_data, indent=4)
        except Exception as e:
            if str(e).find('No such file') >= 0:
                msg = 'unable to get osd %s predicted data'
            else:
                msg = 'unable to get osd %s predicted data, %s' % (cmd['osd_id'], str(e))
            self.log.error(msg)
        return 0, msg, ''

    def _config_set(self, inbuf, cmd):
        self.set_config('diskprediction_server', cmd['server'])
        self.set_config('diskprediction_user', cmd['user'])
        self.set_config('diskprediction_password', cmd['password'])
        if cmd.get('port'):
            self.set_config('diskprediction_port', cmd['port'])
        self.show_module_config()
        return 0, json.dumps(self.config, indent=4), ''

    def _run_prediction_forced(self, inbuf, cmd):
        msg = ''
        for _task in self._tasks:
            if isinstance(_task, PredictionTask):
                msg = 'run prediction agent successfully'
                _task.event.set()
        return 0, msg, ''

    def _run_metrics_forced(self, inbuf, cmd):
        msg = ''
        for _task in self._tasks:
            if isinstance(_task, MetricsTask):
                msg = 'run metrics agent successfully'
                _task.event.set()
        return 0, msg, ''

    def _run_smart_forced(self, inbuf, cmd):
        msg = ' '
        for _task in self._tasks:
            if isinstance(_task, SmartTask):
                msg = 'run smart agent successfully'
                _task.event.set()
        return 0, msg, ''

    def _status(self, inbuf, cmd):
        msg = 'diskprediction plugin status: %s' % self.status
        return 0, msg, ''

    def handle_command(self, inbuf, cmd):
        for o_cmd in self.COMMANDS:
            if cmd['prefix'] == o_cmd['cmd'][:len(cmd['prefix'])]:
                fun = getattr(
                    self, '_%s' % o_cmd['cmd'].split(' ')[1].replace('-', '_'))
                if fun:
                    return fun(inbuf, cmd)
        return 0, 'cmd not found', ''

    def show_module_config(self):
        self.fsid = self.get('mon_map')['fsid']
        self.log.debug('Found Ceph fsid %s', self.fsid)

        for key, default in self.config_keys.items():
            self.set_config_option(key, self.get_config(key, default))

    def serve(self):
        self.log.info('Starting diskprediction module')
        self.run = True
        self.status = DP_MGR_STAT_ENABLED

        while self.run:
            if self.get_configuration("diskprediction_config_mode").lower() in ['cloud', 'onpremise']:
                enable_colud = True
            if enable_colud and not self._activate_cloud:
                self.start_cloud_disk_prediction()
            elif not enable_colud and self._activate_cloud:
                self.stop_cloud_disk_prediction()
            self.event.wait(60)

    def start_cloud_disk_prediction(self):
        try:
            if not self._activate_cloud:
                for dp_task in DP_TASK:
                    obj_task = dp_task(self)
                    if obj_task:
                        obj_task.run()
                    else:
                        raise Exception('failed to start task %s' % obj_task._task_name)
                    self._tasks.append(obj_task)
                self._activate_cloud = True
                self.log.info('start cloud disk prediction')
        except:
            self.log.error('failed to start cloud disk prediction')
            if self._tasks:
                self._activate_cloud = True
                self.stop_cloud_disk_prediction()

    def stop_cloud_disk_prediction(self):
        try:
            if self._activate_cloud:
                self.status = DP_MGR_STAT_DISABLED
                while self._tasks:
                    try:
                        dp_task = self._tasks.pop()
                        dp_task.terminate()
                        del dp_task
                    except:
                        break
                self._activate_cloud = False
                self.log.info("stop cloud disk prediction")
        except:
            self.log.error('failed to stop cloud disk prediction')

    def shutdown(self):
        self.run = False
        self.stop_cloud_disk_prediction()
        self.event.set()
        super(Module, self).shutdown()
