"""
A diskprediction module
"""
from __future__ import absolute_import

from datetime import datetime
import errno
import json
from mgr_module import MgrModule
from threading import Event

from .common import DP_MGR_STAT_ENABLED, DP_MGR_STAT_DISABLED
from .task import MetricsRunner, PredictionRunner, SmartRunner


DP_AGENTS = [MetricsRunner, PredictionRunner, SmartRunner]


class Module(MgrModule):

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
            'name': 'diskprediction_cert_path',
            'default': ''
        },
        {
            'name': 'diskprediction_ssl_target_name_override',
            'default': 'localhost'
        },
        {
            'name': 'diskprediction_default_authority',
            'default': 'localhost'
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
                   'name=dev_id,type=CephString,req=true',
            'desc': 'Get physical device predicted result',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction debug metrics-forced',
            'desc': 'Run metrics agent forced',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction debug prediction-forced',
            'desc': 'Run prediction agent forced',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction debug smart-forced',
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
        },
        {
            'cmd': 'diskprediction test-api',
            'desc': 'Check diskprediction status',
            'perm': 'r'
        }
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.status = DP_MGR_STAT_DISABLED
        self.shutdown_event = Event()
        self._agents = []
        self._activate_cloud = False
        self._prediction_result = {}
        self.config = dict()
        self.show_module_config

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
        self.set_config(option, value)
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
            return -errno.EINVAL, 'invalid configuration, enable=[cloud|onpremise|local]', ''
        try:
            self.set_config('diskprediction_config_mode', str_mode)
            return (0,
                    'success to config disk prediction mode: %s'
                    % str_mode.lower(), 0)
        except Exception as e:
            return -errno.EINVAL, str(e), ''

    def _self_test(self, inbuf, cmd):
        from .test.test_agents import test_agents
        test_agents(self)
        return 0, 'self-test completed', ''

    def _test_api(self, inbuf, cmd):
        from .common.clusterdata import ClusterAPI
        devinfo = ClusterAPI(self).get_device_health('WDC_WD1003FBYX-18Y7B0_WD-WCAW32140123')
        devinfo.values()
        # devinfo = self.get('devices')
        self.log.error("devices: %s" % devinfo)
        return 0, 'test-api completed', ''


    def _set_cert_path(self, inbuf, cmd):
        str_cert_path = cmd.get('cert_path', '')
        try:
            self.set_config('diskprediction_cert_path', str_cert_path)
            return (0,
                    'success to config ssl certification file path %s'
                    % str_cert_path, 0)
        except Exception as e:
            return -errno.EINVAL, str(e), ''

    def _set_ssl_target_name(self, inbuf, cmd):
        str_ssl_target = cmd.get('ssl_target_name', '')
        try:
            self.set_config('diskprediction_ssl_target_name_override', str_ssl_target)
            return (0,
                    'success to config ssl target name', 0)
        except Exception as e:
            return -errno.EINVAL, str(e), ''

    def _set_ssl_default_authority(self, inbuf, cmd):
        str_ssl_authority = cmd.get('ssl_authority', '')
        try:
            self.set_config('diskprediction_default_authority', str_ssl_authority)
            return (0,
                    'success to config ssl default authority', 0)
        except Exception as e:
            return -errno.EINVAL, str(e), ''

    def _get_predicted_status(self, inbuf, cmd):
        osd_data = dict()
        physical_data = dict()
        try:
            if not self._prediction_result:
                for _agent in self._agents:
                    if isinstance(_agent, PredictionRunner):
                        _agent.event.set()
                        break
            pre_data = self._prediction_result.get(cmd['dev_id'])
            if pre_data:
                p_data = pre_data.get('prediction', {})
                if not p_data.get('predicted'):
                    predicted = ''
                else:
                    predicted = datetime.fromtimestamp(int(
                        p_data.get('predicted')) / (1000 ** 3))
                d_data = {
                    'near_failure': p_data.get('near_failure'),
                    'predicted': str(predicted),
                    'serial_number': pre_data.get('serial_number'),
                    'disk_wwn': pre_data.get('disk_wwn'),
                    'attachment': p_data.get('disk_name', '')
                }
                physical_data[cmd['dev_id']] = d_data
                msg = json.dumps(d_data, indent=4)
            else:
                msg = 'device %s predicted data not ready' % cmd['dev_id']
        except Exception as e:
            if str(e).find('No such file') >= 0:
                msg = 'unable to get device {} predicted data'.format(cmd['dev_id'])
            else:
                msg = 'unable to get osd {} predicted data, {}' % (cmd['dev_id'], str(e))
            self.log.error(msg)
            return -errno.EINVAL, msg, ''
        return 0, msg, ''

    def _config_set(self, inbuf, cmd):
        self.set_config('diskprediction_server', cmd['server'])
        self.set_config('diskprediction_user', cmd['user'])
        self.set_config('diskprediction_password', cmd['password'])
        if cmd.get('port'):
            self.set_config('diskprediction_port', cmd['port'])
        return 0, 'Configuration updated', ''

    def _debug_prediction_forced(self, inbuf, cmd):
        msg = ''
        for _agent in self._agents:
            if isinstance(_agent, PredictionRunner):
                msg = 'run prediction agent successfully'
                _agent.event.set()
        return 0, msg, ''

    def _debug_metrics_forced(self, inbuf, cmd):
        msg = ''
        for _agent in self._agents:
            if isinstance(_agent, MetricsRunner):
                msg = 'run metrics agent successfully'
                _agent.event.set()
        return 0, msg, ''

    def _debug_smart_forced(self, inbuf, cmd):
        msg = ' '
        for _agent in self._agents:
            if isinstance(_agent, SmartRunner):
                msg = 'run smart agent successfully'
                _agent.event.set()
        return 0, msg, ''

    def _status(self, inbuf, cmd):
        msg = 'diskprediction plugin status: %s' % self.status
        return 0, msg, ''

    def handle_command(self, inbuf, cmd):
        for o_cmd in self.COMMANDS:
            if cmd['prefix'] == o_cmd['cmd'][:len(cmd['prefix'])]:
                fun_name = ''
                avgs = o_cmd['cmd'].split(' ')
                for avg in avgs:
                    if avg.lower() == 'diskprediction':
                        continue
                    if '=' in avg or ',' in avg:
                        continue
                    fun_name += '_%s' % avg.replace('-', '_')
                if fun_name:
                    fun = getattr(
                        self, fun_name)
                    if fun:
                        return fun(inbuf, cmd)
        return -errno.EINVAL, 'cmd not found', ''

    def show_module_config(self):
        self.fsid = self.get('mon_map')['fsid']
        self.log.debug('Found Ceph fsid %s', self.fsid)

        for key, default in self.config_keys.items():
            self.set_config_option(key, self.get_config(key, default))

    def serve(self):
        self.log.info('Starting diskprediction module')
        self.status = DP_MGR_STAT_ENABLED

        while True:
            if self.get_configuration('diskprediction_config_mode').lower() in ['cloud', 'onpremise']:
                enable_cloud = True
            if enable_cloud and not self._activate_cloud:
                self.start_cloud_disk_prediction()
            elif not enable_cloud and self._activate_cloud:
                self.stop_cloud_disk_prediction()
            self.shutdown_event.wait(5)
            if self.shutdown_event.is_set():
                break
        self.stop_cloud_disk_prediction()

    def start_cloud_disk_prediction(self):
        assert not self._activate_cloud
        for dp_agent in DP_AGENTS:
            obj_agent = dp_agent(self)
            if obj_agent:
                obj_agent.start()
            else:
                raise Exception('failed to start task %s' % obj_agent.task_name)
            self._agents.append(obj_agent)
        self._activate_cloud = True
        self.log.info('start cloud disk prediction')

    def stop_cloud_disk_prediction(self):
        assert self._activate_cloud
        self.status = DP_MGR_STAT_DISABLED
        while self._agents:
            dp_agent = self._agents.pop()
            dp_agent.terminate()
            dp_agent.join(5)
            del dp_agent
        self._activate_cloud = False
        self.log.info('stop cloud disk prediction')

    def shutdown(self):
        self.shutdown_event.set()
        super(Module, self).shutdown()
