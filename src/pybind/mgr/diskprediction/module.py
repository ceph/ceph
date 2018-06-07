"""
A diskprediction module
"""
from __future__ import absolute_import

from datetime import datetime
import json
from mgr_module import MgrModule
from threading import Event

from .task import Metrics_Task, Prediction_Task, Smart_Task

DP_TASK = [Metrics_Task, Prediction_Task, Smart_Task]


class Module(MgrModule):

    config = dict()

    OPTIONS = [
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
    ]

    COMMANDS = [
        {
            'cmd': 'diskprediction self-test',
            'desc': 'Prints hello world to mgr.x.log',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction config-show',
            'desc': 'Prints diskprediction configuration',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction get-predicted-status '
                   'name=osd_id,type=CephString,req=true',
            'desc': 'Get osd physical disk predicted result',
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
            'cmd': 'diskprediction run-metrics-forcely',
            'desc': 'Run metrics agent forcely',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction run-prediction-forcely',
            'desc': 'Run prediction agent forcely',
            'perm': 'r'
        },
        {
            'cmd': 'diskprediction run-smart-forcely',
            'desc': 'Run smart agent forcely',
            'perm': 'r'
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()
        self.run = True
        self._tasks = []

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

    def handle_command(self, cmd):
        if cmd['prefix'] == 'diskprediction config-show':
            self.show_module_config()
            return 0, json.dumps(self.config, indent=4), ''
        if cmd['prefix'] == 'diskprediction get-predicted-status':
            osd_data = dict()
            physical_data = dict()
            from .agent.predict.prediction import PREDICTION_FILE
            try:
                with open(PREDICTION_FILE, "r+") as fd:
                    pre_data = json.load(fd)
                fsid = self.get('mon_map')['fsid']
                for d_host, d_val in pre_data.get(fsid, {}).iteritems():
                    if "osd.%s" % cmd['osd_id'] in d_val.get("osd", {}).keys():
                        s_data = d_val['osd']['osd.%s' % cmd['osd_id']]
                        for dev in s_data.get("physicalDisks", []):
                            p_data = dev.get('prediction', {})
                            if not p_data.get('predicted'):
                                predicted = None
                            else:
                                predicted = datetime.fromtimestamp(int(p_data.get('predicted'))/1000/1000/1000)
                            d_data = {
                                'device': dev.get("diskName"),
                                'near_failure': p_data.get('near_failure'),
                                'predicted': str(predicted),
                                'serial_number': dev.get('serialNumber'),
                                'disk_wwn': dev.get('diskWWN')
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
                msg = 'unable to get osd %s predicted data' % cmd['osd_id']
                self.log.error(msg)
            return 0, msg, ''
        if cmd['prefix'] == 'diskprediction config-set':
            self.set_config('diskprediction_server', cmd['server'])
            self.set_config('diskprediction_user', cmd['user'])
            self.set_config('diskprediction_password', cmd['password'])
            if cmd.get('port'):
                self.set_config('diskprediction_port', cmd['port'])
            self.show_module_config()
            return 0, json.dumps(self.config, indent=4), ''
        if cmd['prefix'] == 'diskprediction run-metrics-forcely':
            msg = ''
            for _task in self._tasks:
                if isinstance(_task, Metrics_Task):
                    msg = 'run metrics agent successfully'
                    _task.event.set()
            return 0, msg, ''
        if cmd['prefix'] == 'diskprediction run-prediction-forcely':
            msg = ''
            for _task in self._tasks:
                if isinstance(_task, Prediction_Task):
                    msg = 'run prediction agent successfully'
                    _task.event.set()
            return 0, msg, ''
        if cmd['prefix'] == 'diskprediction run-smart-forcely':
            msg = ''
            for _task in self._tasks:
                if isinstance(_task, Smart_Task):
                    msg = 'run smart agent successfully'
                    _task.event.set()
            return 0, msg, ''

    def show_module_config(self):
        self.fsid = self.get('mon_map')['fsid']
        self.log.debug('Found Ceph fsid %s', self.fsid)

        for key, default in self.config_keys.items():
            self.set_config_option(key, self.get_config(key, default))

    def serve(self):
        self.log.info('Starting diskprediction module')
        self.run = True

        try:
            self.send_to_diskprophet()
        except Exception as e:
            self.log.error(' %s Unexpected error during send function.', str(e))

        while self.run:
            self.event.wait(60)

    def send_to_diskprophet(self):
        for dp_task in DP_TASK:
            obj_task = dp_task(self)
            obj_task.run()
            self._tasks.append(obj_task)

    def shutdown(self):
        self.run = False
        for dp_task in self._tasks:
            dp_task.terminate()
        self.event.set()
        super(Module, self).shutdown()
