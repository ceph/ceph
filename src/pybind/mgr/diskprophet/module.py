"""
A diskprophet module
"""
from __future__ import absolute_import

import json
from threading import Event
from mgr_module import MgrModule

from .task import Metrics_Task, Prediction_Task, Smart_Task

DP_TASK = [Metrics_Task, Prediction_Task, Smart_Task]


class Module(MgrModule):

    config = dict()

    OPTIONS = [
        {
            'name': 'diskprophet_server',
            'default': None
        },
        {
            'name': 'diskprophet_port',
            'default': 8086
        },
        {
            'name': 'diskprophet_database',
            'default': 'telegraf'
        },
        {
            'name': 'diskprophet_user',
            'default': None
        },
        {
            'name': 'diskprophet_password',
            'default': None
        },
        {
            'name': 'diskprophet_cluster_domain_id',
            'default': None
        },
        {
            'name': 'diskprophet_upload_metrics_interval',
            'default': 600
        },
        {
            'name': 'diskprophet_upload_smart_interval',
            'default': 43200
        },
        {
            'name': 'diskprophet_retrieve_prediction_interval',
            'default': 43200
        },
    ]

    COMMANDS = [
        {
            'cmd': 'diskprophet self-test',
            'desc': 'Prints hello world to mgr.x.log',
            'perm': 'r'
        },
        {
            'cmd': 'diskprophet config-show',
            'desc': 'Prints diskprophet configuration',
            'perm': 'r'
        },
        {
            'cmd': 'diskprophet get-predicted-status '
                   'name=osd_id,type=CephString,req=true',
            'desc': 'Get osd physical disk predicted result',
            'perm': 'r'
        },
        {
            'cmd': 'diskprophet config-set '
                   'name=server,type=CephString,req=true '
                   'name=user,type=CephString,req=true '
                   'name=password,type=CephString,req=true '
                   'name=port,type=CephInt,req=false ',
            'desc': 'Configure DiskProphet service',
            'perm': 'rw'
        },
        {
            'cmd': 'diskprophet run-metrics-forcely',
            'desc': 'Run metrics agent forcely',
            'perm': 'r'
        },
        {
            'cmd': 'diskprophet run-prediction-forcely',
            'desc': 'Run prediction agent forcely',
            'perm': 'r'
        },
        {
            'cmd': 'diskprophet run-smart-forcely',
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

        if option in ['diskprophet_port',
                      'diskprophet_upload_metrics_interval',
                      'diskprophet_upload_smart_interval',
                      'diskprophet_retrieve_prediction_interval']:
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
        if cmd['prefix'] == 'diskprophet config-show':
            self.show_module_config()
            return 0, json.dumps(self.config), ''
        if cmd['prefix'] == 'diskprophet get-predicted-status':
            osd_data = {}
            from .agent.predict.prediction import PREDICTION_FILE
            try:
                with open(PREDICTION_FILE, "r+") as fd:
                    pre_data = json.load(fd)
                fsid = self.get('mon_map')['fsid']
                for d_host, d_val in pre_data.get(fsid, {}).iteritems():
                    if "osd.%s" % cmd['osd_id'] in d_val.get("osd", {}).keys():
                        osd_data = d_val['osd']['osd.%s' % cmd['osd_id']]
                        break
                if not osd_data:
                    msg = 'not found osd %s predicted data' % cmd['osd_id']
                else:
                    msg = json.dumps(osd_data, indent=4)
            except Exception as e:
                msg = 'unable to get osd %s predicted data' % cmd['osd_id']
                self.log.error(msg)
            return 0, msg, ''
        if cmd['prefix'] == 'diskprophet config-set':
            self.set_config('diskprophet_server', cmd['server'])
            self.set_config('diskprophet_user', cmd['user'])
            self.set_config('diskprophet_password', cmd['password'])
            if cmd.get('port'):
                self.set_config('diskprophet_port', cmd['port'])
            self.show_module_config()
            return 0, json.dumps(self.config, indent=4), ''
        if cmd['prefix'] == 'diskprophet run-metrics-forcely':
            msg = ''
            for _task in self._tasks:
                if isinstance(_task, Metrics_Task):
                    msg = 'run metrics agent successfully'
                    _task.event.set()
            return 0, msg, ''
        if cmd['prefix'] == 'diskprophet run-prediction-forcely':
            msg = ''
            for _task in self._tasks:
                if isinstance(_task, Prediction_Task):
                    msg = 'run prediction agent successfully'
                    _task.event.set()
            return 0, msg, ''
        if cmd['prefix'] == 'diskprophet run-smart-forcely':
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
        self.log.info('Starting diskprophet module')
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
