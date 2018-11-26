from __future__ import absolute_import

import time
from threading import Event, Thread

from .agent.predictor import PredictAgent
from .agent.metrics.ceph_cluster import CephClusterAgent
from .agent.metrics.ceph_mon_osd import CephMonOsdAgent
from .agent.metrics.ceph_pool import CephPoolAgent
from .agent.metrics.db_relay import DBRelayAgent
from .agent.metrics.sai_agent import SAIAgent
from .agent.metrics.sai_cluster import SAICluserAgent
from .agent.metrics.sai_disk import SAIDiskAgent
from .agent.metrics.sai_disk_smart import SAIDiskSmartAgent
from .agent.metrics.sai_host import SAIHostAgent
from .common import DP_MGR_STAT_FAILED, DP_MGR_STAT_OK, DP_MGR_STAT_WARNING


class AgentRunner(Thread):

    task_name = ''
    interval_key = ''
    agents = []

    def __init__(self, mgr_module, agent_timeout=60, call_back=None):
        """

        :param mgr_module: parent ceph mgr module
        :param agent_timeout: (unit seconds) agent execute timeout value, default: 60 secs
        """
        Thread.__init__(self)
        self._agent_timeout = agent_timeout
        self._module_inst = mgr_module
        self._log = mgr_module.log
        self._start_time = time.time()
        self._th = None
        self._call_back = call_back
        self.exit = False
        self.event = Event()
        self.task_interval = \
            int(self._module_inst.get_configuration(self.interval_key))

    def terminate(self):
        self.exit = True
        self.event.set()
        self._log.info('PDS terminate %s complete' % self.task_name)

    def run(self):
        self._start_time = time.time()
        self._log.info(
            'start %s, interval: %s'
            % (self.task_name, self.task_interval))
        while not self.exit:
            self.run_agents()
            if self._call_back:
                self._call_back()
            if self.event:
                self.event.wait(int(self.task_interval))
                self.event.clear()
                self._log.info(
                    'completed %s(%s)' % (self.task_name, time.time()-self._start_time))

    def run_agents(self):
        obj_sender = None
        try:
            self._log.debug('run_agents %s' % self.task_name)
            from .common.grpcclient import GRPcClient, gen_configuration
            conf = gen_configuration(
                host=self._module_inst.get_configuration('diskprediction_server'),
                user=self._module_inst.get_configuration('diskprediction_user'),
                password=self._module_inst.get_configuration(
                    'diskprediction_password'),
                port=self._module_inst.get_configuration('diskprediction_port'),
                cert_context=self._module_inst.get_configuration('diskprediction_cert_context'),
                mgr_inst=self._module_inst,
                ssl_target_name=self._module_inst.get_configuration('diskprediction_ssl_target_name_override'),
                default_authority=self._module_inst.get_configuration('diskprediction_default_authority'))
            obj_sender = GRPcClient(conf)
            if not obj_sender:
                self._log.error('invalid diskprediction sender')
                self._module_inst.status = \
                    {'status': DP_MGR_STAT_FAILED,
                     'reason': 'invalid diskprediction sender'}
                raise Exception('invalid diskprediction sender')
            if obj_sender.test_connection():
                self._module_inst.status = {'status': DP_MGR_STAT_OK}
                self._log.debug('succeed to test connection')
                self._run(self._module_inst, obj_sender)
            else:
                self._log.error('failed to test connection')
                self._module_inst.status = \
                    {'status': DP_MGR_STAT_FAILED,
                     'reason': 'failed to test connection'}
                raise Exception('failed to test connection')
        except Exception as e:
            self._module_inst.status = \
                {'status': DP_MGR_STAT_FAILED,
                 'reason': 'failed to start %s agents, %s'
                           % (self.task_name, str(e))}
            self._log.error(
                'failed to start %s agents, %s' % (self.task_name, str(e)))
            raise
        finally:
            if obj_sender:
                obj_sender.close()

    def is_timeout(self):
        now = time.time()
        if (now - self._start_time) > self._agent_timeout:
            return True
        else:
            return False

    def _run(self, module_inst, sender):
        self._log.debug('%s run' % self.task_name)
        for agent in self.agents:
            self._start_time = time.time()
            retry_count = 3
            while retry_count:
                retry_count -= 1
                try:
                    obj_agent = agent(module_inst, sender, self._agent_timeout)
                    obj_agent.run()
                    del obj_agent
                    break
                except Exception as e:
                    if str(e).find('configuring') >= 0:
                        self._log.debug(
                            'failed to execute {}, {}, retry again.'.format(
                                agent.measurement, str(e)))
                        time.sleep(1)
                        continue
                    else:
                        module_inst.status = \
                            {'status': DP_MGR_STAT_WARNING,
                             'reason': 'failed to execute {}, {}'.format(
                                agent.measurement, ';'.join(str(e).split('\n\t')))}
                        self._log.warning(
                            'failed to execute {}, {}'.format(
                                agent.measurement, ';'.join(str(e).split('\n\t'))))
                        break


class MetricsRunner(AgentRunner):

    task_name = 'Metrics Agent'
    interval_key = 'diskprediction_upload_metrics_interval'
    agents = [CephClusterAgent, CephMonOsdAgent, CephPoolAgent,
              SAICluserAgent, SAIDiskAgent, SAIHostAgent, DBRelayAgent,
              SAIAgent]


class PredictRunner(AgentRunner):

    task_name = 'Predictor Agent'
    interval_key = 'diskprediction_retrieve_prediction_interval'
    agents = [PredictAgent]


class SmartRunner(AgentRunner):

    task_name = 'Smart data Agent'
    interval_key = 'diskprediction_upload_smart_interval'
    agents = [SAIDiskSmartAgent]


class TestRunner(object):
    task_name = 'Test Agent'
    interval_key = 'diskprediction_upload_metrics_interval'
    agents = [CephClusterAgent, CephMonOsdAgent, CephPoolAgent,
              SAICluserAgent, SAIDiskAgent, SAIHostAgent, DBRelayAgent,
              SAIAgent, SAIDiskSmartAgent]

    def __init__(self, mgr_module):
        self._module_inst = mgr_module

    def run(self):
        for agent in self.agents:
            obj_agent = agent(self._module_inst, None)
            obj_agent.run()
            del obj_agent
