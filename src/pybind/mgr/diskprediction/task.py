from __future__ import absolute_import

import time
from threading import Event, Thread

from .agent import Command
from .agent.metrics.ceph_cluster import CephCluster_Agent
from .agent.metrics.ceph_mon import CephMon_Agent
from .agent.metrics.ceph_osd import CephOSD_Agent
from .agent.metrics.ceph_pool import CephPool_Agent
from .agent.metrics.db_relay import DB_RelayAgent
from .agent.metrics.sai_cluster import SAI_CluserAgent
from .agent.metrics.sai_disk import SAI_DiskAgent
from .agent.metrics.sai_disk_smart import SAI_DiskSmartAgent
from .agent.metrics.sai_host import SAI_HostAgent
from .agent.predict.prediction import Prediction_Agent


class DP_Task(object):

    _task_name = ""
    _interval_key = ""

    def __init__(self, ceph_context, agent_timeout=60):
        """

        :param ceph_context: parent ceph mgr module
        :param obj_sender: data push sender object
        :param task_interval: (unit seconds) interval trigger task , default: 1 hour
        :param agent_timeout: (unit seconds) agent execute timeout value, default: 60 secs
        """
        self._agent_timeout = agent_timeout
        self._context = ceph_context
        self._log = ceph_context.log
        self._obj_sender = None
        self._start_time = None
        self._th = None

        self.exit = False
        self.event = Event()
        self.task_interval = \
            self._context.get_configuration(self._interval_key)
        self.cluster_domain_id = \
            self._context.get_configuration('diskprediction_cluster_domain_id')

    def run(self):
        self._start_time = time.time()
        # self._run()
        self._th = Thread(target=self._start, args=(self,))
        self._th.start()

    def terminate(self):
        self.exit = True
        self.event.set()
        if self._th:
            self._th.join(60)
        self._log.info("PDS terminate %s complete" % self._task_name)

    @staticmethod
    def _start(obj_task):
        obj_task._log.debug(
            "start %s, interval: %s"
            % (obj_task._task_name, obj_task.task_interval))
        while not obj_task.exit:
            obj_task.run_agents()
            if obj_task.event:
                obj_task.event.wait(int(obj_task.task_interval))
                obj_task.event.clear()
        obj_task._log.info(
            "completed %s(%s)"
            % (obj_task._task_name, time.time()-obj_task._start_time))

    def run_agents(self):
        # for testing
        try:
            self._log.debug("run_agents %s" % self._task_name)
            self._obj_sender = Command(
                host=self._context.get_configuration("diskprediction_server"),
                user=self._context.get_configuration("diskprediction_user"),
                password=self._context.get_configuration("diskprediction_password"))
            if not self._obj_sender:
                self._log.error("invalid diskprediction sender")
                return
            if self._obj_sender.test_connection():
                self._log.debug("succeed to test connection")
                self._run()
            else:
                self._log.error("failed to test connection")
        except Exception as e:
            self._log.error(
                "failed to start %s agents, %s" % (self._task_name, str(e)))

    def _run(self):
        pass


class Metrics_Task(DP_Task):

    _task_name = "Metrics Task"
    _interval_key = "diskprediction_upload_metrics_interval"
    _agents = [CephCluster_Agent, CephMon_Agent, CephOSD_Agent, CephPool_Agent,
               SAI_CluserAgent, SAI_DiskAgent, SAI_HostAgent, DB_RelayAgent]

    def _run(self):
        self._log.debug("%s run" % self._task_name)
        for agent in self._agents:
            try:
                obj_agent = agent(
                    self._context, self._obj_sender, self._agent_timeout)
                obj_agent.run()
            except Exception:
                continue


class Prediction_Task(DP_Task):

    _task_name = "Prediction Task"
    _interval_key = "diskprediction_retrieve_prediction_interval"
    _agents = [Prediction_Agent]

    def _run(self):
        self._log.debug("%s run" % self._task_name)
        for agent in self._agents:
            try:
                obj_agent = agent(
                    self._context, self._obj_sender, self._agent_timeout)
                obj_agent.run()
            except Exception:
                continue


class Smart_Task(DP_Task):

    _task_name = "Smart data Task"
    _interval_key = "diskprediction_upload_smart_interval"
    _agents = [SAI_DiskSmartAgent]

    def _run(self):
        self._log.debug("%s run" % self._task_name)
        for agent in self._agents:
            try:
                obj_agent = agent(
                    self._context, self._obj_sender, self._agent_timeout)
                obj_agent.run()
            except Exception:
                continue
