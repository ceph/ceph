from .exception import MDSPartException
from .perf_metric import MetricType
from .workload_table import WorkloadTable, SubdirState
from .workload_policy_mgr import WorkloadPolicyMgr
from prettytable import PrettyTable
from enum import Enum, unique
from mgr_util import RTimer
import threading
import datetime
import logging
import json
import time

log = logging.getLogger(__name__)

@unique
class JobState(Enum):
    STOPPED  = 0
    STARTED  = 1
    RUNNING  = 2
    DONE     = 3

@unique
class AnalyzerState(Enum):
    STOPPED  = 0
    STARTED  = 1
    RUNNING  = 2


METRIC_COLLECT_INTERVAL = 1

class AnalyzerTimerCtx:

    def __init__(self, duration, interval, task_func):
        self.start_datetime = datetime.datetime.now()
        self.finish_datetime = self.start_datetime + datetime.timedelta(seconds=duration)
        self.interval = interval
        self.fs_metric_types = [MetricType.metadata_latency, MetricType.stddev_metadata_latency]
        self.fs_metrics: dict = { metric_type.name: { 'first': None, 'last': None } for metric_type in self.fs_metric_types }
        self.timer_task = RTimer(METRIC_COLLECT_INTERVAL, task_func)
        self.state = JobState.STOPPED
        self.subtree_set: set = set()
        self.terminated = threading.Event()
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)


class Analyzer:
    def __init__(self, mgr, policy_mgr, fsmetric, fs_name):
        log.debug("Init Analyzer module.")
        self.mgr = mgr
        self.rados = mgr.rados
        self.fsmetric = fsmetric
        self.fs_map = self.mgr.get('fs_map')
        self.fs_name = fs_name
        self.state = AnalyzerState.STOPPED
        self.policy_mgr: WorkloadPolicyMgr = policy_mgr
        self.policy_name = None
        self.stopping = threading.Event()
        self.lock = threading.Lock()
        self.timer_ctx:AnalyzerTimerCtx

    def _check_finish(self):
        if self.timer_ctx.finish_datetime and datetime.datetime.now() > self.timer_ctx.finish_datetime:
            return True
        return False

    def _collect_fs_metric(self):
        log.debug(f'timer task: _collect_fs_metric {self.fs_name}')
        subtree_perf_metrics = self.fsmetric.collect_avg_metadata_latency(self.fs_name, self.timer_ctx.subtree_set,\
                                                                                        self.timer_ctx.fs_metric_types)
        last_metric_ready = False

        for metric_type, metric in subtree_perf_metrics.items():
            if not self.timer_ctx.fs_metrics[metric_type]['first']:
                self.timer_ctx.fs_metrics[metric_type]['first'] = metric
            else:
                self.timer_ctx.fs_metrics[metric_type]['last'] = metric
                last_metric_ready = True

        if last_metric_ready:
            for metric_type, metric in subtree_perf_metrics.items():
                for subtree in self.timer_ctx.subtree_set:
                    if subtree not in self.timer_ctx.fs_metrics[metric_type]['first']:
                        log.debug(f'metric of subtree is added to first metric due to newly added')
                        self.timer_ctx.fs_metrics[metric_type]['first'][subtree] = metric[subtree]

        log.debug(f'timer task: _collect_fs_metric {self.fs_name} done')
        return last_metric_ready

    def timer_handler(self):
        with self.lock:
            if self.stopping.is_set():
                return

            self.timer_ctx.state = JobState.RUNNING
            self.state = AnalyzerState.RUNNING

            self._update_subtree_list()
            metric_ready = self._collect_fs_metric()
            if  metric_ready:
                self._update_workload_table()

            if self._check_finish():
                self.timer_ctx.timer_task.cancel()
                self.timer_ctx.state = JobState.DONE

    def _update_workload_table(self):
        subtree_perf_metric = self._calc_metrics()
        self.policy_mgr.update_workload_table(self.policy_name, subtree_perf_metric)

    def _update_subtree_list(self):
        _, self.timer_ctx.subtree_set = self.policy_mgr.list_subtree(self.policy_name, use_subtree_set=False)

    def _calc_metrics(self):
        log.debug('_calc_metrics')
        lat_first = self.timer_ctx.fs_metrics['metadata_latency']['first']
        lat_last = self.timer_ctx.fs_metrics['metadata_latency']['last']

        if not lat_first or not lat_last:
            return {}

        cnt_first = self.timer_ctx.fs_metrics['stddev_metadata_latency']['first']
        cnt_last = self.timer_ctx.fs_metrics['stddev_metadata_latency']['last']

        results = {}
        for dirpath, lat_list in lat_first.items():
            lat_results = []
            count_results = []
            for client_id, value in lat_list.items():
                lat_first_value = value
                lat_last_value = lat_last[dirpath][client_id]
                lat_diff = lat_last_value - lat_first_value
                lat_results.append(lat_diff)

                count_first_value = cnt_first[dirpath][client_id]
                count_last_value = cnt_last[dirpath][client_id]
                count_diff = count_last_value - count_first_value
                count_results.append(count_diff)

            latency_sum = sum(lat_results)
            count_sum = sum(count_results)
            avg = latency_sum / count_sum if latency_sum > 0 and count_sum > 0 else 0
            results[dirpath] = {
                                'latency_sum': latency_sum,
                                'count': count_sum,
                                'avg': avg
                               }
        return results

    def _get_state_all(self):
        return {
                   'fs_name': self.fs_name,
                   'policy_name': self.policy_name,
                   'subtree_list': list(self.timer_ctx.subtree_set),
                   'start_datetime': self.timer_ctx.start_datetime.strftime("%x_%X"),
                   'finish_datetime': self.timer_ctx.finish_datetime.strftime("%x_%X"),
                   'interval': self.timer_ctx.interval,
                   'timer_state': self.timer_ctx.state.name,
                   'fs_metrics': self._calc_metrics(),
                   'state': self.state.name,
               }


    def start(self, policy_name, duration, interval):
        with self.lock:
            if self.state == AnalyzerState.RUNNING or \
                self.state == AnalyzerState.STARTED:
                log.debug(f"Analzyer already started {self.fs_name}")
                return False, f'analyze already started {self.fs_name}'

            self.policy_name = policy_name
            timer_ctx = AnalyzerTimerCtx(duration, interval, self.timer_handler)
            timer_ctx.timer_task.start()
            timer_ctx.state = JobState.STARTED
            self.stopping.clear()
            self.timer_ctx = timer_ctx
            self.state = AnalyzerState.STARTED

            return True, f'analyze start {self.fs_name}'

    def stop(self):
        with self.lock:
            if self.state == AnalyzerState.STOPPED:
                log.debug(f"Analzyer is not enabled.")
                return

            if self.timer_ctx.state == JobState.RUNNING:
                self.stopping.set()
                self.timer_ctx.timer_task.cancel()
                self.timer_ctx.state = JobState.DONE
                self.policy_name = None

            if self.state == AnalyzerState.RUNNING:
                self.state = AnalyzerState.STOPPED

    def status(self, output_format='dict'):
        with self.lock:
            dict_data = self._get_state_all()
            if output_format == 'dict':
                return dict_data
            elif output_format == 'json':
                return json.dumps(dict_data, indent=4)
            else:
                table = PrettyTable()
                table.field_names = ['key', 'value']
                for k, v in dict_data.items():
                    if k in ['fs_metrics', 'subtree_list']:
                        continue
                    table.add_row([k, v])
                return table.get_string(header=False)

