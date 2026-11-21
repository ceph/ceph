from .exception import MDSPartException
from .analyzer import Analyzer, AnalyzerState
from .mover import Mover
from enum import Enum, unique
import logging
import threading
import time
import json

log = logging.getLogger(__name__)

@unique
class SchedulerState(Enum):
    INITIALIZED          = 0
    INVOKED              = 1
    RUNNING_ANALYZER     = 2
    RUNNING_MOVER        = 3
    WAIT                 = 4
    STOP                 = 5

class Scheduler:
    def __init__(self, analyzer, mover, policy_mgr):
        self.analyzer = analyzer
        self.mover = mover
        self.policy_mgr = policy_mgr
        self.thread = None
        self.stop_event: threading.Event
        self.lock = threading.Lock()
        self.state = SchedulerState.INITIALIZED

    def start(self, policy_name, analyzer_period, scheduler_period):
        log.debug('Scheduler start')
        with self.lock:
            res, msg  = self.policy_mgr.show(policy_name)
            if res == False:
                return False, msg

            if self.state not in [SchedulerState.INITIALIZED, SchedulerState.STOP]:
                return False, f'Scheduler is in {self.state.name}'

            if analyzer_period <= 0:
                return False, f'analyhzer_period should be greater than 0'

            if scheduler_period <= 0:
                return False, f'scheduler_period should be greater than 0'

            self.analyzer_period = analyzer_period
            self.scheduler_period = scheduler_period
            self.policy_name = policy_name
            self.stop_event = threading.Event()
            self.thread = threading.Thread(target=self.run)
            self.thread.start()
            self.state = SchedulerState.INVOKED
            return True, f"Scheduler for {policy_name} has been invoked."

    def stop(self):
        log.debug('Scheduler stop')
        with self.lock:
            if self.state in [SchedulerState.INITIALIZED, SchedulerState.STOP]:
                return False, f'Scheduler is in {self.state.name}'
            self.stop_event.set()

    def wait_next(self):
        start_time = time.time()

        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time >= self.scheduler_period:
                return True
            time.sleep(1)

            if self.stop_event.is_set():
                return True

    def status(self, output_format='json'):
        with self.lock:
            dict_data = {
                            'state': self.state.name,
                            'scheduler_period': self.scheduler_period,
                            'analyzer_period': self.analyzer_period,
                            'policy_name': self.policy_name
                        }
            if output_format == 'dict':
                return dict_data
            return json.dumps(dict_data, indent=4)

    def run_analyzer(self):
        res, msg = self.analyzer.start(self.policy_name, 1000, 1)
        if res != True:
            log.debug(f'Scheduler anayzler error in scheduler: {msg}')
            return res

        log.debug("analyzer running")
        start_time = time.time()
        while True:
            res, data = self.policy_mgr.show(self.policy_name, output_format='dict')
            assert res == True
            elapsed_time = time.time() - start_time

            if data['workload_table_ready'] == 'True' and \
                elapsed_time >= self.analyzer_period:
                log.debug("ready analyzer data")
                break
            time.sleep(5)

            if self.stop_event.is_set():
                break

        log.debug("analyzer check status done")
        self.analyzer.stop()
        log.debug("analyzer stop")
        return True

    def run_mover(self):
        res, msg = self.mover.start(self.policy_name)
        if res != True:
            log.debug(f'Mover start error in scheduler: {msg}')
            return False

        log.debug("mover start")

        while True:
            status = self.mover.status('dict')
            log.debug("mover status ")
            if status['total_jobs'] > 0 and  \
                status['total_jobs'] == status['processed_jobs']:
                break

            time.sleep(5)
            if self.stop_event.is_set():
                break

        self.mover.stop()
        log.debug("mover stop")
        return True

    def run(self):
        log.debug('Scheduler run')

        with self.lock:
            self.state = SchedulerState.RUNNING_ANALYZER

        while not self.stop_event.is_set():
            with self.lock:
                state = self.state

            log.debug(f"Scheduler worker thread is running in {state}.")

            if state == SchedulerState.RUNNING_ANALYZER:
                res = self.run_analyzer()
            elif state == SchedulerState.RUNNING_MOVER:
                res = self.run_mover()
            else:
                res = self.wait_next()

            if res == False:
                state = SchedulerState.WAIT
                continue

            # state transition to next
            with self.lock:
                if state == SchedulerState.RUNNING_ANALYZER:
                    self.state = SchedulerState.RUNNING_MOVER
                elif state == SchedulerState.RUNNING_MOVER:
                    self.state = SchedulerState.WAIT
                else:
                    self.state = SchedulerState.RUNNING_ANALYZER

        with self.lock:
            self.state = SchedulerState.STOP
            self.policy_name = ''
