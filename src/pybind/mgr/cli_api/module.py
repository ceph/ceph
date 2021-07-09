import json
import logging
import threading
import time
from functools import partial
from queue import Queue

from mgr_module import CLICommand, HandleCommandResult, MgrModule

logger = logging.getLogger()


class CLI(MgrModule):

    @CLICommand('mgr api get')
    def api_get(self, arg: str):
        '''
        Called by the plugin to fetch named cluster-wide objects from ceph-mgr.
        :param str data_name: Valid things to fetch are osd_crush_map_text,
                osd_map, osd_map_tree, osd_map_crush, config, mon_map, fs_map,
                osd_metadata, pg_summary, io_rate, pg_dump, df, osd_stats,
                health, mon_status, devices, device <devid>, pg_stats,
                pool_stats, pg_ready, osd_ping_times.
        Note:
            All these structures have their own JSON representations: experiment
            or look at the C++ ``dump()`` methods to learn about them.
        '''
        t1_start = time.time()
        str_arg = self.get(arg)
        t1_end = time.time()
        time_final = (t1_end - t1_start)
        return HandleCommandResult(0, json.dumps(str_arg), str(time_final))

    @CLICommand('mgr api benchmark get')
    def api_get_benchmark(self, arg: str, number_of_total_calls: int,
                          number_of_parallel_calls: int):
        benchmark_runner = ThreadedBenchmarkRunner(number_of_total_calls, number_of_parallel_calls)
        benchmark_runner.start(partial(self.get, arg))
        benchmark_runner.join()
        stats = benchmark_runner.get_stats()
        return HandleCommandResult(0, json.dumps(stats), "")


class ThreadedBenchmarkRunner:
    def __init__(self, number_of_total_calls, number_of_parallel_calls):
        self._number_of_parallel_calls = number_of_parallel_calls
        self._number_of_total_calls = number_of_total_calls
        self._threads = []
        self._jobs: Queue = Queue()
        self._time = 0.0
        self._self_time = []
        self._lock = threading.Lock()

    def start(self, func):
        if(self._number_of_total_calls and self._number_of_parallel_calls):
            for thread_id in range(self._number_of_parallel_calls):
                new_thread = threading.Thread(target=ThreadedBenchmarkRunner.timer,
                                              args=(self, self._jobs, func,))
                self._threads.append(new_thread)
            for job_id in range(self._number_of_total_calls):
                self._jobs.put(job_id)
            for thread in self._threads:
                thread.start()
        else:
            raise BenchmarkException("Number of Total and number of parallel calls must be greater than 0")

    def join(self):
        for thread in self._threads:
            thread.join()

    def get_stats(self):
        stats = {
            "avg": (self._time / self._number_of_total_calls),
            "min": min(self._self_time),
            "max": max(self._self_time)
        }
        return stats

    def timer(self, jobs, func):
        self._lock.acquire()
        while not self._jobs.empty():
            jobs.get()
            time_start = time.time()
            func()
            time_end = time.time()
            self._self_time.append(time_end - time_start)
            self._time += (time_end - time_start)
            self._jobs.task_done()
        self._lock.release()


class BenchmarkException(Exception):
    pass
