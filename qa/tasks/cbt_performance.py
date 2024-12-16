import logging
import requests
import re
import os
import json

from pathlib import Path
from io import StringIO
from teuthology import misc

server  = "http://mira118.front.sepia.ceph.com"
api_port = "4000"
grafana_port = "3000"
schema_name = "public"
table_name = "cbt_performance"
user_name = "postgres"
password = "root"

class CBTperformance:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.auth = (user_name, password)
        self.endpoint_url = f'{server}:{api_port}/{table_name}'
        self.headers = {'Content-Type': 'application/json'}

    def collect(self, ctx, config):
        self.log.info('Collecting CBT performance data config')

        tasks = ctx.config.get('tasks', None)
        for task in tasks:
            if "cbt" in task:
                benchmark = task["cbt"]
                break

        cbt_results_arry = self.read_results(ctx, config)
        for cbt_results in cbt_results_arry:
            cbt_results = json.loads(json.dumps(cbt_results))
            if cbt_results:
                data = {
                    "job_id" : ctx.config.get('job_id', None),
                    "started_at" : ctx.config.get('timestamp', None),
                    "benchmark_mode" : cbt_results.get("Benchmark_mode", None),
                    "seq" : cbt_results.get("seq", None),
                    "total_cpu_cycles" : cbt_results.get("total_cpu_cycles", None),
                    "branch" : ctx.config.get('branch', None),
                    "sha1" : ctx.config.get('sha1', None),
                    "os_type" : ctx.config.get('os_type', None),
                    "os_version" : ctx.config.get('os_version', None),
                    "machine_type" : ctx.config.get('machine_type', None),
                    "benchmark" : benchmark["benchmarks"],
                    "results" : cbt_results.get("results", None),
                }
                response = requests.post(self.endpoint_url, json=data, headers=self.headers, auth=self.auth)
                if response.status_code == 201:
                    self.log.info("Data inserted successfully.")
                    ctx.summary['cbt_perf_url'] = self.create_cbt_perf_url(ctx, config)
                else:
                    self.log.info(f"Error inserting data: {response}")


    def read_results(self, ctx, config):
        results = []
        if not config.get('pref_read', True):                  #change to False to disable
            return results

        self.log.info('reading cbt preferences')
        testdir = misc.get_testdir(ctx)
        first_mon = next(iter(ctx.cluster.only(misc.get_first_mon(ctx, config)).remotes.keys()))

        # return all json_results files from remote
        proc = first_mon.run(
                    args=[
                        'find', '{tdir}'.format(tdir=testdir), '-name',
                        'json_output.*'
                    ],
                    stdout=StringIO(),
                    wait=True
                )
        json_output_paths = proc.stdout.getvalue().split('\n')

        for json_output_path in json_output_paths:
            if json_output_path:
                path_full = Path(json_output_path)
                match = re.search(r'/json_output\.(?P<json>\d+)', json_output_path)
                if match:
                    Benchmark_mode = path_full.parent.name if path_full.parent.name in ['rand', 'write', 'seq'] else 'fio'
                    seq = match.group('json')

                    results.append(
                                   {  "results": json.loads(first_mon.read_file(json_output_path).decode('utf-8'))
                                    , "Benchmark_mode": Benchmark_mode
                                    , "seq": seq
                                    , "total_cpu_cycles": self.read_total_cpu_cycles(ctx, config, os.path.dirname(json_output_path))
                                    }
                                )
        return results

    def read_total_cpu_cycles(self, ctx, config, testdir):
        if not config.get('pref_read', True):                  #change to False to disable
            return None

        self.log.info('reading total cpu cycles')
        first_mon = next(iter(ctx.cluster.only(misc.get_first_mon(ctx, config)).remotes.keys()))

        # return all json_results files from remote
        proc = first_mon.run(
                args=[
                    'find', '{tdir}'.format(tdir=testdir), '-name',
                    'perf_stat.*'
                ],
                stdout=StringIO(),
                wait=True
        )

        cpu_cycles_paths = proc.stdout.getvalue().split('\n')
        self.log.info(f'cpu_cycles_paths: {cpu_cycles_paths}')
        total_cpu_cycles = 0
        for cpu_cycles_path in cpu_cycles_paths:
            if not cpu_cycles_path:
                continue

            match = re.search(r'(.*) cycles(.*?) .*', first_mon.read_file(cpu_cycles_path).decode('utf-8'), re.M | re.I)
            if not match:
                continue

            cpu_cycles = match.group(1).strip()
            total_cpu_cycles = total_cpu_cycles + int(cpu_cycles.replace(',', ''))

        self.log.info(f'total cpu cycles: {total_cpu_cycles}')
        return total_cpu_cycles

    def create_cbt_perf_url(self, ctx, config):
        tasks = ctx.config.get('tasks', None)
        for task in tasks:
            if "cbt" in task:
                benchmark = task["cbt"]
                break

        is_fio = benchmark["benchmarks"].get("librbdfio")
        if is_fio:
            Dash_id = '2Jx1MmfVk/fio'
        else:
            is_radosbench = benchmark["benchmarks"].get("radosbench")
            if is_radosbench:
                Dash_id = 'Rfy7LkB4k/rados-bench'
            else:
                return None

        job_id = ctx.config.get('job_id', None)
        branch = ctx.config.get('branch', None)

        return f'{server}:{grafana_port}/d/{Dash_id}?orgId=1&var-branch_name={branch}&var-job_id_selected={job_id}'
