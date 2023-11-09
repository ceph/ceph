from typing import Dict, List
import time
import psutil
import argparse
import numpy as np
import matplotlib.pyplot as plt
import subprocess
import os
import json
from threading import Thread, Lock

class ProcessSample:
    def __init__(self, cpu_usage: float, memory_bytes: float, sample_time: float) -> None:
        self.cpu_usage = cpu_usage
        self.memory_bytes = memory_bytes
        self.sample_time = sample_time

def get_osd_pids():
    pids = []
    for proc in psutil.process_iter():
        if 'ceph-osd' in proc.name() and proc.username() == os.getlogin():
            pids.append(proc.pid)
    return pids

def smooth(y, box_pts):
    box = np.ones(box_pts)/box_pts
    y_smooth = np.convolve(y, box, mode='same')
    return y_smooth

class Benchmarker:
    def __init__(self, args):
        self.args = args
        self.finished_bench = False
        self.warmup_completed = False

    def benchmark_run_and_wait(self):
        bench_process = subprocess.Popen(f'{self.args.bench}'.split(' '))
        bench_process.wait()

    def run_bench(self, samples, iterations, warmup_iterations):
        print('Running warmup')
        for iteration in range(warmup_iterations):
            self.benchmark_run_and_wait()
        self.warmup_completed = True

        print('Running benchmark')
        for sample in range(samples):
            for iteration in range(iterations):
                self.benchmark_run_and_wait()

    def bench_entrypoint(self):
        self.run_bench(self.args.samples, self.args.iterations, self.args.iterations)
        self.finished_bench = True

    def run(self):
        if self.args.new_cluster:
            subprocess.run(f'../src/stop.sh'.split(' '))
            subprocess.run(f'../src/vstart.sh -n -x'.split(' '), env={"MON": "1", "OSD": str(self.args.osds), "MGR": "0", "MDS": "0"})

            subprocess.run('bin/ceph osd pool create test'.split(' '))
            subprocess.run('bin/ceph osd pool set test pg_autoscale_mode off'.split(' '))
            subprocess.run(f'bin/ceph osd pool set test pg_num {self.args.pgs}'.split(' '))

        samples_per_process: Dict[int, List[ProcessSample]] = {}
        processes: List[psutil.Process] = []
        pids = get_osd_pids()
        for pid in pids:
            process = psutil.Process(int(pid))
            processes.append(process)
            process.cpu_percent()
            samples_per_process[process.pid] = []



        freq = self.args.freq
        start = time.time()
        period = self.args.period
        print(self.args.bench)
        run_bench = self.args.bench != ""
        if run_bench:
            bench_thread = Thread(target=self.bench_entrypoint)
            bench_thread.start()


        while run_bench and not self.warmup_completed:
            time.sleep(freq)

        if not run_bench:
            print(f'Collecting data for {period}s')
        while (run_bench and not self.finished_bench) or (not run_bench and time.time() - start < period):
            time.sleep(freq)

            sample_time = (time.time_ns() - (start*1000000000)) / 1000000
            for process in processes:
                cpu_usage = process.cpu_percent()
                mem_bytes = process.memory_info().rss
                sample = ProcessSample(cpu_usage, mem_bytes, sample_time)
                samples_per_process[process.pid].append(sample)
            
        
        if run_bench:
            bench_thread.join()


        # save data to compare with other runs
        save_data = {}

        for process in processes:
            name = f'{process.name()}.{process.pid}'
            save_data[name] = {}


        for process in processes:
            x = [sample.sample_time for sample in samples_per_process[process.pid]]
            ycpu = [sample.cpu_usage for sample in samples_per_process[process.pid]]
            # use MB
            ymem = [sample.memory_bytes/1024/1024 for sample in samples_per_process[process.pid]]

            name = f'{process.name()}.{process.pid}'
            save_data[name]['cpu'] = {}
            save_data[name]['cpu']['x'] = x
            save_data[name]['cpu']['y'] = ycpu
            save_data[name]['cpu']['stddev'] = np.std(ycpu)
            save_data[name]['cpu']['mean'] = np.mean(ycpu)
            save_data[name]['cpu']['median'] = np.median(ycpu)
            save_data[name]['mem'] = {}
            save_data[name]['mem']['x'] = x
            save_data[name]['mem']['y'] = ymem
            save_data[name]['mem']['stddev'] = np.std(ymem)
            save_data[name]['mem']['mean'] = np.mean(ymem)
            save_data[name]['mem']['median'] = np.median(ymem)

        now = int(time.time())
        with open(f'{now}.json', '+w') as f:
            json.dump(save_data, f)

        self.compare(files=[f'{now}.json', 'previous.json'])

        with open(f'previous.json', '+w') as f:
            json.dump(save_data, f)

    def compare(self, files=[]):
        if not files:
            files = self.args.files

        num_files = len(files)
        rows = num_files 
        columns = 2 # mem, cpu
        fig, ax = plt.subplots(rows, columns, figsize=(30,20))

        for i in range(len(files)):
            file_path = files[i]

            data = {}
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    data = json.load(f)

            cpu_aggregate: np.ndarray = np.array([])
            mem_aggregate: np.ndarray = np.array([])

            for name in data.keys():
                x = data[name]['cpu']['x']
                ycpu = np.array(data[name]['cpu']['y'])
                ymem = np.array(data[name]['mem']['y'])

                cpu_aggregate = np.append(cpu_aggregate, ycpu)
                mem_aggregate = np.append(mem_aggregate, ymem)


                def plot_data(row, col, title, file_path, x, y):
                    ax[row, col].set_title(f'{title} {file_path}')
                    ax[row, col].set_xlabel('Time')
                    ax[row, col].set_ylabel(title)

                    ax[row, col].plot(x, smooth(y, 10), dash_joinstyle="round", color='#507baf')


                plot_data(i, 0, 'Cpu usage', file_path, x, ycpu)
                plot_data(i, 1, 'Memory (MB)', file_path, x, ymem)

            def plot_stats(row, col, y):
                _max = max(y)
                _min = min(y)
                _avg = np.average(y)
                _p95 = np.percentile(y, 95)
                _p90 = np.percentile(y, 90)
                print(y)
                try:
                    ax[row, col].set_yticks(np.arange(_min, _max, int(_max/50)))
                except:
                    ax[row, col].set_yticks(y)
                ax[row, col].axhline(_avg, label='avg', color='red', in_layout=True)
                ax[row, col].axhline(_p90, label='p90', color='#c676db', in_layout=True)
                ax[row, col].axhline(_p95, label='p95', color='#a533c1', in_layout=True)
                ax[row, col].legend(bbox_to_anchor=(1.0, 1), loc='upper left')

            print(cpu_aggregate)
            plot_stats(i, 0, cpu_aggregate)
            plot_stats(i, 1, mem_aggregate)
        plt.tight_layout()
        plt.savefig(f'benchmark.{self.args.format}', format=self.args.format)



def main():
    parser = argparse.ArgumentParser(
                        prog='ProgramName',
                        description='What the program does',
                        epilog='Text at the bottom of help')

    parser.add_argument('--period', type=int)
    parser.add_argument('--new-cluster', '-n', action='store_true', help="Set this flag if you want to create a new cluster with available parameters.")
    parser.add_argument('--osds', type=int, default=1)
    parser.add_argument('--pgs', type=int, default=128)
    parser.add_argument('--freq', type=int, default=0.1, help="Frequency of sampling")
    parser.add_argument('--samples', type=int, default=5, help="Number of samples")
    parser.add_argument('--iterations', type=int, default=5, help="Number of iterations per sample")
    parser.add_argument('--bench', type=str, default="")
    parser.add_argument('--format', help="format of matplotlib plots saved image", type=str, default='png')
    parser.set_defaults(func=Benchmarker.run)

    compare_subparser = parser.add_subparsers(help="compare results")
    compare_parser = compare_subparser.add_parser('compare', help="compare results")
    compare_parser.add_argument('files', help="Results to compare", type=str, nargs='+')
    compare_parser.set_defaults(func=Benchmarker.compare)

    args = parser.parse_args()
    print(args.__dict__)
    bench = Benchmarker(args)
    args.func(bench)


if __name__ == "__main__":
    main()