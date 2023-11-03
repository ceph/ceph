from typing import Dict, List
import time
import psutil
import argparse
import numpy as np
import matplotlib.pyplot as plt
import subprocess
import os
import json

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
    parser.add_argument('--rados_bench_args', type=str, default="write --no-cleanup 5")
    args = parser.parse_args()

    if args.new_cluster:
        subprocess.run(f'../src/stop.sh'.split(' '))
        subprocess.run(f'../src/vstart.sh -n -x'.split(' '), env={"MON": "1", "OSD": str(args.osds), "MGR": "0", "MDS": "0"})

        subprocess.run('bin/ceph osd pool create test'.split(' '))
        subprocess.run('bin/ceph osd pool set test pg_autoscale_mode off'.split(' '))
        subprocess.run(f'bin/ceph osd pool set test pg_num {args.pgs}'.split(' '))

    samples_per_process: Dict[int, List[ProcessSample]] = {}
    processes: List[psutil.Process] = []
    pids = get_osd_pids()
    for pid in pids:
        process = psutil.Process(int(pid))
        processes.append(process)
        process.cpu_percent()
        samples_per_process[process.pid] = []



    freq = args.freq
    start = time.time()
    period = args.period
    print(args.rados_bench_args)
    rados_bench_process = subprocess.Popen(f'bin/rados bench -p test {args.period} write {args.rados_bench_args}'.split(' '))
    while time.time() - start < period:
        time.sleep(freq)

        sample_time = (time.time_ns() - (start*1000000000)) / 1000000
        for process in processes:
            cpu_usage = process.cpu_percent()
            mem_bytes = process.memory_info().rss
            sample = ProcessSample(cpu_usage, mem_bytes, sample_time)
            samples_per_process[process.pid].append(sample)
    
    rados_bench_process.wait()


    # save data to compare with other runs
    save_data = {}

    # plot cpu and memory usage

    prev_data = {}
    if os.path.exists('last_bench.json'):
        with open('last_bench.json', 'r') as f:
            prev_data = json.load(f)


    rows = 2
    columns = 2

    fig, ax = plt.subplots(rows, columns)

    for process in processes:
        name = f'{process.name()}.{process.pid}'
        save_data[name] = {}


    cpu_delta = 0
    memory_delta = 0
    for process in processes:
        x = [sample.sample_time for sample in samples_per_process[process.pid]]
        ycpu = [sample.cpu_usage for sample in samples_per_process[process.pid]]
        # use MBs
        ymem = [sample.memory_bytes/1024/1024 for sample in samples_per_process[process.pid]]

        # kde = gaussian_kde(y)
        # dist_space = np.linspace( min(y), max(y), 100 )
        # ax.plot(dist_space, kde(dist_space))

        ax[0, 0].set_title('Cpu usage current')
        ax[0, 1].set_title('Mem used current (MBs)')

        ax[0, 0].set_ylabel('Cpu Usage')
        ax[0, 1].set_ylabel('Mem used(MBs)')

        ax[0, 0].plot(x, smooth(ycpu, 10), dash_joinstyle="round")
        ax[0, 1].plot(x, smooth(ymem, 10), dash_joinstyle="round")

        cpu_delta += np.std(ycpu)
        memory_delta += np.std(ymem)

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

    # Plot last data
    for name in prev_data.keys():
        x = prev_data[name]['cpu']['x']
        ycpu = prev_data[name]['cpu']['y']
        ymem = prev_data[name]['mem']['y']

        cpu_delta -= np.std(ycpu)
        memory_delta -= np.std(ymem)

        ax[1, 0].set_title('Cpu usage previous')
        ax[1, 1].set_title('Mem usage last (MBs)')

        ax[1, 0].set_xlabel('Time')
        ax[1, 1].set_xlabel('Time')

        ax[1, 0].set_ylabel('Cpu Usage')
        ax[1, 1].set_ylabel('Mem used (MBs)')

        ax[1, 0].plot(x, smooth(ycpu, 10), dash_joinstyle="round")
        ax[1, 1].plot(x, smooth(ymem, 10), dash_joinstyle="round")

    ax[0, 0].text(1, 1, f'Regression: {round(cpu_delta, 1)}', transform=ax[0, 0].transAxes, horizontalalignment='right', verticalalignment='top')
    ax[0, 1].text(1, 1, f'Regression: {int(memory_delta)}', transform=ax[0, 1].transAxes, horizontalalignment='right', verticalalignment='top')


    plt.tight_layout()
    plt.savefig('benchmark.png')

    now = int(time.time())
    with open(f'{now}.json', '+w') as f:
        json.dump(save_data, f)
    with open(f'previous.json', '+w') as f:
        json.dump(save_data, f)

if __name__ == "__main__":
    main()