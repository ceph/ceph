#!/usr/bin/python3

import argparse
import copy
import json
import rados
import time
import multiprocessing

caps_base = ["mon", "profile rbd", "osd", "profile rbd pool=rbd namespace=test"]

def create_users(conn, num_namespaces, num_users):
    cmd = {'prefix': 'auth get-or-create'}

    for i in range(num_namespaces):
        caps_base[-1] += ", profile rbd pool=rbd namespace=namespace{}".format(i)

    cmd['caps'] = caps_base
    for i in range(num_users):
        cmd['entity'] = "client.{}".format(i)
        conn.mon_command(json.dumps(cmd), b'')

class Worker(multiprocessing.Process):
    def __init__(self, conn, num, queue, duration):
        super().__init__()
        self.conn = conn
        self.num = num
        self.queue = queue
        self.duration = duration

    def run(self):
        client = "client.{}".format(self.num)
        cmd = {'prefix': 'auth caps', 'entity': client}
        start_time = time.time()
        num_complete = 0
        with rados.Rados(conffile='') as conn:
            while True:
                now = time.time()
                diff = now - start_time
                if diff > self.duration:
                    self.queue.put((num_complete, diff))
                    return
                caps = copy.deepcopy(caps_base)
                caps[-1] += ", profile rbd pool=rbd namespace=namespace{}".format(self.num * 10000 + num_complete)
                cmd['caps'] = caps
                cmd_start = time.time()
                ret, buf, out = conn.mon_command(json.dumps(cmd), b'')
                cmd_end = time.time()
                if ret != 0:
                    self.queue.put((Exception("{0}: {1}".format(ret, out)), 0))
                    return
                num_complete += 1
                print("Process {} finished op {} - latency: {}".format(self.num, num_complete, cmd_end - cmd_start))

def main():
    parser = argparse.ArgumentParser(description="""
Benchmark updates to ceph users' capabilities. Run one update at a time in each thread.
""")
    parser.add_argument(
        '-n', '--num-namespaces',
        type=int,
        default=300,
        help='number of namespaces per user',
    )
    parser.add_argument(
        '-t', '--threads',
        type=int,
        default=10,
        help='number of threads (and thus parallel operations) to use',
    )
    parser.add_argument(
        '-d', '--duration',
        type=int,
        default=30,
        help='how long to run, in seconds',
    )
    args = parser.parse_args()
    num_namespaces = args.num_namespaces
    num_threads = args.threads
    duration = args.duration
    workers = []
    results = []
    q = multiprocessing.Queue()
    with rados.Rados(conffile=rados.Rados.DEFAULT_CONF_FILES) as conn:
        create_users(conn, num_namespaces, num_threads)
    for i in range(num_threads):
        workers.append(Worker(conn, i, q, duration))
        workers[-1].start()
    for i in range(num_threads):
        num_complete, seconds = q.get()
        if isinstance(num_complete, Exception):
            raise num_complete
        results.append((num_complete, seconds))
    total = 0
    total_rate = 0
    for num, sec in results:
        print("Completed {} in {} ({} / s)".format(num, sec, num / sec))
        total += num
        total_rate += num / sec

    print("Total: ", total)
    print("Avg rate: ", total_rate / len(results))

if __name__ == '__main__':
    main()
