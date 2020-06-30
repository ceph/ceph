#!/usr/bin/env python3

from __future__ import print_function
import argparse
import os
import os.path
import socket
import sys
import yaml


class Translator(object):
    def __init__(self, build_dir):
        self.build_dir = build_dir

    def translate(self, config):
        cluster = config.get('cluster', {})
        benchmarks = config.get('benchmarks', [])
        monitoring_profiles = config.get('monitoring_profiles', {})
        return dict(cluster=self._create_cluster_config(cluster),
                    benchmarks=benchmarks,
                    monitoring_profiles=monitoring_profiles)

    def _create_cluster_config(self, cluster):
        # prepare the "cluster" section consumed by CBT
        localhost = socket.getfqdn()
        num_osds = cluster.get('osds_per_node', 3)
        items_to_copy = ['iterations', 'pool_profiles']
        conf = dict((k, cluster[k]) for k in items_to_copy if k in cluster)
        conf.update(dict(
            head=localhost,
            osds=[localhost],
            osds_per_node=num_osds,
            mons=[localhost],
            clients=[localhost],
            rebuild_every_test=False,
            conf_file=os.path.join(self.build_dir, 'ceph.conf'),
            ceph_cmd=os.path.join(self.build_dir, 'bin', 'ceph'),
            rados_cmd=os.path.join(self.build_dir, 'bin', 'rados'),
            pid_dir=os.path.join(self.build_dir, 'out')
        ))
        return conf 

def get_cbt_tasks(path):
    with open(path) as input:
        teuthology_config = yaml.load(input)
    for task in teuthology_config['tasks']:
        for name, conf in task.items():
            if name == 'cbt':
                yield conf

def main():
    parser = argparse.ArgumentParser(description='translate teuthology yaml to CBT yaml')
    parser.add_argument('--build-dir',
                        default=os.getcwd(),
                        required=False,
                        help='Directory where CMakeCache.txt is located')
    parser.add_argument('--input',
                        required=True,
                        help='The path to the input YAML file')
    parser.add_argument('--output',
                        required=True,
                        help='The path to the output YAML file')
    options = parser.parse_args(sys.argv[1:])
    cbt_tasks = [task for task in get_cbt_tasks(options.input)]
    if not cbt_tasks:
        print('cbt not found in "tasks" section', file=sys.stderr)
        return sys.exit(1)
    elif len(cbt_tasks) > 1:
        print('more than one cbt task found in "tasks" section', file=sys.stderr)
        return sys.exit(1)
    translator = Translator(options.build_dir)
    cbt_config = translator.translate(cbt_tasks[0])
    with open(options.output, 'w') as output:
        yaml.dump(cbt_config, output)

if __name__ == '__main__':
    main()
