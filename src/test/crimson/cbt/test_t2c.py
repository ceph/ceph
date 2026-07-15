#!/usr/bin/env python3

import os
import sys
import tempfile
import unittest
import unittest.mock

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import t2c  # noqa: E402


SAMPLE_TEUTHOLOGY_YAML = """\
meta:
- desc: sample radosbench workload
tasks:
- install:
    extra_system_packages:
      deb:
      - lvm2
- cbt:
    benchmarks:
      radosbench:
        read_time: 30
        read_only: true
    monitoring_profiles:
      perf:
        nodes:
          - osds
    cluster:
      osds_per_node: 3
      iterations: 1
      pool_profiles:
        replicated:
          pg_size: 128
          pgp_size: 128
          replication: replicated
"""


class TestGetCbtTasks(unittest.TestCase):
    def _write_yaml(self, contents):
        handle = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml',
                                             delete=False)
        handle.write(contents)
        handle.close()
        self.addCleanup(os.unlink, handle.name)
        return handle.name

    def test_extracts_cbt_task(self):
        path = self._write_yaml(SAMPLE_TEUTHOLOGY_YAML)
        tasks = list(t2c.get_cbt_tasks(path))
        self.assertEqual(len(tasks), 1)
        self.assertIn('benchmarks', tasks[0])
        self.assertEqual(tasks[0]['benchmarks']['radosbench']['read_time'], 30)
        self.assertEqual(tasks[0]['cluster']['osds_per_node'], 3)

    def test_ignores_non_cbt_tasks(self):
        path = self._write_yaml("tasks:\n- install:\n    version: main\n")
        tasks = list(t2c.get_cbt_tasks(path))
        self.assertEqual(tasks, [])

    def test_empty_file_returns_no_tasks(self):
        path = self._write_yaml("")
        tasks = list(t2c.get_cbt_tasks(path))
        self.assertEqual(tasks, [])

    def test_missing_tasks_key_returns_no_tasks(self):
        path = self._write_yaml("meta:\n- desc: no tasks here\n")
        tasks = list(t2c.get_cbt_tasks(path))
        self.assertEqual(tasks, [])

    def test_multiple_cbt_tasks(self):
        path = self._write_yaml(
            "tasks:\n"
            "- cbt:\n"
            "    cluster:\n"
            "      iterations: 1\n"
            "- cbt:\n"
            "    cluster:\n"
            "      iterations: 2\n")
        tasks = list(t2c.get_cbt_tasks(path))
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0]['cluster']['iterations'], 1)
        self.assertEqual(tasks[1]['cluster']['iterations'], 2)


class TestTranslator(unittest.TestCase):
    def test_translate_builds_cbt_cluster_section(self):
        build_dir = '/tmp/ceph-build'
        translator = t2c.Translator(build_dir)
        cbt_task = {
            'cluster': {
                'osds_per_node': 4,
                'iterations': 2,
                'pool_profiles': {
                    'replicated': {
                        'pg_size': 64,
                        'pgp_size': 64,
                        'replication': 'replicated',
                    },
                },
            },
            'benchmarks': {
                'radosbench': {
                    'read_time': 10,
                },
            },
            'monitoring_profiles': {
                'perf': {
                    'nodes': ['osds'],
                },
            },
        }

        translated = translator.translate(cbt_task)
        cluster = translated['cluster']
        self.assertEqual(cluster['osds_per_node'], 4)
        self.assertEqual(cluster['iterations'], 2)
        self.assertEqual(cluster['pool_profiles'], cbt_task['cluster']['pool_profiles'])
        self.assertEqual(cluster['conf_file'], os.path.join(build_dir, 'ceph.conf'))
        self.assertEqual(cluster['ceph_cmd'], os.path.join(build_dir, 'bin', 'ceph'))
        self.assertEqual(cluster['rados_cmd'], os.path.join(build_dir, 'bin', 'rados'))
        self.assertEqual(cluster['pid_dir'], os.path.join(build_dir, 'out'))
        self.assertFalse(cluster['rebuild_every_test'])
        self.assertEqual(translated['benchmarks'], cbt_task['benchmarks'])
        self.assertEqual(translated['monitoring_profiles'],
                         cbt_task['monitoring_profiles'])


class TestMain(unittest.TestCase):
    def test_main_writes_translated_yaml(self):
        input_path = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml',
                                                 delete=False)
        input_path.write(SAMPLE_TEUTHOLOGY_YAML)
        input_path.close()
        self.addCleanup(os.unlink, input_path.name)

        output_path = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml',
                                                  delete=False)
        output_path.close()
        self.addCleanup(os.unlink, output_path.name)

        build_dir = '/tmp/ceph-build'
        argv = [
            't2c.py',
            '--build-dir', build_dir,
            '--input', input_path.name,
            '--output', output_path.name,
        ]
        with unittest.mock.patch.object(sys, 'argv', argv):
            t2c.main()

        with open(output_path.name) as output:
            translated = yaml.safe_load(output)
        self.assertIn('cluster', translated)
        self.assertIn('benchmarks', translated)
        self.assertEqual(translated['cluster']['conf_file'],
                         os.path.join(build_dir, 'ceph.conf'))

    def test_main_errors_when_cbt_task_missing(self):
        input_path = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml',
                                                 delete=False)
        input_path.write("tasks:\n- install:\n")
        input_path.close()
        self.addCleanup(os.unlink, input_path.name)

        output_path = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml',
                                                  delete=False)
        output_path.close()
        self.addCleanup(os.unlink, output_path.name)

        argv = [
            't2c.py',
            '--input', input_path.name,
            '--output', output_path.name,
        ]
        with unittest.mock.patch.object(sys, 'argv', argv):
            with self.assertRaises(SystemExit) as ctx:
                t2c.main()
        self.assertEqual(ctx.exception.code, 1)


if __name__ == '__main__':
    unittest.main()
