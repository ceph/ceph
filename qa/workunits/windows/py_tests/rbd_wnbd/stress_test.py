# Copyright (C) 2023 Cloudbase Solutions
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation (see LICENSE).

import argparse
import collections
import json
import logging
import math
import os
import prettytable
import random
import time
import threading
import typing
import uuid
from concurrent import futures

from py_tests.internal import exception
from py_tests.internal.rbd_image import RbdImage
from py_tests.internal.tracer import Tracer
from py_tests.internal import utils

LOG = logging.getLogger()

parser = argparse.ArgumentParser(description='rbd-wnbd stress tests')
parser.add_argument('--test-name',
                    help='The test to be run.',
                    default="RbdFioTest")
parser.add_argument('--iterations',
                    help='Total number of test iterations',
                    default=1, type=int)
parser.add_argument('--concurrency',
                    help='The number of tests to run in parallel',
                    default=4, type=int)
parser.add_argument('--fio-iterations',
                    help='Total number of benchmark iterations per disk.',
                    default=1, type=int)
parser.add_argument('--fio-workers',
                    help='Total number of fio workers per disk.',
                    default=1, type=int)
parser.add_argument('--fio-depth',
                    help='The number of concurrent asynchronous operations '
                         'executed per disk',
                    default=64, type=int)
parser.add_argument('--fio-verify',
                    help='The mechanism used to validate the written '
                         'data. Examples: crc32c, md5, sha1, null, etc. '
                         'If set to null, the written data will not be '
                         'verified.',
                    default='crc32c')
parser.add_argument('--bs',
                    help='Benchmark block size.',
                    default="2M")
parser.add_argument('--op',
                    help='Benchmark operation. '
                         'Examples: read, randwrite, rw, etc.',
                    default="rw")
parser.add_argument('--image-prefix',
                    help='The image name prefix.',
                    default="cephTest-")
parser.add_argument('--image-size-mb',
                    help='The image size in megabytes.',
                    default=1024, type=int)
parser.add_argument('--map-timeout',
                    help='Image map timeout.',
                    default=60, type=int)
parser.add_argument('--skip-enabling-disk', action='store_true',
                    help='If set, the disk will not be turned online and the '
                         'read-only flag will not be removed. Useful when '
                         'the SAN policy is set to "onlineAll".')
parser.add_argument('--verbose', action='store_true',
                    help='Print info messages.')
parser.add_argument('--debug', action='store_true',
                    help='Print debug messages.')
parser.add_argument('--stop-on-error', action='store_true',
                    help='Stop testing when hitting errors.')
parser.add_argument('--skip-cleanup-on-error', action='store_true',
                    help='Skip cleanup when hitting errors.')


class RbdTest(object):
    image: RbdImage

    requires_disk_online = False
    requires_disk_write = False

    def __init__(self,
                 image_prefix: str = "cephTest-",
                 image_size_mb: int = 1024,
                 map_timeout: int = 60,
                 **kwargs):
        self.image_size_mb = image_size_mb
        self.image_name = image_prefix + str(uuid.uuid4())
        self.map_timeout = map_timeout
        self.skip_enabling_disk = kwargs.get("skip_enabling_disk")

    @Tracer.trace
    def initialize(self):
        self.image = RbdImage.create(
            self.image_name,
            self.image_size_mb)
        self.image.map(timeout=self.map_timeout)

        if not self.skip_enabling_disk:
            if self.requires_disk_write:
                self.image.set_writable()

            if self.requires_disk_online:
                self.image.set_online()

    def run(self):
        pass

    def cleanup(self):
        if self.image:
            self.image.cleanup()
            self.image = None

    @classmethod
    def print_results(cls,
                      title: str = "Test results",
                      description: str = ''):
        pass


class RbdFsTestMixin(object):
    # Windows disks must be turned online before accessing partitions.
    requires_disk_online = True
    requires_disk_write = True

    @Tracer.trace
    def initialize(self):
        super(RbdFsTestMixin, self).initialize()

        self.image.init_fs()

    def get_subpath(self, *args):
        drive_path = f"{self.image.drive_letter}:\\"
        return os.path.join(drive_path, *args)


class RbdFsTest(RbdFsTestMixin, RbdTest):
    pass


class RbdFioTest(RbdTest):
    data: typing.DefaultDict[str, typing.List[typing.Dict[str, str]]] = (
        collections.defaultdict(list))
    lock = threading.Lock()

    def __init__(self,
                 *args,
                 fio_size_mb: int = 0,
                 iterations: int = 1,
                 workers: int = 1,
                 bs: str = "2M",
                 iodepth: int = 64,
                 op: str = "rw",
                 verify: str = "crc32c",
                 **kwargs):

        super(RbdFioTest, self).__init__(*args, **kwargs)

        self.fio_size_mb = fio_size_mb or self.image_size_mb
        self.iterations = iterations
        self.workers = workers
        self.bs = bs
        self.iodepth = iodepth
        self.op = op
        if op not in ("read", "randread"):
            self.requires_disk_write = True
        self.verify = verify

    def process_result(self, raw_fio_output: str):
        result = json.loads(raw_fio_output)
        with self.lock:
            for job in result["jobs"]:
                # Fio doesn't support trim on Windows
                for op in ['read', 'write']:
                    if op in job:
                        self.data[op].append({
                            'error': job['error'],
                            'io_bytes': job[op]['io_bytes'],
                            'bw_bytes': job[op]['bw_bytes'],
                            'runtime': job[op]['runtime'] / 1000,  # seconds
                            'total_ios': job[op]['short_ios'],
                            'short_ios': job[op]['short_ios'],
                            'dropped_ios': job[op]['short_ios'],
                            'clat_ns_min': job[op]['clat_ns']['min'],
                            'clat_ns_max': job[op]['clat_ns']['max'],
                            'clat_ns_mean': job[op]['clat_ns']['mean'],
                            'clat_ns_stddev': job[op]['clat_ns']['stddev'],
                            'clat_ns_10': job[op].get('clat_ns', {})
                                                 .get('percentile', {})
                                                 .get('10.000000', 0),
                            'clat_ns_90': job[op].get('clat_ns', {})
                                                 .get('percentile', {})
                                                 .get('90.000000', 0)
                        })

    def _get_fio_path(self):
        return self.image.path

    @Tracer.trace
    def _run_fio(self, fio_size_mb: int = 0) -> None:
        LOG.info("Starting FIO test.")
        cmd = [
            "fio", "--thread", "--output-format=json",
            "--randrepeat=%d" % self.iterations,
            "--direct=1", "--name=test",
            "--bs=%s" % self.bs, "--iodepth=%s" % self.iodepth,
            "--size=%sM" % (fio_size_mb or self.fio_size_mb),
            "--readwrite=%s" % self.op,
            "--numjobs=%s" % self.workers,
            "--filename=%s" % self._get_fio_path(),
        ]
        if self.verify:
            cmd += ["--verify=%s" % self.verify]
        result = utils.execute(*cmd)
        LOG.info("Completed FIO test.")
        self.process_result(result.stdout)

    @Tracer.trace
    def run(self):
        self._run_fio()

    @classmethod
    def print_results(cls,
                      title: str = "Benchmark results",
                      description: str = ''):
        if description:
            title = "%s (%s)" % (title, description)

        for op in cls.data.keys():
            op_title = "%s op=%s" % (title, op)

            table = prettytable.PrettyTable(title=op_title)
            table.field_names = ["stat", "min", "max", "mean",
                                 "median", "std_dev",
                                 "max 90%", "min 90%", "total"]
            table.float_format = ".4"

            op_data = cls.data[op]

            s = utils.array_stats(
                [float(i["bw_bytes"]) / 1000_000 for i in op_data])
            table.add_row(["bandwidth (MB/s)",
                           s['min'], s['max'], s['mean'],
                           s['median'], s['std_dev'],
                           s['max_90'], s['min_90'], 'N/A'])

            s = utils.array_stats([float(i["runtime"]) for i in op_data])
            table.add_row(["duration (s)",
                          s['min'], s['max'], s['mean'],
                          s['median'], s['std_dev'],
                          s['max_90'], s['min_90'], s['sum']])

            s = utils.array_stats([i["error"] for i in op_data])
            table.add_row(["errors",
                           s['min'], s['max'], s['mean'],
                           s['median'], s['std_dev'],
                           s['max_90'], s['min_90'], s['sum']])

            s = utils.array_stats([i["short_ios"] for i in op_data])
            table.add_row(["incomplete IOs",
                           s['min'], s['max'], s['mean'],
                           s['median'], s['std_dev'],
                           s['max_90'], s['min_90'], s['sum']])

            s = utils.array_stats([i["dropped_ios"] for i in op_data])
            table.add_row(["dropped IOs",
                           s['min'], s['max'], s['mean'],
                           s['median'], s['std_dev'],
                           s['max_90'], s['min_90'], s['sum']])

            clat_min = utils.array_stats([i["clat_ns_min"] for i in op_data])
            clat_max = utils.array_stats([i["clat_ns_max"] for i in op_data])
            clat_mean = utils.array_stats([i["clat_ns_mean"] for i in op_data])
            clat_stddev = math.sqrt(
                sum([float(i["clat_ns_stddev"]) ** 2
                     for i in op_data]) / len(op_data)
                if len(op_data) else 0)
            clat_10 = utils.array_stats([i["clat_ns_10"] for i in op_data])
            clat_90 = utils.array_stats([i["clat_ns_90"] for i in op_data])
            # For convenience, we'll convert it from ns to seconds.
            table.add_row(["completion latency (s)",
                           clat_min['min'] / 1e+9,
                           clat_max['max'] / 1e+9,
                           clat_mean['mean'] / 1e+9,
                           clat_mean['median'] / 1e+9,
                           clat_stddev / 1e+9,
                           clat_10['mean'] / 1e+9,
                           clat_90['mean'] / 1e+9,
                           clat_mean['sum'] / 1e+9])
            print(table)


class RbdResizeFioTest(RbdFioTest):
    """Image resize test.

    This test extends and then shrinks the image, performing FIO tests to
    validate the resized image.
    """

    @Tracer.trace
    def run(self):
        self.image.resize(self.image_size_mb * 2)
        self.image.wait_for_disk_resize()

        self._run_fio(fio_size_mb=self.image_size_mb * 2)

        self.image.resize(self.image_size_mb // 2, allow_shrink=True)
        self.image.wait_for_disk_resize()

        self._run_fio(fio_size_mb=self.image_size_mb // 2)

        # Just like rbd-nbd, rbd-wnbd is masking out-of-bounds errors.
        # For this reason, we don't have a negative test that writes
        # passed the disk boundary.


class RbdFsFioTest(RbdFsTestMixin, RbdFioTest):
    def initialize(self):
        super(RbdFsFioTest, self).initialize()

        if not self.fio_size_mb or self.fio_size_mb == self.image_size_mb:
            # Out of caution, we'll use up to 80% of the FS by default
            self.fio_size_mb = int(
                self.image.get_fs_capacity() * 0.8 / (1024 * 1024))

    @staticmethod
    def _fio_escape_path(path):
        # FIO allows specifying multiple files separated by colon.
        # This means that ":" has to be escaped, so
        # F:\filename becomes F\:\filename.
        return path.replace(":", "\\:")

    def _get_fio_path(self):
        return self._fio_escape_path(self.get_subpath("test-fio"))


class RbdStampTest(RbdTest):
    requires_disk_write = True

    _write_open_mode = "rb+"
    _read_open_mode = "rb"
    _expect_path_exists = True
    _stamp_size = 512

    def __init__(self, *args, **kwargs):
        super(RbdStampTest, self).__init__(*args, **kwargs)

        # We allow running the test repeatedly, for example after a
        # remount operation.
        self._previous_stamp = b'\0' * self._stamp_size

    @staticmethod
    def _rand_float(min_val: float, max_val: float):
        return min_val + (random.random() * max_val - min_val)

    def _get_stamp(self):
        buff_str = self.image_name + "-" + str(uuid.uuid4())
        buff = buff_str.encode()
        assert len(buff) <= self._stamp_size

        padding = self._stamp_size - len(buff)
        buff += b'\0' * padding
        return buff

    def _get_stamp_path(self):
        return self.image.path

    @Tracer.trace
    def _write_stamp(self, stamp):
        with open(self._get_stamp_path(), self._write_open_mode) as disk:
            disk.write(stamp)

    @Tracer.trace
    def _read_stamp(self):
        with open(self._get_stamp_path(), self._read_open_mode) as disk:
            return disk.read(self._stamp_size)

    @Tracer.trace
    def run(self):
        if self._expect_path_exists:
            # Wait up to 5 seconds and then check the disk, ensuring that
            # nobody else wrote to it. This is particularly useful when
            # running a high number of tests in parallel, ensuring that
            # we aren't writing to the wrong disk.
            time.sleep(self._rand_float(0, 5))

            r_stamp = self._read_stamp()
            assert self._previous_stamp == r_stamp

        w_stamp = self._get_stamp()
        self._write_stamp(w_stamp)

        r_stamp = self._read_stamp()
        assert w_stamp == r_stamp

        self._previous_stamp = w_stamp


class RbdFsStampTest(RbdFsTestMixin, RbdStampTest):
    _write_open_mode = "wb"
    _expect_path_exists = False

    def _get_stamp_path(self):
        return self.get_subpath("test-stamp")


class StressTestRunner(object):
    def __init__(self,
                 test_cls: typing.Type[RbdTest],
                 test_params: dict = {},
                 iterations: int = 1,
                 workers: int = 1,
                 stop_on_error: bool = False,
                 cleanup_on_error: bool = True):
        self.test_cls = test_cls
        self.test_params = test_params
        self.iterations = iterations
        self.workers = workers
        self.executor = futures.ThreadPoolExecutor(max_workers=workers)
        self.lock = threading.Lock()
        self.completed = 0
        self.errors = 0
        self.stopped = False
        self.stop_on_error = stop_on_error
        self.cleanup_on_error = cleanup_on_error

    @Tracer.trace
    def run(self):
        tasks = []
        for i in range(self.iterations):
            task = self.executor.submit(self.run_single_test)
            tasks.append(task)

        LOG.info("Waiting for %d tests to complete.", self.iterations)
        for task in tasks:
            task.result()

    def run_single_test(self):
        failed = False
        if self.stopped:
            return

        try:
            test = self.test_cls(**self.test_params)
            test.initialize()
            test.run()
        except KeyboardInterrupt:
            LOG.warning("Received Ctrl-C.")
            self.stopped = True
        except Exception as ex:
            failed = True
            if self.stop_on_error:
                self.stopped = True
            with self.lock:
                self.errors += 1
                LOG.exception(
                    "Test exception: %s. Total exceptions: %d",
                    ex, self.errors)
        finally:
            if not failed or self.cleanup_on_error:
                try:
                    test.cleanup()
                except KeyboardInterrupt:
                    LOG.warning("Received Ctrl-C.")
                    self.stopped = True
                    # Retry the cleanup
                    test.cleanup()
                except Exception:
                    LOG.exception("Test cleanup failed.")

            with self.lock:
                self.completed += 1
                LOG.info("Completed tests: %d. Pending: %d",
                         self.completed, self.iterations - self.completed)


TESTS: typing.Dict[str, typing.Type[RbdTest]] = {
    'RbdTest': RbdTest,
    'RbdFioTest': RbdFioTest,
    'RbdResizeFioTest': RbdResizeFioTest,
    'RbdStampTest': RbdStampTest,
    # FS tests
    'RbdFsTest': RbdFsTest,
    'RbdFsFioTest': RbdFsFioTest,
    'RbdFsStampTest': RbdFsStampTest,
}

if __name__ == '__main__':
    args = parser.parse_args()

    log_level = logging.WARNING
    if args.verbose:
        log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG
    utils.setup_logging(log_level)

    test_params = dict(
        image_size_mb=args.image_size_mb,
        image_prefix=args.image_prefix,
        bs=args.bs,
        op=args.op,
        verify=args.fio_verify,
        iodepth=args.fio_depth,
        map_timeout=args.map_timeout,
        skip_enabling_disk=args.skip_enabling_disk,
    )

    try:
        test_cls = TESTS[args.test_name]
    except KeyError:
        raise exception.CephTestException(
            "Unknown test: {}".format(args.test_name))

    runner = StressTestRunner(
        test_cls,
        test_params=test_params,
        iterations=args.iterations,
        workers=args.concurrency,
        stop_on_error=args.stop_on_error,
        cleanup_on_error=not args.skip_cleanup_on_error)
    runner.run()

    Tracer.print_results()
    test_cls.print_results(
        description="count: %d, concurrency: %d" %
        (args.iterations, args.concurrency))

    assert runner.errors == 0, f"encountered {runner.errors} error(s)."
