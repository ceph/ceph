# Copyright (C) 2023 Cloudbase Solutions
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation (see LICENSE).

import argparse
import logging
import typing

from py_tests.internal import exception
from py_tests.internal import task_group
from py_tests.internal.tracer import Tracer
from py_tests.internal import utils
from py_tests.rbd_wnbd import stress_test

LOG = logging.getLogger()

parser = argparse.ArgumentParser(description='rbd-wnbd service restart test')
parser.add_argument('--test-name',
                    help='The test to be run.',
                    default="RbdStampTest")
parser.add_argument('--iterations',
                    help='Total number of test iterations',
                    default=2, type=int)
parser.add_argument('--image-count',
                    help='The number of images to use.',
                    default=8, type=int)
parser.add_argument('--concurrency',
                    help='The number of workers to use when '
                         'initializing and running the tests.',
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
                    default=32, type=int)
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


class ServiceRestartTestRunner(object):
    def __init__(self,
                 test_cls: typing.Type[stress_test.RbdTest],
                 test_params: dict = {},
                 iterations: int = 1,
                 image_count: int = 8,
                 workers: int = 1,
                 stop_on_error: bool = False,
                 cleanup_on_error: bool = True):
        self.test_cls = test_cls
        self.test_params = test_params
        self.iterations = iterations
        self.image_count = image_count
        self.workers = workers
        self.errors = 0
        self.stop_on_error = stop_on_error
        self.cleanup_on_error = cleanup_on_error

        self.test_instances: list[stress_test.RbdTest] = []

    @Tracer.trace
    def initialize(self):
        LOG.info("Initializing mappings")

        tg = task_group.TaskGroup(max_workers=self.workers,
                                  stop_on_error=self.stop_on_error)

        for idx in range(self.image_count):
            test = self.test_cls(**self.test_params)
            self.test_instances.append(test)

            tg.submit(test.initialize)

        tg.join()
        self.errors += tg.errors

    @Tracer.trace
    def cleanup(self):
        LOG.info("Performing cleanup")

        tg = task_group.TaskGroup(max_workers=self.workers,
                                  stop_on_error=self.stop_on_error)

        for test_instance in self.test_instances:
            tg.submit(test_instance.cleanup)

        tg.join()
        self.errors += tg.errors

    @Tracer.trace
    def run_tests(self):
        LOG.info("Running the tests")

        tg = task_group.TaskGroup(max_workers=self.workers,
                                  stop_on_error=self.stop_on_error)

        for test_instance in self.test_instances:
            tg.submit(test_instance.run)

        tg.join()
        self.errors += tg.errors

    @Tracer.trace
    def _restart_service(self):
        LOG.info("Restarting ceph-rbd service")

        utils.ps_execute("restart-service", "ceph-rbd")

    @Tracer.trace
    def _refresh_test_instances(self):
        LOG.info("Refreshing mappings after service restart")

        tg = task_group.TaskGroup(max_workers=self.workers,
                                  stop_on_error=self.stop_on_error)

        for test_instance in self.test_instances:
            tg.submit(test_instance.image.refresh_after_remap)

        tg.join()
        self.errors += tg.errors

    @Tracer.trace
    def run(self):
        try:
            self.initialize()

            for iteration in range(self.iterations):
                self.run_tests()

                self._restart_service()

                self._refresh_test_instances()
        except Exception:
            LOG.exception("Test failed")
            self.errors += 1
        finally:
            if not self.errors or self.cleanup_on_error:
                self.cleanup()


TESTS: typing.Dict[str, typing.Type[stress_test.RbdTest]] = {
    'RbdTest': stress_test.RbdTest,
    'RbdFioTest': stress_test.RbdFioTest,
    'RbdStampTest': stress_test.RbdStampTest,
    # FS tests
    'RbdFsTest': stress_test.RbdFsTest,
    'RbdFsFioTest': stress_test.RbdFsFioTest,
    'RbdFsStampTest': stress_test.RbdFsStampTest,
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

    runner = ServiceRestartTestRunner(
        test_cls,
        test_params=test_params,
        iterations=args.iterations,
        image_count=args.image_count,
        workers=args.concurrency,
        stop_on_error=args.stop_on_error,
        cleanup_on_error=not args.skip_cleanup_on_error)
    runner.run()

    Tracer.print_results()
    test_cls.print_results(
        description="count: %d, concurrency: %d" %
        (args.iterations, args.concurrency))

    assert runner.errors == 0, f"encountered {runner.errors} error(s)."
