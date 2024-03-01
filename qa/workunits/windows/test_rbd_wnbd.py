import argparse
import collections
import functools
import json
import logging
import math
import os
import prettytable
import random
import subprocess
import time
import threading
import typing
import uuid
from concurrent import futures

LOG = logging.getLogger()

parser = argparse.ArgumentParser(description='rbd-wnbd tests')
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


class CephTestException(Exception):
    msg_fmt = "An exception has been encountered."

    def __init__(self, message: str = '', **kwargs):
        self.kwargs = kwargs
        if not message:
            message = self.msg_fmt % kwargs
        self.message = message
        super(CephTestException, self).__init__(message)


class CommandFailed(CephTestException):
    msg_fmt = (
        "Command failed: %(command)s. "
        "Return code: %(returncode)s. "
        "Stdout: %(stdout)s. Stderr: %(stderr)s.")


class CephTestTimeout(CephTestException):
    msg_fmt = "Operation timeout."


def setup_logging(log_level: int = logging.INFO):
    handler = logging.StreamHandler()
    handler.setLevel(log_level)

    log_fmt = '[%(asctime)s] %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_fmt)
    handler.setFormatter(formatter)

    LOG.addHandler(handler)
    LOG.setLevel(logging.DEBUG)


def retry_decorator(timeout: int = 60,
                    retry_interval: int = 2,
                    silent_interval: int = 10,
                    additional_details: str = "",
                    retried_exceptions:
                        typing.Union[
                            typing.Type[Exception],
                            collections.abc.Iterable[
                                typing.Type[Exception]]] = Exception):
    def wrapper(f: typing.Callable[..., typing.Any]):
        @functools.wraps(f)
        def inner(*args, **kwargs):
            tstart: float = time.time()
            elapsed: float = 0
            exc = None
            details = additional_details or "%s failed" % f.__qualname__

            while elapsed < timeout or not timeout:
                try:
                    return f(*args, **kwargs)
                except retried_exceptions as ex:
                    exc = ex
                    elapsed = time.time() - tstart
                    if elapsed > silent_interval:
                        level = logging.WARNING
                    else:
                        level = logging.DEBUG
                    LOG.log(level,
                            "Exception: %s. Additional details: %s. "
                            "Time elapsed: %d. Timeout: %d",
                            ex, details, elapsed, timeout)

                    time.sleep(retry_interval)
                    elapsed = time.time() - tstart

            msg = (
                "Operation timed out. Exception: %s. Additional details: %s. "
                "Time elapsed: %d. Timeout: %d.")
            raise CephTestTimeout(
                msg % (exc, details, elapsed, timeout))
        return inner
    return wrapper


def execute(*args, **kwargs):
    LOG.debug("Executing: %s", args)
    result = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **kwargs)
    LOG.debug("Command %s returned %d.", args, result.returncode)
    if result.returncode:
        exc = CommandFailed(
            command=args, returncode=result.returncode,
            stdout=result.stdout, stderr=result.stderr)
        LOG.error(exc)
        raise exc
    return result


def ps_execute(*args, **kwargs):
    # Disable PS progress bar, causes issues when invoked remotely.
    prefix = "$global:ProgressPreference = 'SilentlyContinue' ; "
    return execute(
        "powershell.exe", "-NonInteractive",
        "-Command", prefix, *args, **kwargs)


def array_stats(array: list):
    mean = sum(array) / len(array) if len(array) else 0
    variance = (sum((i - mean) ** 2 for i in array) / len(array)
                if len(array) else 0)
    std_dev = math.sqrt(variance)
    sorted_array = sorted(array)

    return {
        'min': min(array) if len(array) else 0,
        'max': max(array) if len(array) else 0,
        'sum': sum(array) if len(array) else 0,
        'mean': mean,
        'median': sorted_array[len(array) // 2] if len(array) else 0,
        'max_90': sorted_array[int(len(array) * 0.9)] if len(array) else 0,
        'min_90': sorted_array[int(len(array) * 0.1)] if len(array) else 0,
        'variance': variance,
        'std_dev': std_dev,
        'count': len(array)
    }


class Tracer:
    data: collections.OrderedDict = collections.OrderedDict()
    lock = threading.Lock()

    @classmethod
    def trace(cls, func):
        def wrapper(*args, **kwargs):
            tstart = time.time()
            exc_str = None

            # Preserve call order
            with cls.lock:
                if func.__qualname__ not in cls.data:
                    cls.data[func.__qualname__] = list()

            try:
                return func(*args, **kwargs)
            except Exception as exc:
                exc_str = str(exc)
                raise
            finally:
                tend = time.time()

                with cls.lock:
                    cls.data[func.__qualname__] += [{
                        "duration": tend - tstart,
                        "error": exc_str,
                    }]

        return wrapper

    @classmethod
    def get_results(cls):
        stats = collections.OrderedDict()
        for f in cls.data.keys():
            stats[f] = array_stats([i['duration'] for i in cls.data[f]])
            errors = []
            for i in cls.data[f]:
                if i['error']:
                    errors.append(i['error'])

            stats[f]['errors'] = errors
        return stats

    @classmethod
    def print_results(cls):
        r = cls.get_results()

        table = prettytable.PrettyTable(title="Duration (s)")
        table.field_names = [
            "function", "min", "max", "total",
            "mean", "median", "std_dev",
            "max 90%", "min 90%", "count", "errors"]
        table.float_format = ".4"
        for f, s in r.items():
            table.add_row([f, s['min'], s['max'], s['sum'],
                           s['mean'], s['median'], s['std_dev'],
                           s['max_90'], s['min_90'],
                           s['count'], len(s['errors'])])
        print(table)


class RbdImage(object):
    def __init__(self,
                 name: str,
                 size_mb: int,
                 is_shared: bool = True,
                 disk_number: int = -1,
                 mapped: bool = False):
        self.name = name
        self.size_mb = size_mb
        self.is_shared = is_shared
        self.disk_number = disk_number
        self.mapped = mapped
        self.removed = False
        self.drive_letter = ""

    @classmethod
    @Tracer.trace
    def create(cls,
               name: str,
               size_mb: int = 1024,
               is_shared: bool = True):
        LOG.info("Creating image: %s. Size: %s.", name, "%sM" % size_mb)
        cmd = ["rbd", "create", name, "--size", "%sM" % size_mb]
        if is_shared:
            cmd += ["--image-shared"]
        execute(*cmd)

        return RbdImage(name, size_mb, is_shared)

    @Tracer.trace
    def get_disk_number(self,
                        timeout: int = 60,
                        retry_interval: int = 2):
        @retry_decorator(
            retried_exceptions=CephTestException,
            timeout=timeout,
            retry_interval=retry_interval)
        def _get_disk_number():
            LOG.info("Retrieving disk number: %s", self.name)

            result = execute("rbd-wnbd", "show", self.name, "--format=json")
            disk_info = json.loads(result.stdout)
            disk_number = disk_info["disk_number"]
            if disk_number > 0:
                LOG.debug("Image %s disk number: %d", self.name, disk_number)
                return disk_number

            raise CephTestException(
                f"Could not get disk number: {self.name}.")

        return _get_disk_number()

    @Tracer.trace
    def _wait_for_disk(self,
                       timeout: int = 60,
                       retry_interval: int = 2):
        @retry_decorator(
            retried_exceptions=(FileNotFoundError, OSError),
            additional_details="the mapped disk isn't available yet",
            timeout=timeout,
            retry_interval=retry_interval)
        def wait_for_disk():
            LOG.debug("Waiting for disk to be accessible: %s %s",
                      self.name, self.path)

            with open(self.path, 'rb'):
                pass

        return wait_for_disk()

    @property
    def path(self):
        return f"\\\\.\\PhysicalDrive{self.disk_number}"

    @Tracer.trace
    @retry_decorator(additional_details="couldn't clear disk read-only flag")
    def set_writable(self):
        ps_execute(
            "Set-Disk", "-Number", str(self.disk_number),
            "-IsReadOnly", "$false")

    @Tracer.trace
    @retry_decorator(additional_details="couldn't bring the disk online")
    def set_online(self):
        ps_execute(
            "Set-Disk", "-Number", str(self.disk_number),
            "-IsOffline", "$false")

    @Tracer.trace
    def map(self, timeout: int = 60):
        LOG.info("Mapping image: %s", self.name)
        tstart = time.time()

        execute("rbd-wnbd", "map", self.name)
        self.mapped = True

        self.disk_number = self.get_disk_number(timeout=timeout)

        elapsed = time.time() - tstart
        self._wait_for_disk(timeout=timeout - elapsed)

    @Tracer.trace
    def unmap(self):
        if self.mapped:
            LOG.info("Unmapping image: %s", self.name)
            execute("rbd-wnbd", "unmap", self.name)
            self.mapped = False

    @Tracer.trace
    def remove(self):
        if not self.removed:
            LOG.info("Removing image: %s", self.name)
            execute("rbd", "rm", self.name)
            self.removed = True

    def cleanup(self):
        try:
            self.unmap()
        finally:
            self.remove()

    @Tracer.trace
    @retry_decorator()
    def _init_disk(self):
        cmd = f"Get-Disk -Number {self.disk_number} | Initialize-Disk"
        ps_execute(cmd)

    @Tracer.trace
    @retry_decorator()
    def _create_partition(self):
        cmd = (f"Get-Disk -Number {self.disk_number} | "
               "New-Partition -AssignDriveLetter -UseMaximumSize")
        ps_execute(cmd)

    @Tracer.trace
    @retry_decorator()
    def _format_volume(self):
        cmd = (
            f"(Get-Partition -DiskNumber {self.disk_number}"
            " | ? { $_.DriveLetter }) | Format-Volume -Force -Confirm:$false")
        ps_execute(cmd)

    @Tracer.trace
    @retry_decorator()
    def _get_drive_letter(self):
        cmd = (f"(Get-Partition -DiskNumber {self.disk_number}"
               " | ? { $_.DriveLetter }).DriveLetter")
        result = ps_execute(cmd)

        # The PowerShell command will place a null character if no drive letter
        # is available. For example, we can receive "\x00\r\n".
        self.drive_letter = result.stdout.decode().strip()
        if not self.drive_letter.isalpha() or len(self.drive_letter) != 1:
            raise CephTestException(
                "Invalid drive letter received: %s" % self.drive_letter)

    @Tracer.trace
    def init_fs(self):
        if not self.mapped:
            raise CephTestException("Unable to create fs, image not mapped.")

        LOG.info("Initializing fs, image: %s.", self.name)

        self._init_disk()
        self._create_partition()
        self._format_volume()
        self._get_drive_letter()

    @Tracer.trace
    def get_fs_capacity(self):
        if not self.drive_letter:
            raise CephTestException("No drive letter available")

        cmd = f"(Get-Volume -DriveLetter {self.drive_letter}).Size"
        result = ps_execute(cmd)

        return int(result.stdout.decode().strip())

    @Tracer.trace
    def resize(self, new_size_mb, allow_shrink=False):
        LOG.info(
            "Resizing image: %s. New size: %s MB, old size: %s MB",
            self.name, new_size_mb, self.size_mb)

        cmd = ["rbd", "resize", self.name,
               "--size", f"{new_size_mb}M", "--no-progress"]
        if allow_shrink:
            cmd.append("--allow-shrink")

        execute(*cmd)

        self.size_mb = new_size_mb

    @Tracer.trace
    def get_disk_size(self):
        """Retrieve the virtual disk size (bytes) reported by Windows."""
        cmd = f"(Get-Disk -Number {self.disk_number}).Size"
        result = ps_execute(cmd)

        disk_size = result.stdout.decode().strip()
        if not disk_size.isdigit():
            raise CephTestException(
                "Invalid disk size received: %s" % disk_size)

        return int(disk_size)

    @Tracer.trace
    @retry_decorator(timeout=30)
    def wait_for_disk_resize(self):
        # After resizing the rbd image, the daemon is expected to receive
        # the notification, inform the WNBD driver and then trigger a disk
        # rescan (IOCTL_DISK_UPDATE_PROPERTIES). This might take a few seconds,
        # so we'll need to do some polling.
        disk_size = self.get_disk_size()
        disk_size_mb = disk_size // (1 << 20)

        if disk_size_mb != self.size_mb:
            raise CephTestException(
                "The disk size hasn't been updated yet. Retrieved size: "
                f"{disk_size_mb}MB. Expected size: {self.size_mb}MB.")


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
        result = execute(*cmd)
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

            s = array_stats([float(i["bw_bytes"]) / 1000_000 for i in op_data])
            table.add_row(["bandwidth (MB/s)",
                           s['min'], s['max'], s['mean'],
                           s['median'], s['std_dev'],
                           s['max_90'], s['min_90'], 'N/A'])

            s = array_stats([float(i["runtime"]) for i in op_data])
            table.add_row(["duration (s)",
                          s['min'], s['max'], s['mean'],
                          s['median'], s['std_dev'],
                          s['max_90'], s['min_90'], s['sum']])

            s = array_stats([i["error"] for i in op_data])
            table.add_row(["errors",
                           s['min'], s['max'], s['mean'],
                           s['median'], s['std_dev'],
                           s['max_90'], s['min_90'], s['sum']])

            s = array_stats([i["short_ios"] for i in op_data])
            table.add_row(["incomplete IOs",
                           s['min'], s['max'], s['mean'],
                           s['median'], s['std_dev'],
                           s['max_90'], s['min_90'], s['sum']])

            s = array_stats([i["dropped_ios"] for i in op_data])
            table.add_row(["dropped IOs",
                           s['min'], s['max'], s['mean'],
                           s['median'], s['std_dev'],
                           s['max_90'], s['min_90'], s['sum']])

            clat_min = array_stats([i["clat_ns_min"] for i in op_data])
            clat_max = array_stats([i["clat_ns_max"] for i in op_data])
            clat_mean = array_stats([i["clat_ns_mean"] for i in op_data])
            clat_stddev = math.sqrt(
                sum([float(i["clat_ns_stddev"]) ** 2 for i in op_data]) / len(op_data)
                if len(op_data) else 0)
            clat_10 = array_stats([i["clat_ns_10"] for i in op_data])
            clat_90 = array_stats([i["clat_ns_90"] for i in op_data])
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

    @staticmethod
    def _rand_float(min_val: float, max_val: float):
        return min_val + (random.random() * max_val - min_val)

    def _get_stamp(self):
        buff = self.image_name.encode()
        padding = 512 - len(buff)
        buff += b'\0' * padding
        return buff

    def _get_stamp_path(self):
        return self.image.path

    @Tracer.trace
    def _write_stamp(self):
        with open(self._get_stamp_path(), self._write_open_mode) as disk:
            stamp = self._get_stamp()
            disk.write(stamp)

    @Tracer.trace
    def _read_stamp(self):
        with open(self._get_stamp_path(), self._read_open_mode) as disk:
            return disk.read(len(self._get_stamp()))

    @Tracer.trace
    def run(self):
        if self._expect_path_exists:
            # Wait up to 5 seconds and then check the disk, ensuring that
            # nobody else wrote to it. This is particularly useful when
            # running a high number of tests in parallel, ensuring that
            # we aren't writing to the wrong disk.
            time.sleep(self._rand_float(0, 5))

            stamp = self._read_stamp()
            assert stamp == b'\0' * len(self._get_stamp())

        self._write_stamp()

        stamp = self._read_stamp()
        assert stamp == self._get_stamp()


class RbdFsStampTest(RbdFsTestMixin, RbdStampTest):
    _write_open_mode = "wb"
    _expect_path_exists = False

    def _get_stamp_path(self):
        return self.get_subpath("test-stamp")


class TestRunner(object):
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
    setup_logging(log_level)

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
        raise CephTestException("Unknown test: {}".format(args.test_name))

    runner = TestRunner(
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
