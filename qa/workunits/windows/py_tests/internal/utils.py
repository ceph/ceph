# Copyright (C) 2023 Cloudbase Solutions
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation (see LICENSE).

import collections
import functools
import hashlib
import logging
import math
import os
import subprocess
import time
import typing

from py_tests.internal import exception

LOG = logging.getLogger()


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
            raise exception.CephTestTimeout(
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
        exc = exception.CommandFailed(
            command=args, returncode=result.returncode,
            stdout=result.stdout, stderr=result.stderr)
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


def generate_random_file(path, size, chunk_size=8192):
    if size % chunk_size:
        raise exception.CephTestException(
            f"The file size ({size}) is not a multiple of the "
            f"chunk size ({chunk_size}).")
    with open(path, 'rb+') as f:
        for chunk_idx in range(size // chunk_size):
            f.write(os.urandom(chunk_size))


def checksum(path, algorithm='md5', chunk_size=1048576):
    total_read = 0
    hash_func = getattr(hashlib, algorithm)
    file_hash = hash_func()

    with open(path, "rb") as f:
        try:
            while chunk := f.read(chunk_size):
                file_hash.update(chunk)
                total_read += len(chunk)
        except PermissionError:
            # Windows throws a permission error when reading past
            # the disk boundary.
            if not total_read:
                raise

    return file_hash.hexdigest()


def str2bool(val):
    val = val or ''
    return val.lower() in ['y', 'yes', 'true', 't', '1', 'enabled']
