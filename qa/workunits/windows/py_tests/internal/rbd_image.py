# Copyright (C) 2023 Cloudbase Solutions
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation (see LICENSE).

import json
import logging
import os
import time

from py_tests.internal import exception
from py_tests.internal.tracer import Tracer
from py_tests.internal import utils

LOG = logging.getLogger()


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
        utils.execute(*cmd)

        return RbdImage(name, size_mb, is_shared)

    @Tracer.trace
    def get_disk_number(self,
                        timeout: int = 60,
                        retry_interval: int = 2):
        @utils.retry_decorator(
            retried_exceptions=exception.CephTestException,
            timeout=timeout,
            retry_interval=retry_interval)
        def _get_disk_number():
            LOG.info("Retrieving disk number: %s", self.name)

            result = utils.execute(
                "rbd-wnbd", "show", self.name, "--format=json")
            disk_info = json.loads(result.stdout)
            disk_number = disk_info["disk_number"]
            if disk_number > 0:
                LOG.debug("Image %s disk number: %d", self.name, disk_number)
                return disk_number

            raise exception.CephTestException(
                f"Could not get disk number: {self.name}.")

        return _get_disk_number()

    @Tracer.trace
    def _wait_for_disk(self,
                       timeout: int = 60,
                       retry_interval: int = 2):
        @utils.retry_decorator(
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

    @Tracer.trace
    def _wait_for_fs(self,
                     timeout: int = 60,
                     retry_interval: int = 2):
        @utils.retry_decorator(
            retried_exceptions=exception.CephTestException,
            additional_details="the mapped fs isn't available yet",
            timeout=timeout,
            retry_interval=retry_interval)
        def wait_for_fs():
            drive_letter = self._get_drive_letter()
            path = f"{drive_letter}:\\"

            LOG.debug("Waiting for disk to be accessible: %s %s",
                      self.name, self.path)

            if not os.path.exists(path):
                raise exception.CephTestException(
                    f"path not available yet: {path}")

        return wait_for_fs()

    @property
    def path(self):
        return f"\\\\.\\PhysicalDrive{self.disk_number}"

    @Tracer.trace
    @utils.retry_decorator(
        additional_details="couldn't clear disk read-only flag")
    def set_writable(self):
        utils.ps_execute(
            "Set-Disk", "-Number", str(self.disk_number),
            "-IsReadOnly", "$false")

    @Tracer.trace
    @utils.retry_decorator(additional_details="couldn't bring the disk online")
    def set_online(self):
        utils.ps_execute(
            "Set-Disk", "-Number", str(self.disk_number),
            "-IsOffline", "$false")

    @Tracer.trace
    def map(self, timeout: int = 60):
        LOG.info("Mapping image: %s", self.name)
        tstart = time.time()

        utils.execute("rbd-wnbd", "map", self.name)
        self.mapped = True

        self.disk_number = self.get_disk_number(timeout=timeout)

        elapsed = time.time() - tstart
        self._wait_for_disk(timeout=timeout - elapsed)

    @Tracer.trace
    def refresh_after_remap(self, timeout: int = 60):
        tstart = time.time()

        # The disk number may change after a remap, we need to refresh it.
        self.disk_number = self.get_disk_number(timeout=timeout)

        elapsed = time.time() - tstart
        self._wait_for_disk(timeout=timeout - elapsed)

        if self.drive_letter:
            elapsed = time.time() - tstart
            self._wait_for_fs(timeout=timeout - elapsed)

            drive_letter = self._get_drive_letter()

            # We expect the drive letter to remain the same after a remap.
            assert self.drive_letter == drive_letter

    @Tracer.trace
    def unmap(self):
        if self.mapped:
            LOG.info("Unmapping image: %s", self.name)
            utils.execute("rbd-wnbd", "unmap", self.name)
            self.mapped = False

    @Tracer.trace
    @utils.retry_decorator()
    def remove(self):
        if not self.removed:
            LOG.info("Removing image: %s", self.name)
            utils.execute("rbd", "rm", self.name)
            self.removed = True

    def cleanup(self):
        try:
            self.unmap()
        finally:
            self.remove()

    @Tracer.trace
    @utils.retry_decorator()
    def _init_disk(self):
        cmd = (f"Get-Disk -Number {self.disk_number} | "
               "Initialize-Disk -PartitionStyle MBR")
        utils.ps_execute(cmd)

    @Tracer.trace
    @utils.retry_decorator()
    def _create_partition(self):
        cmd = (f"Get-Disk -Number {self.disk_number} | "
               "New-Partition -AssignDriveLetter -UseMaximumSize")
        utils.ps_execute(cmd)

    @Tracer.trace
    @utils.retry_decorator()
    def _format_volume(self):
        cmd = (
            f"(Get-Partition -DiskNumber {self.disk_number}"
            " | ? { $_.DriveLetter }) | Format-Volume -Force -Confirm:$false")
        utils.ps_execute(cmd)

    @Tracer.trace
    @utils.retry_decorator()
    def _get_drive_letter(self):
        cmd = (f"(Get-Partition -DiskNumber {self.disk_number}"
               " | ? { $_.DriveLetter }).DriveLetter")
        result = utils.ps_execute(cmd)

        # The PowerShell command will place a null character if no drive letter
        # is available. For example, we can receive "\x00\r\n".
        drive_letter = result.stdout.decode().strip()
        if not drive_letter.isalpha() or len(drive_letter) != 1:
            raise exception.CephTestException(
                "Invalid drive letter received: %s" % drive_letter)
        return drive_letter

    @Tracer.trace
    def init_fs(self):
        if not self.mapped:
            raise exception.CephTestException(
                "Unable to create fs, image not mapped.")

        LOG.info("Initializing fs, image: %s.", self.name)

        self._init_disk()
        self._create_partition()
        self._format_volume()
        self.drive_letter = self._get_drive_letter()

    @Tracer.trace
    def get_fs_capacity(self):
        if not self.drive_letter:
            raise exception.CephTestException("No drive letter available")

        cmd = f"(Get-Volume -DriveLetter {self.drive_letter}).Size"
        result = utils.ps_execute(cmd)

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

        utils.execute(*cmd)

        self.size_mb = new_size_mb

    @Tracer.trace
    def get_disk_size(self):
        """Retrieve the virtual disk size (bytes) reported by Windows."""
        cmd = f"(Get-Disk -Number {self.disk_number}).Size"
        result = utils.ps_execute(cmd)

        disk_size = result.stdout.decode().strip()
        if not disk_size.isdigit():
            raise exception.CephTestException(
                "Invalid disk size received: %s" % disk_size)

        return int(disk_size)

    @Tracer.trace
    @utils.retry_decorator(timeout=30)
    def wait_for_disk_resize(self):
        # After resizing the rbd image, the daemon is expected to receive
        # the notification, inform the WNBD driver and then trigger a disk
        # rescan (IOCTL_DISK_UPDATE_PROPERTIES). This might take a few seconds,
        # so we'll need to do some polling.
        disk_size = self.get_disk_size()
        disk_size_mb = disk_size // (1 << 20)

        if disk_size_mb != self.size_mb:
            raise exception.CephTestException(
                "The disk size hasn't been updated yet. Retrieved size: "
                f"{disk_size_mb}MB. Expected size: {self.size_mb}MB.")
