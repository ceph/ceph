import logging
from ceph_volume import terminal
from ceph_volume.util.device import Devices, Device
from .specs import DriveGroupSpecs
from .filter import Filter, FilterGenerator

mlogger = terminal.MultiLogger(__name__)
logger = logging.getLogger(__name__)


class DriveSelection(object):
    def __init__(self, spec_obj):
        self.disks = self._load_disks()
        self.spec = DriveGroupSpecs(spec_obj)

    def _load_disks(self):
        return Devices().devices

    def data_devices(self):
        return self.assign_devices(self.spec.data_device_attrs)

    def wal_devices(self):
        return self.assign_devices(self.spec.wal_device_attrs)

    def db_devices(self):
        return self.assign_devices(self.spec.db_device_attrs)

    def journal_devices(self):
        return self.assign_devices(self.spec.journal_device_attrs)

    @staticmethod
    def _limit_reached(device_filter: dict, len_devices: int,
                       disk_path: str) -> bool:
        """ Check for the <limit> property and apply logic

        If a limit is set in 'device_attrs' we have to stop adding
        disks at some point.

        If limit is set (>0) and len(devices) >= limit

        :param int len_devices: Length of the already populated device set/list
        :param str disk_path: The disk identifier (for logging purposes)
        :return: True/False if the device should be added to the list of devices
        :rtype: bool
        """
        limit = int(device_filter.get('limit', 0))

        if limit > 0 and len_devices >= limit:
            mlogger.info("Refuse to add {} due to limit policy of <{}>".format(
                disk_path, limit))
            return True
        return False

    @staticmethod
    def _has_mandatory_idents(disk: dict) -> bool:
        """ Check for mandatory indentification fields
        """
        if disk.path:
            logger.debug("Found matching disk: {}".format(disk.path))
            return True
        else:
            raise Exception(
                "Disk {} doesn't have a 'path' identifier".format(disk))

    def assign_devices(self, device_filter: dict) -> list:
        """ Assign drives based on used filters

        Do not add disks when:

        1) Filter didn't match
        2) Disk doesn't have a mandatory identification item (path)
        3) The set :limit was reached

        After the disk was added we make sure not to re-assign this disk
        for another defined type[wal/db/journal devices]

        return a sorted(by path) list of devices
        """
        devices: list = list()
        for _filter in FilterGenerator(device_filter):
            if not _filter.is_matchable:
                logger.debug(
                    "Ignoring disk {}. Filter is not matchable".format(
                        disk.path))
                continue

            for disk in self.disks:
                logger.debug("Processing disk {}".format(disk.path))

                # continue criterias
                if not _filter.matcher.compare(disk):
                    logger.debug(
                        "Ignoring disk {}. Filter did not match".format(
                            disk.path))
                    continue

                if not self._has_mandatory_idents(disk):
                    logger.debug(
                        "Ignoring disk {}. Missing mandatory idents".format(
                            disk.path))
                    continue

                # break on this condition.
                if self._limit_reached(device_filter, len(devices), disk.path):
                    logger.debug("Ignoring disk {}. Limit reached".format(
                        disk.path))
                    break

                if disk not in devices:
                    logger.debug('Adding disk {}'.format(disk.path))
                    devices.append(disk)

        # This disk is already taken and must not be re-assigned.
        for taken_device in devices:
            if taken_device in self.disks:
                self.disks.remove(taken_device)

        return sorted([x for x in devices], key=lambda dev: dev.path)
