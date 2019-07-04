# -*- coding: utf-8 -*-

from .validate import DriveGroupValidator


class DriveGroupSpecs(object):
    def __init__(self, specs):
        assert isinstance(specs, dict)
        self.specs = specs
        DriveGroupValidator(self).validate()

    @property
    def db_slots(self) -> dict:
        """ Property of db_slots

        db_slots are essentially ratio indicators
        """
        return self.specs.get("db_slots", False)

    @property
    def wal_slots(self) -> dict:
        """ Property of wal_slots

        wal_slots are essentially ratio indicators
        """
        return self.specs.get("wal_slots", False)

    @property
    def encryption(self) -> dict:
        """ Property of encryption

        True/Flase if encryption is enabled
        """
        return self.specs.get("encryption", False)

    @property
    def data_device_attrs(self) -> dict:
        """ Data Device attributes
        """
        return self.specs.get("data_devices", dict())

    @property
    def db_device_attrs(self) -> dict:
        """ Db Device attributes
        """
        return self.specs.get("db_devices", dict())

    @property
    def wal_device_attrs(self) -> dict:
        """ Wal Device attributes
        """
        return self.specs.get("wal_devices", dict())

    @property
    def journal_device_attrs(self) -> dict:
        """ Journal Device attributes
        """
        return self.specs.get("journal_devices", dict())

    @property
    def block_wal_size(self) -> int:
        """ Wal Device attributes
        """
        return self.specs.get("block_wal_size", 0)

    @property
    def block_db_size(self) -> int:
        """ Wal Device attributes
        """
        return self.specs.get("block_db_size", 0)

    @property
    def format(self) -> str:
        """
        On-disk-format - Filestore/Bluestore
        """
        return self.specs.get("format", "bluestore")

    @property
    def journal_size(self) -> int:
        """
        Journal size
        """
        return self.specs.get("journal_size", 0)
