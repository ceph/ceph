# -*- coding: utf-8 -*-
from enum import Enum

from .. import mgr


class ClusterModel:

    class Status(Enum):
        INSTALLED = 0
        POST_INSTALLED = 1

    status: Status

    def __init__(self, status=Status.POST_INSTALLED.name):
        """
        :param status: The status of the cluster. Assume that the cluster
            is already functional by default.
        :type status: str
        """
        self.status = self.Status[status]

    def dict(self):
        return {'status': self.status.name}

    def to_db(self):
        mgr.set_store('cluster/status', self.status.name)

    @classmethod
    def from_db(cls):
        """
        Get the stored cluster status from the configuration key/value store.
        If the status is not set, assume it is already fully functional.
        """
        return cls(status=mgr.get_store('cluster/status', cls.Status.POST_INSTALLED.name))
