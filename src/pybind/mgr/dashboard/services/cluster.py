# -*- coding: utf-8 -*-
from enum import Enum

from .. import mgr


class ClusterModel:

    class Status(Enum):
        INSTALLED = 0
        POST_INSTALLED = 1

    status: Status

    def __init__(self, status=Status.INSTALLED.name):
        self.status = self.Status[status]

    def dict(self):
        return {'status': self.status.name}

    def to_db(self):
        mgr.set_store('cluster/status', self.status.name)

    @classmethod
    def from_db(cls):
        return cls(status=mgr.get_store('cluster/status', cls.Status.INSTALLED.name))
