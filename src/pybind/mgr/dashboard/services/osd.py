# -*- coding: utf-8 -*-
from enum import Enum


class OsdDeploymentOptions(str, Enum):
    COST_CAPACITY = 'cost_capacity'
    THROUGHPUT = 'throughput_optimized'
    IOPS = 'iops_optimized'


class HostStorageSummary:
    def __init__(self, name: str, title=None, desc=None, available=False,
                 capacity=0, used=0, hdd_used=0, ssd_used=0, nvme_used=0):
        self.name = name
        self.title = title
        self.desc = desc
        self.available = available
        self.capacity = capacity
        self.used = used
        self.hdd_used = hdd_used
        self.ssd_used = ssd_used
        self.nvme_used = nvme_used

    def as_dict(self):
        return self.__dict__
