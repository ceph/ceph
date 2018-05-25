from __future__ import absolute_import


class BaseDP(object):
    """ basic diskprediction structure """
    _fields = []
    def __init__(self, *args, **kwargs):
        if len(args) > len(self._fields):
            raise TypeError('Expected {} arguments'.format(len(self._fields)))

        for name, value in zip(self._fields, args):
            setattr(self, name, value)

        for name in self._fields[len(args):]:
            setattr(self, name, kwargs.pop(name))

        if kwargs:
            raise TypeError('Invalid argument(s): {}'.format(','.join(kwargs)))


class DPHost(BaseDP):
    _fields = ['agenthost', 'agenthost_domain_id']


class DPSaiDisk(BaseDP):
    _fields = [
        'agenthost', 'agenthost_domain_id', 'cluster_domain_id', 'disk_domain_id',
        'disk_name', 'disk_status', 'disk_type', 'disk_wwn', 'firmware_version',
        'host_domain_id', 'model', 'primary_key', 'sata_version', 'sector_size',
        'serial_number', 'size', 'smart_health_status', 'transport_protocol',
        'vendor'
    ]


class DPSaiDiskSmart(BaseDP):
    _fields = [
        'agenthost', 'agenthost_domain_id', 'cluster_domain_id', 'disk_domain_id',
        'disk_name', 'disk_wwn', 'host_domain_id', 'primary_key', 'row_value',
    ]


class DPLog(BaseDP):
    _fields = [
        'agenthost', 'agenthost_domain_id', 'path', 'detail', 'pid', 'program',
        'timestamp'
    ]


class DPCpu(BaseDP):
    _fields = [
        'agenthost', 'agenthost_domain_id', 'cpu', 'usage_guest', 'usage_guest_nice',
        'usage_idle', 'usage_iowait', 'usage_irq', 'usage_nice', 'usage_softirq',
        'usage_steal', 'usage_system', 'usage_user'
    ]


class DPDiskIO(BaseDP):
    _fields = [
        'agenthost', 'agenthost_domain_id', 'io_time', 'iops_in_progress', 'name',
        'read_time', 'reads', 'weighted_io_time', 'write_bytes', 'write_time',
        'writes'
    ]


class DPMemory(BaseDP):
    _fields = [
        'agenthost', 'agenthost_domain_id', 'available', 'available_percent',
        'buffered', 'cached', 'free', 'inactive', 'slab', 'total', 'used',
        'used_percent'
    ]


class DPNet(BaseDP):
    _fields = [
        'agenthost', 'agenthost_domain_id', 'bytes_recv', 'bytes_send',
        'drop_in', 'drop_out', 'err_in', 'err_out'
    ]
