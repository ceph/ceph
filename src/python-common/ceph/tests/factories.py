from ceph.deployment.inventory import Device


class InventoryFactory(object):
    def __init__(self):
        self.taken_paths = []

    def _make_path(self, ident='b'):
        return "/dev/{}{}".format(self.prefix, ident)

    def _find_new_path(self):
        cnt = 0
        if len(self.taken_paths) >= 25:
            raise Exception(
                "Double-character disks are not implemented. Maximum amount"
                "of disks reached.")

        while self.path in self.taken_paths:
            ident = chr(ord('b') + cnt)
            self.path = "/dev/{}{}".format(self.prefix, ident)
            cnt += 1

    def assemble(self):
        if self.empty:
            return {}
        self._find_new_path()
        inventory_sample = {
            'available': self.available,
            'lvs': [],
            'path': self.path,
            'rejected_reasons': self.rejected_reason,
            'sys_api': {
                'human_readable_size': self.human_readable_size,
                'locked': 1,
                'model': self.model,
                'nr_requests': '256',
                'partitions':
                {  # partitions are not as relevant for now, todo for later
                    'sda1': {
                        'sectors': '41940992',
                        'sectorsize': 512,
                        'size': self.human_readable_size,
                        'start': '2048'
                    }
                },
                'path': self.path,
                'removable': '0',
                'rev': '',
                'ro': '0',
                'rotational': str(self.rotational),
                'sas_address': '',
                'sas_device_handle': '',
                'scheduler_mode': 'mq-deadline',
                'sectors': 0,
                'sectorsize': '512',
                'size': self.size,
                'support_discard': '',
                'vendor': self.vendor
            }
        }

        if self.available:
            self.taken_paths.append(self.path)
            return inventory_sample
        return {}

    def _init(self, **kwargs):
        self.prefix = 'sd'
        self.path = kwargs.get('path', self._make_path())
        self.human_readable_size = kwargs.get('human_readable_size',
                                              '50.00 GB')
        self.vendor = kwargs.get('vendor', 'samsung')
        self.model = kwargs.get('model', '42-RGB')
        self.available = kwargs.get('available', True)
        self.rejected_reason = kwargs.get('rejected_reason', [''])
        self.rotational = kwargs.get('rotational', '1')
        if not self.available:
            self.rejected_reason = ['locked']
        self.empty = kwargs.get('empty', False)
        self.size = kwargs.get('size', 5368709121)

    def produce(self, pieces=1, **kwargs):
        if kwargs.get('path') and pieces > 1:
            raise Exception("/path/ and /pieces/ are mutually exclusive")
        # Move to custom init to track _taken_paths.
        # class is invoked once in each context.
        # if disks with different properties are being created
        # we'd have to re-init the class and loose track of the
        # taken_paths
        self._init(**kwargs)
        return [self.assemble() for x in range(0, pieces)]


class DeviceFactory(object):
    def __init__(self, device_setup):
        self.device_setup = device_setup
        self.pieces = device_setup.get('pieces', 1)
        self.device_conf = device_setup.get('device_config', {})

    def produce(self):
        return [Device(**self.device_conf) for x in range(0, self.pieces)]
