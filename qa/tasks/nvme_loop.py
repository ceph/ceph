import contextlib
import logging
import json

from io import StringIO
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run


log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    log.info('Setting up nvme_loop on scratch devices...')
    host = 'hostnqn'
    port = '1'
    devs_by_remote = {}
    old_scratch_by_remote = {}
    for remote, roles in ctx.cluster.remotes.items():
        devs = teuthology.get_scratch_devices(remote)
        devs_by_remote[remote] = devs
        base = '/sys/kernel/config/nvmet'
        remote.run(
            args=[
                'sudo', 'modprobe', 'nvme_loop',
                run.Raw('&&'),
                'sudo', 'mkdir', '-p', f'{base}/hosts/{host}',
                run.Raw('&&'),
                'sudo', 'mkdir', '-p', f'{base}/ports/{port}',
                run.Raw('&&'),
                'echo', 'loop', run.Raw('|'),
                'sudo', 'tee', f'{base}/ports/{port}/addr_trtype',
            ]
        )
        for dev in devs:
            short = dev.split('/')[-1]
            log.info(f'Connecting nvme_loop {remote.shortname}:{dev}...')
            remote.run(
                args=[
                    'sudo', 'mkdir', '-p', f'{base}/subsystems/{short}',
                    run.Raw('&&'),
                    'echo', '1', run.Raw('|'),
                    'sudo', 'tee', f'{base}/subsystems/{short}/attr_allow_any_host',
                    run.Raw('&&'),
                    'sudo', 'mkdir', '-p', f'{base}/subsystems/{short}/namespaces/1',
                    run.Raw('&&'),
                    'echo', dev, run.Raw('|'),
                    'sudo', 'tee', f'{base}/subsystems/{short}/namespaces/1/device_path',
                    run.Raw('&&'),
                    'echo', '1', run.Raw('|'),
                    'sudo', 'tee', f'{base}/subsystems/{short}/namespaces/1/enable',
                    run.Raw('&&'),
                    'sudo', 'ln', '-s', f'{base}/subsystems/{short}',
                    f'{base}/ports/{port}/subsystems/{short}',
                    run.Raw('&&'),
                    'sudo', 'nvme', 'connect', '-t', 'loop', '-n', short, '-q', host,
                ]
            )

        # identify nvme_loops devices
        old_scratch_by_remote[remote] = remote.read_file('/scratch_devs')

        with contextutil.safe_while(sleep=1, tries=15) as proceed:
            while proceed():
                p = remote.run(args=['sudo', 'nvme', 'list', '-o', 'json'], stdout=StringIO())
                new_devs = []
                # `nvme list -o json` will return the following output:
                '''{
                     "Devices" : [
                       {
                         "DevicePath" : "/dev/nvme0n1",
                         "Firmware" : "8DV101H0",
                         "Index" : 0,
                         "ModelNumber" : "INTEL SSDPEDMD400G4",
                         "ProductName" : "Unknown Device",
                         "SerialNumber" : "PHFT620400WB400BGN"
                       },
                       {
                         "DevicePath" : "/dev/nvme1n1",
                         "Firmware" : "5.15.0-1",
                         "Index" : 1,
                         "ModelNumber" : "Linux",
                         "ProductName" : "Unknown Device",
                         "SerialNumber" : "7672ce414766ba44a8e5"
                       }
                     ]
                   }'''
                nvme_list = json.loads(p.stdout.getvalue())
                for device in nvme_list['Devices']:
                    dev = device['DevicePath']
                    vendor = device['ModelNumber']
                    if dev.startswith('/dev/') and vendor == 'Linux':
                        new_devs.append(dev)
                log.info(f'new_devs {new_devs}')
                assert len(new_devs) <= len(devs)
                if len(new_devs) == len(devs):
                    break

        remote.write_file(
            path='/scratch_devs',
            data='\n'.join(new_devs) + '\n',
            sudo=True
        )

    try:
        yield

    finally:
        for remote, devs in devs_by_remote.items():
            for dev in devs:
                short = dev.split('/')[-1]
                log.info(f'Disconnecting nvme_loop {remote.shortname}:{dev}...')
                remote.run(
                    args=[
                        'sudo', 'nvme', 'disconnect', '-n', short
                    ],
                    check_status=False,
                )
            remote.write_file(
                path='/scratch_devs',
                data=old_scratch_by_remote[remote],
                sudo=True
            )
