import contextlib
import logging
import json

from io import StringIO
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.exceptions import CommandCrashedError
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
        if remote.is_container:
            continue
        devs = teuthology.get_scratch_devices(remote)
        devs_by_remote[remote] = devs
        base = '/sys/kernel/config/nvmet'
        remote.run(
            args=[
                'grep', '^nvme_loop', '/proc/modules', run.Raw('||'),
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
        provide_hostname = True
        for dev in devs:
            short = dev.split('/')[-1]
            log.info(f'Connecting nvme_loop {remote.shortname}:{dev}...')
            nvme_connect_args=[
                'sudo', 'mkdir', '-p', f'{base}/subsystems/{short}',
                run.Raw('&&'),
                'echo', '1', run.Raw('|'),
                'sudo', 'tee', f'{base}/subsystems/{short}/attr_allow_any_host',
                run.Raw('&&'),
                'sudo', 'mkdir', '-p', f'{base}/subsystems/{short}/namespaces/1',
                run.Raw('&&'),
                'echo', '-n', dev, run.Raw('|'),
                'sudo', 'tee', f'{base}/subsystems/{short}/namespaces/1/device_path',
                run.Raw('&&'),
                'echo', '1', run.Raw('|'),
                'sudo', 'tee', f'{base}/subsystems/{short}/namespaces/1/enable',
                run.Raw('&&'),
                'sudo', 'ln', '-s', f'{base}/subsystems/{short}',
                f'{base}/ports/{port}/subsystems/{short}',
                run.Raw('&&'),
                'sudo', 'nvme', 'connect', '-t', 'loop', '-n', short
            ]
            if provide_hostname:
                nvme_connect_args.extend(['-q', host])
            try:
                remote.run(args=nvme_connect_args)
            except Exception:
                if provide_hostname:
                    provide_hostname = False
                    remote.run(args=['sudo', 'nvme', 'connect', '-t', 'loop', '-n', short])
                else:
                    raise

        # identify nvme_loops devices
        old_scratch_by_remote[remote] = remote.read_file('/scratch_devs')

        # nqn used for each scratch device when connecting it above; used
        # as a fallback identifier if `nvme list -o json` is unreliable
        # (see sysfs fallback below).
        nqns_by_dev = {dev: dev.split('/')[-1] for dev in devs}

        new_devs = []
        json_failures = 0
        # after this many consecutive `nvme list -o json` crashes, stop
        # retrying the (apparently broken) command and fall back to
        # discovering devices directly via sysfs instead.
        max_json_failures = 5

        with contextutil.safe_while(sleep=1, tries=15) as proceed:
            while proceed():
                remote.run(args=['lsblk'], stdout=StringIO())

                if json_failures >= max_json_failures:
                    log.warning(
                        f'nvme list -o json crashed {json_failures} times '
                        'in a row; falling back to sysfs-based discovery'
                    )
                    new_devs = discover_devs_via_sysfs(remote, nqns_by_dev)
                    for dev in new_devs:
                        bluestore_zap(remote, dev)
                    log.info(f'new_devs (sysfs fallback) {new_devs}')
                    assert len(new_devs) <= len(devs)
                    if len(new_devs) == len(devs):
                        break
                    continue

                try:
                    p = remote.run(
                        args=['sudo', 'nvme', 'list', '-o', 'json'],
                        stdout=StringIO(),
                    )
                except CommandCrashedError:
                    json_failures += 1
                    log.warning(
                        f'nvme list -o json command failed '
                        f'({json_failures}/{max_json_failures}), retrying...'
                    )
                    continue

                json_failures = 0
                new_devs = []
                # `nvme list -o json` will return one of the following output:
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
                '''{
                  "Devices":[
                    {
                      "HostNQN":"nqn.2014-08.org.nvmexpress:uuid:00000000-0000-0000-0000-0cc47ada6ba4",
                      "HostID":"898a0e10-da2d-4a42-8017-d9c445089d0c",
                      "Subsystems":[
                        {
                          "Subsystem":"nvme-subsys0",
                          "SubsystemNQN":"nqn.2014.08.org.nvmexpress:80868086CVFT623300LN400BGN  INTEL SSDPEDMD400G4",
                          "Controllers":[
                            {
                              "Controller":"nvme0",
                              "Cntlid":"0",
                              "SerialNumber":"CVFT623300LN400BGN",
                              "ModelNumber":"INTEL SSDPEDMD400G4",
                              "Firmware":"8DV101H0",
                              "Transport":"pcie",
                              "Address":"0000:02:00.0",
                              "Slot":"2",
                              "Namespaces":[
                                {
                                  "NameSpace":"nvme0n1",
                                  "Generic":"ng0n1",
                                  "NSID":1,
                                  "UsedBytes":400088457216,
                                  "MaximumLBA":781422768,
                                  "PhysicalSize":400088457216,
                                  "SectorSize":512
                                }
                              ],
                              "Paths":[
                              ]
                            }
                          ],
                          "Namespaces":[
                          ]
                        }
                      ]
                    }
                  ]
                }
                '''
                '''{
                  "Devices":[
                    {
                      "HostNQN":"nqn.2014-08.org.nvmexpress:uuid:00000000-0000-0000-0000-0cc47ada6ba4",
                      "HostID":"898a0e10-da2d-4a42-8017-d9c445089d0c",
                      "Subsystems":[
                        {
                          "Subsystem":"nvme-subsys0",
                          "SubsystemNQN":"nqn.2014.08.org.nvmexpress:80868086CVFT534400C2400BGN  INTEL SSDPEDMD400G4",
                          "Controllers":[
                            {
                              "Controller":"nvme0",
                              "Cntlid":"0",
                              "SerialNumber":"CVFT534400C2400BGN",
                              "ModelNumber":"INTEL SSDPEDMD400G4",
                              "Firmware":"8DV101H0",
                              "Transport":"pcie",
                              "Address":"0000:02:00.0",
                              "Slot":"2",
                              "Namespaces":[
                                {
                                  "NameSpace":"nvme0n1",
                                  "Generic":"ng0n1",
                                  "NSID":1,
                                  "UsedBytes":400088457216,
                                  "MaximumLBA":781422768,
                                  "PhysicalSize":400088457216,
                                  "SectorSize":512
                                }
                              ],
                              "Paths":[
                              ]
                            }
                          ],
                          "Namespaces":[
                          ]
                        }
                      ]
                    }
                  ]
                }
                '''
                nvme_list = json.loads(p.stdout.getvalue())
                for device in nvme_list['Devices']:
                    try:
                        # first try format 1 / older format
                        dev = device['DevicePath']
                        vendor = device['ModelNumber']
                        if dev.startswith('/dev/') and vendor == 'Linux':
                            new_devs.append(dev)
                            bluestore_zap(remote, dev)
                    except KeyError:
                        for subsystem in device['Subsystems']:
                            # format 2
                            if 'Namespaces' in subsystem and subsystem['Namespaces']:
                                dev = '/dev/' + subsystem['Namespaces'][0]['NameSpace']
                            # try format 3 last
                            else:
                                dev = '/dev/' + subsystem['Controllers'][0]['Namespaces'][0]['NameSpace']
                            # vendor is the same for format 2 and 3
                            vendor = subsystem['Controllers'][0]['ModelNumber']
                            if vendor == 'Linux':
                                new_devs.append(dev)
                                bluestore_zap(remote, dev)
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
            if remote.is_container:
                continue
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


def discover_devs_via_sysfs(remote, nqns_by_dev: dict) -> list:
    """
    Fallback nvme_loop device discovery that does not rely on
    `nvme list -o json`, which has been observed to segfault on some
    kernels (e.g. 6.8.0-* on ubuntu_24.04), exhausting safe_while retries
    before any device discovery could succeed.

    Each nvme_loop subsystem is created (see above) using the scratch
    device's basename as both the subsystem directory name and the NQN
    passed to `nvme connect -n <nqn>`. After connecting, the resulting
    in-kernel NVMe controller exposes that NQN at
    /sys/class/nvme/nvmeX/subsysnqn, and its namespace block device(s)
    live in /sys/class/nvme/nvmeX/nvmeXnY. This walks sysfs directly to
    map each expected NQN back to its /dev/nvmeXnY path, with no
    dependency on nvme-cli's (crash-prone) JSON formatter.

    :param nqns_by_dev: mapping of original scratch device path
        (e.g. '/dev/sdb') -> nqn used to connect it (e.g. 'sdb')
    :returns: list of discovered /dev/nvmeXnY paths, one per nqn found
    """
    out = StringIO()
    remote.run(
        args=[
            'bash', '-c',
            'for d in /sys/class/nvme/nvme*/; do '
            'c=$(basename "$d"); '
            'n=$(cat "${d}subsysnqn" 2>/dev/null); '
            'for ns in "${d}"${c}n*; do '
            '[ -e "$ns" ] && echo "${c}|${n}|$(basename "$ns")"; '
            'done; '
            'done'
        ],
        stdout=out,
        check_status=False,
    )

    nqn_to_dev = {}
    for line in out.getvalue().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            _ctrl, nqn, ns = line.split('|', 2)
        except ValueError:
            continue
        # first namespace seen for a given nqn wins; nvme_loop subsystems
        # here are only ever set up with a single namespace (see above)
        nqn_to_dev.setdefault(nqn, f'/dev/{ns}')

    new_devs = []
    for nqn in nqns_by_dev.values():
        dev = nqn_to_dev.get(nqn)
        if dev:
            new_devs.append(dev)
    return new_devs


def bluestore_zap(remote, device: str) -> None:
    for offset in [0, 1073741824, 10737418240]:
        remote.run(args=['sudo', 'dd',
                         'if=/dev/zero', f'of={device}',
                         f'seek={offset}', 'bs=1',
                         'count=4096'], stdout=StringIO())
        remote.run(args=['sudo', 'hexdump', '-n22',
                         '-C', f'-s{offset}', f'{device}'],
                   stdout=StringIO())