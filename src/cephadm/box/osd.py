import json
import os
import time
import re
from typing import Dict

from util import (
    BoxType,
    Config,
    Target,
    ensure_inside_container,
    ensure_outside_container,
    get_orch_hosts,
    run_cephadm_shell_command,
    run_dc_shell_command,
    get_container_engine,
    run_shell_command,
)

DEVICES_FILE="./devices.json"

def remove_loop_img() -> None:
    loop_image = Config.get('loop_img')
    if os.path.exists(loop_image):
        os.remove(loop_image)

def create_loopback_devices(osds: int) -> Dict[int, Dict[str, str]]:
    assert osds
    cleanup_osds()
    osd_devs = dict()

    for i in range(osds):
        img_name = f'osd{i}'
        loop_dev = create_loopback_device(img_name)
        osd_devs[i] = dict(img_name=img_name, device=loop_dev)
    with open(DEVICES_FILE, 'w') as dev_file:
        dev_file.write(json.dumps(osd_devs))
    return osd_devs

def create_loopback_device(img_name, size_gb=5):
    loop_img_dir = Config.get('loop_img_dir')
    run_shell_command(f'mkdir -p {loop_img_dir}')
    loop_img = os.path.join(loop_img_dir, img_name)
    run_shell_command(f'rm -f {loop_img}')
    run_shell_command(f'dd if=/dev/zero of={loop_img} bs=1 count=0 seek={size_gb}G')
    loop_dev = run_shell_command(f'sudo losetup -f')
    if not os.path.exists(loop_dev):
        dev_minor = re.match(r'\/dev\/[^\d]+(\d+)', loop_dev).groups()[0]
        run_shell_command(f'sudo mknod -m777 {loop_dev} b 7 {dev_minor}')
        run_shell_command(f'sudo chown {os.getuid()}:{os.getgid()} {loop_dev}')
    if os.path.ismount(loop_dev):
        os.umount(loop_dev)
    run_shell_command(f'sudo losetup {loop_dev} {loop_img}')
    run_shell_command(f'sudo chown {os.getuid()}:{os.getgid()} {loop_dev}')
    return loop_dev


def get_lvm_osd_data(data: str) -> Dict[str, str]:
    osd_lvm_info = run_cephadm_shell_command(f'ceph-volume lvm list {data}')
    osd_data = {}
    for line in osd_lvm_info.split('\n'):
        line = line.strip()
        if not line:
            continue
        line = line.split()
        if line[0].startswith('===') or line[0].startswith('[block]'):
            continue
        # "block device" key -> "block_device"
        key = '_'.join(line[:-1])
        osd_data[key] = line[-1]
    return osd_data

def load_osd_devices():
    if not os.path.exists(DEVICES_FILE):
        return dict()
    with open(DEVICES_FILE) as dev_file:
        devs = json.loads(dev_file.read())
    return devs


@ensure_inside_container
def deploy_osd(data: str, hostname: str) -> bool:
    out = run_cephadm_shell_command(f'ceph orch daemon add osd {hostname}:{data} raw')
    return 'Created osd(s)' in out


def cleanup_osds() -> None:
    loop_img_dir = Config.get('loop_img_dir')
    osd_devs = load_osd_devices()
    for osd in osd_devs.values():
        device = osd['device']
        if 'loop' in device:
            loop_img = os.path.join(loop_img_dir, osd['img_name'])
            run_shell_command(f'sudo losetup -d {device}', expect_error=True)
            if os.path.exists(loop_img):
                os.remove(loop_img)
    run_shell_command(f'rm -rf {loop_img_dir}')


def deploy_osds(count: int):
    osd_devs = load_osd_devices()
    hosts = get_orch_hosts()
    host_index = 0
    seed = get_container_engine().get_seed()
    v = '-v' if Config.get('verbose') else ''
    for osd in osd_devs.values():
        deployed = False
        while not deployed:
            print(hosts)
            hostname = hosts[host_index]['hostname']
            deployed = run_dc_shell_command(
                f'/cephadm/box/box.py {v} osd deploy --data {osd["device"]} --hostname {hostname}',
                seed
            )
            deployed = 'created osd' in deployed.lower() or 'already created?' in deployed.lower()
            print('Waiting 5 seconds to re-run deploy osd...')
            time.sleep(5)
        host_index = (host_index + 1) % len(hosts)


class Osd(Target):
    _help = """
    Deploy osds and create needed block devices with loopback devices:
    Actions:
    - deploy: Deploy an osd given a block device
    - create_loop: Create needed loopback devices and block devices in logical volumes
    for a number of osds.
    - destroy: Remove all osds and the underlying loopback devices.
    """
    actions = ['deploy', 'create_loop', 'destroy']

    def set_args(self):
        self.parser.add_argument('action', choices=Osd.actions)
        self.parser.add_argument('--data', type=str, help='path to a block device')
        self.parser.add_argument('--hostname', type=str, help='host to deploy osd')
        self.parser.add_argument('--osds', type=int, default=0, help='number of osds')

    def deploy(self):
        data = Config.get('data')
        hostname = Config.get('hostname')
        if not hostname:
            # assume this host
            hostname = run_shell_command('hostname')
        if not data:
            deploy_osds(Config.get('osds'))
        else:
            deploy_osd(data, hostname)

    @ensure_outside_container
    def create_loop(self):
        osds = Config.get('osds')
        create_loopback_devices(int(osds))
        print('Successfully created loopback devices')

    @ensure_outside_container
    def destroy(self):
        cleanup_osds()
