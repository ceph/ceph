import argparse
import json
import os
from typing import Dict

from util import (Config, Target, ensure_inside_container,
                  ensure_outside_container, run_cephadm_shell_command,
                  run_shell_command)


def remove_loop_img() -> None:
    loop_image = Config.get('loop_img')
    if os.path.exists(loop_image):
        os.remove(loop_image)

@ensure_outside_container
def create_loopback_devices(osds: int) -> None:
    assert osds
    size = (5 * osds) + 1
    print(f'Using {size}GB of data to store osds')
    avail_loop = run_shell_command('sudo losetup -f')
    base_name = os.path.basename(avail_loop)

    # create loop if we cannot find it
    if not os.path.exists(avail_loop):
        num_loops = int(run_shell_command('lsmod | grep loop | awk \'{print $3}\''))
        num_loops += 1
        run_shell_command(f'mknod {avail_loop} b 7 {num_loops}')

    if os.path.ismount(avail_loop):
        os.umount(avail_loop)

    loop_devices = json.loads(run_shell_command(f'losetup -l -J', expect_error=True))
    for dev in loop_devices['loopdevices']:
        if dev['name'] == avail_loop:
            run_shell_command(f'sudo losetup -d {avail_loop}')

    if not os.path.exists('./loop-images'):
        os.mkdir('loop-images')

    remove_loop_img()

    loop_image = Config.get('loop_img')
    run_shell_command(f'sudo dd if=/dev/zero of={loop_image} bs=1 count=0 seek={size}G')
    run_shell_command(f'sudo losetup {avail_loop} {loop_image}')

    # cleanup last call
    cleanup()

    run_shell_command(f'sudo pvcreate {avail_loop} ')
    run_shell_command(f'sudo vgcreate vg1 {avail_loop}')

    p = int(100 / osds)
    for i in range(osds):
        run_shell_command('sudo vgchange --refresh')
        run_shell_command(f'sudo lvcreate -l {p}%VG --name lv{i} vg1')

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

@ensure_inside_container
def deploy_osd(data: str, hostname: str):
    run_cephadm_shell_command(f'ceph orch daemon add osd "{hostname}:{data}"')

def cleanup() -> None:
    vg = 'vg1'
    pvs = json.loads(run_shell_command('sudo pvs --reportformat json'))
    for pv in pvs['report'][0]['pv']:
        if pv['vg_name'] == vg:
            device = pv['pv_name']
            run_shell_command(f'sudo vgremove -f --yes {vg}')
            run_shell_command(f'sudo losetup -d {device}')
            run_shell_command(f'sudo wipefs -af {device}')
            # FIX: this can fail with excluded filter
            run_shell_command(f'sudo pvremove -f --yes {device}', expect_error=True)
            break

    remove_loop_img()

class Osd(Target):
    _help = '''
    Deploy osds and create needed block devices with loopback devices:
    Actions:
    - deploy: Deploy an osd given a block device
    - create_loop: Create needed loopback devices and block devices in logical volumes
    for a number of osds.
    '''
    actions = ['deploy', 'create_loop']

    def set_args(self):
        self.parser.add_argument('action', choices=Osd.actions)
        self.parser.add_argument('--data', type=str, help='path to a block device')
        self.parser.add_argument('--hostname', type=str, help='host to deploy osd')
        self.parser.add_argument('--osds', type=int, default=0, help='number of osds')
        self.parser.add_argument('--vg', type=str, help='Deploy with all lv from virtual group')

    @ensure_inside_container
    def deploy(self):
        data = Config.get('data')
        hostname = Config.get('hostname')
        vg = Config.get('vg')
        if not hostname:
            # assume this host
            hostname = run_shell_command('hostname')
        if vg:
            # deploy with vg
            lvs = json.loads(run_shell_command('lvs --reportformat json'))
            for lv in lvs['report'][0]['lv']:
                if lv['vg_name'] == vg:
                    deploy_osd(f'{vg}/{lv["lv_name"]}', hostname)
        else:
            deploy_osd(data, hostname)

    @ensure_outside_container
    def create_loop(self):
        osds = Config.get('osds')
        create_loopback_devices(osds)
        print('Successfully added logical volumes in loopback devices')

