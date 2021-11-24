from typing import Dict
import os
import argparse
from util import ensure_inside_container, ensure_outside_container, run_shell_command, \
    run_cephadm_shell_command, Config

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

    if run_shell_command(f'losetup -l | grep {avail_loop}', expect_error=True):
        run_shell_command(f'sudo losetup -d {avail_loop}')

    if not os.path.exists('./loop-images'):
        os.mkdir('loop-images')

    loop_image = 'loop-images/loop.img'
    if os.path.exists(loop_image):
        os.remove(loop_image)

    run_shell_command(f'sudo dd if=/dev/zero of={loop_image} bs=1G count={size}')
    run_shell_command(f'sudo losetup {avail_loop} {loop_image}')

    vgs = run_shell_command('sudo vgs | grep vg1', expect_error=True)
    if vgs:
        run_shell_command('sudo lvm vgremove -f -y vg1')

    run_shell_command(f'sudo pvcreate {avail_loop}')
    run_shell_command(f'sudo vgcreate vg1 {avail_loop}')
    for i in range(osds):
        run_shell_command('sudo vgchange --refresh')
        run_shell_command(f'sudo lvcreate --size 5G --name lv{i} vg1')

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
def deploy_osd(data: str):
    assert data
    out = run_shell_command(f'cephadm ceph-volume lvm zap {data}')
    out = run_shell_command(f'cephadm ceph-volume --shared_ceph_folder /ceph lvm prepare --data {data} --no-systemd --no-tmpfs')

    osd_data = get_lvm_osd_data(data)

    osd = 'osd.' + osd_data['osd_id']
    run_shell_command(f'cephadm deploy --name {osd}')
class Osd:
    _help = '''
    Deploy osds and create needed block devices with loopback devices:
    Actions:
    - deploy: Deploy an osd given a block device
    - create_loop: Create needed loopback devices and block devices in logical volumes
    for a number of osds.
    '''
    actions = ['deploy', 'create_loop']
    parser = None

    def __init__(self, argv):
        self.argv = argv

    @staticmethod
    def add_parser(subparsers):
        assert not Osd.parser
        Osd.parser = subparsers.add_parser('osd', help=Osd._help)
        parser = Osd.parser
        parser.add_argument('action', choices=Osd.actions)
        parser.add_argument('--data', type=str, help='path to a block device')
        parser.add_argument('--osds', type=int, default=0, help='number of osds')

    @ensure_inside_container
    def deploy(self):
        data = Config.get('data')
        deploy_osd(data)

    @ensure_outside_container
    def create_loop(self):
        osds = Config.get('osds')
        create_loopback_devices(osds)
        print('Successfully added logical volumes in loopback devices')

    def main(self):
        parser = Osd.parser
        args = parser.parse_args(self.argv)
        Config.add_args(vars(args))
        function = getattr(self, args.action)
        function()
    
