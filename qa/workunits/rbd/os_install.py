import os
import sys
import shlex
import guestfs
import logging

# Import subprocess32 if using python version < 3,  otherwise use default subprocess
if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def execute_command(cmd_args, wait_time=None, return_output=False):
    try:
        command = shlex.split(cmd_args)
        logging.info('Command:' + ' '.join(command))
        command_output = subprocess.run(cmd_args, shell=True, timeout=wait_time, check=True, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)
        if command_output.returncode == 0:
            if return_output is True:
                return command_output.stdout
            else:
                logging.info('Output: \n' + command_output.stdout)

        else:
            logging.error(command_output.stdout)

    except subprocess.CalledProcessError as e:
        logging.error(e.output + str(e.returncode))
    except subprocess.TimeoutExpired:
        logging.error('Command timed out after {} seconds'.format(wait_time))
    except Exception as e:
        logging.error(e)


def inspect_disk(disk_parameter):
    try:
        # Pass python_return_dict=True to the constructor.
        # It indicates that program wants to receive Python dicts for methods in the API that return hashtables.
        g = guestfs.GuestFS(python_return_dict=True)

        g.add_drive_opts(disk_parameter, readonly=1)
        g.launch()

        print '{0:-^20}'.format('Inspection')
        roots = g.inspect_os()
        if len(roots) == 0:
            logging.error('No operating systems found on the specified disk \nNow exiting...')
            exit(1)

        for root_dev in roots:
            print 'Root device: {}'.format(root_dev)
            print '  Product name: {}'.format(g.inspect_get_product_name(root_dev))
            print '  Version:      {}.{}'.format(g.inspect_get_major_version(root_dev),
                                                 g.inspect_get_minor_version(root_dev))
            print '  Type:         {}'.format(g.inspect_get_type(root_dev))
            print '  Distro:       {}'.format(g.inspect_get_distro(root_dev))

        # Mount up the disks
        # Sort keys by length, shortest first, so that we end up
        # mounting the filesystems in the correct order.
        mps = g.inspect_get_mountpoints(root_dev)

        def compare(a, b):
            return len(a) - len(b)

        for device in sorted(mps.keys(), compare):
            g.mount_ro(mps[device], device)

        # If /etc/issue.net file exists, print up to 3 lines.
        filename = '/etc/issue.net'
        if g.is_file(filename):
            print '{0:-^20}'.format(filename)
            lines = g.head_n(3, filename)
            for line in lines:
                print line

        g.umount_all()

    except RuntimeError as msg:
        logging.error(str(msg))


if __name__ == '__main__':

    pool_name = os.getenv('POOL_NAME')
    image_name = os.getenv('IMAGE_NAME')
    vm_name = os.getenv('VM_NAME')
    memory = os.getenv('MEMORY')
    vcpus = os.getenv('VCPUS')
    size = os.getenv('SIZE')
    iso_location = os.getenv('ISO_LOCATION')
    ks_cfg = os.getenv('KS_CFG')

    if 'ubuntu' in execute_command('lsb_release -is', return_output=True).lower():
        execute_command('ceph osd crush tunables hammer')

    execute_command('ceph osd pool create {} 128 128'.format(pool_name))
    execute_command('rbd create -s {} --image-feature layering {}/{}'.format(size, pool_name, image_name))
    disk = execute_command('rbd map {}/{}'.format(pool_name, image_name), return_output=True).strip()
    execute_command('virt-install --name {} --memory {} --vcpus {} --disk {},size={},bus=virtio \
    --location {} --boot cdrom --network bridge=virbr0 --initrd-inject {} \
    --extra-args "inst.ks=file:/{}"'.format(vm_name, memory, vcpus, disk, size, iso_location, ks_cfg,
                                            ks_cfg.split('/')[-1]), wait_time=1800)
    inspect_disk(disk)
    exit(0)

