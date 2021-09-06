"""
This module is responsible for building a metadata cache lookup for every devices on the system, and
may be used as the --with-preload option within the ceph-volume 'inventory' sub-command. The metadata
is gathered using a producer/consume thread model to try and perform as much processing as possible in
parallel to reduce runtime. In addition, the LVM queries are run once, returning all data for all
related objects in place of the LVM calls issued per device within the Device class.

The parallelism via the thread model settled on producer/consumer + queue model for speed (~0.7s). The
inbuilt concurrent.futures module was tested, but ran at nearly double the latency (~1.3s).
"""


import os
import glob
import json
import time
import queue
import logging
import threading

from typing import List, Dict, Any, Optional

from ceph_volume.process import call
from ceph_volume.util.lsmdisk import LSMDisk
from ceph_volume.util.disk import udevadm_property, get_file_contents, blkid
from ceph_volume.api.lvm import Volume, PVolume, VolumeGroup, LV_FIELDS, PV_FIELDS, VG_FIELDS
from ceph_volume.util.constants import DEVID_PROPS

logger = logging.getLogger(__name__)

task_queue = queue.Queue()
shared_data_lock = threading.Lock()


class SharedData:
    def __init__(self):
        self.udev = {}
        self.lsm = {}
        self.blkid = {}
        self.lv = {}
        self.pv = {}
        self.vg = {}


shared_data = SharedData()


def get_lsm(dev_path: str) -> Dict[str, Any]:
    """Use the LSMDisk class to fetch libstoragemgmt details for a device

    Args:
        dev_path (str): Device path e.g. /dev/sdb

    Returns:
        [dict]: JSON representing the libstoragemgmt attributes of the device
    """
    lsm = LSMDisk(dev_path)
    return lsm.json_report()


def run_lvm_cmd(command: List[str]) -> Dict[str, str]:
    """Execute an LVM command, and perform initial parse on output

    Args:
        command ([List]): Command to run represented as a list of strings

    Returns:
        Dict: json output from the lvm command
    """
    out, err, rc = call(command)
    if rc > 0:
        raise

    lvm_data = _js_reformat(out)
    assert 'report' in lvm_data
    return lvm_data


def get_lvm_data(query_type:str) -> Dict[str, Any]:
    """Execute LVM commands and return the objects state as JSON

    Args:
        query_type (str): denotes the type of lvm command to execute (lv, pv or vg)

    Raises:
        ValueError: query_type is invalid

    Returns:
        Dict[str, Any]: LVM object represented as JSON
    """

    lv_cmds = {
        "lv": ['lvs', '-a', '--reportformat=json', '--readonly', '--units=b', '-o', LV_FIELDS],
        "pv": ['pvs', '--reportformat=json', '--readonly', '--units=b', '-o', PV_FIELDS],
        "vg": ['vgs', '--reportformat=json', '--readonly', '--units=b', '-o', VG_FIELDS],
    }
    assert query_type in lv_cmds, "Invalid lvm query"

    lvm_data = run_lvm_cmd(lv_cmds[query_type])
    lvm = {}

    # Post process the LVM output
    for lvm_info in lvm_data['report'][0][query_type]:
        if query_type == 'lv':
            key = lvm_info.get('lv_dm_path').replace('/dev/mapper/', '')
        elif query_type == 'pv':
            key = lvm_info.get('pv_name')
        elif query_type == 'vg':
            key = lvm_info.get('vg_name')
        else:
            raise ValueError("Call to get_lvm_data is invalid, must use lv, pv or vg query_type")

        lvm[key] = lvm_info
    return lvm


def worker():
    """worker function that takes a task from a queue, executes it and updates the shared data object
    """
    active = True
    while active:
        try:
            task = task_queue.get_nowait()
        except queue.Empty:
            time.sleep(0.005)
        else:
            if not task:
                active = False
            else:
                var_name, func, args = task
                res = func(*args)
                if '.' in var_name:
                    assert var_name.count('.') == 1, "variable name passed with > 1 '.' separator"
                    attr, key = var_name.split('.')
                    with shared_data_lock:
                        d = getattr(shared_data, attr)
                        d.update({key: res})
                else:
                    with shared_data_lock:
                        setattr(shared_data, var_name, res)
                task_queue.task_done()


def get_holders(dev_name: str) -> List[str]:
    """Use the holder directory is sysfs to identify child devices

    Args:
        dev_name (str): device id e.g. sdb

    Returns:
        List[str]: List of devices related to the given device
    """
    holders = set()
    dev_root = f"/sys/block/{dev_name}"
    holder_dir_list = [f"{dev_root}/holders"]
    holder_dir_list.extend(glob.glob(f"{dev_root}/**/holders"))
    for holder_dir in holder_dir_list:
        for dm_dev_id in os.listdir(holder_dir):
            dm_name = get_file_contents(f"/sys/block/{dm_dev_id}/dm/name")
            holders.add(dm_name)

    return list(holders)


def _js_reformat(raw_data: List[str]) -> Dict[str, Any]:
    """reformat command output into json

    Args:
        raw_data (List[str]): Each string is a line of output

    Returns:
        Dict[str, Any]: JSON representation of the output
    """
    js_items = []
    for j in raw_data:
        js_items.append(j.strip())

    try:
        js = json.loads(''.join(js_items))
    except json.JSONDecodeError:
        # can't continue
        raise

    return js


class MetadataCache:
    """Create a cache of device metadata

    Uses worker threads as a producer/consumer pool where tasks to run are placed
    on a global queue. The worker threads can then process the task, and update the
    shared_data object

    Raises:
        ValueError: [description]

    Returns:
        [type]: [description]
    """

    # used by disk_api in Device objects
    disk_field_list = [
        'name',
        'kname',
        'pkname',
        'disc-aln',
        'disc-max',
        'disc-zero',
        'fstype',
        'group',
        'label',
        'maj:min',
        'mode',
        'mountpoint',
        'owner',
        'partlabel',
        'state',
        'type',
        'uuid',
    ]

    # used within sys_api in Device objects
    blk_field_list = [
        'disc-gran',
        'hotplug',
        'kname',
        'log-sec',
        'model',
        'name',
        'parttype',
        'phy-sec',
        'pkname',
        'rev',
        'rm',
        'ro',
        'rota',
        'rq-size',
        'sched',
        'size',
        'tran',
        'type',
        'vendor']

    lsm_compatible_devices = ('sd',)
    excluded_devs = ('rbd',)

    def __init__(self, with_lsm: bool = False, worker_pool_size: Optional[int] = None):

        self.disk = {}
        self.lsblk = self.get_device_data()
        dev_paths = [f"/dev/{dev}" for dev in self.lsblk.keys()]
        dev_names = self.lsblk.keys()
        lsm_devs = self.get_lsm_devices(dev_names)

        self.set_worker_pool_size(worker_pool_size, dev_names)

        self.start_workers()

        self._holders = self.get_device_holders(dev_names)
        self.metadata: Dict[str, Any] = {}

        for dev in dev_paths:
            # shared_data.udev[dev] = {}
            task_queue.put((f'udev.{dev}', udevadm_property, (dev, DEVID_PROPS)))
            task_queue.put((f'blkid.{dev}', blkid, (dev,)))

            if with_lsm and dev in lsm_devs:
                # shared_data.lsm[dev] = {}
                task_queue.put((f'lsm[{dev}]', get_lsm, (dev,)))

        # add the lvm based tasks
        task_queue.put(('lv', get_lvm_data, ('lv',)))
        task_queue.put(('pv', get_lvm_data, ('pv',)))
        task_queue.put(('vg', get_lvm_data, ('vg',)))

        task_queue.join()

        self.stop_workers()

        self.assemble_metadata()

    def set_worker_pool_size(self, worker_pool_size: Optional[int], dev_names: List[str]):
        """Set the side of the worker pool using device count and cpu as a guide

        Args:
            worker_pool_size (Optional[int]): requested pool size from the caller
            dev_names (List[str]): List of devices that are on the host
        """
        if not worker_pool_size:
            self.worker_pool_size = os.cpu_count()
        else:
            assert worker_pool_size <= os.cpu_count()
            self.worker_pool_size = worker_pool_size

        logger.info(f"MetadataCache using {self.worker_pool_size} threads")

    def get_device_data(self) -> Dict[str, Any]:
        """Use lsblk to discover devices on the host

        The attributes of the devices will be used in the disk_api consumed by the
        Device class

        Raises:
            OSError: lsblk problems, unable to continue
            ValueError: Returned data looks to be in an incompatible format

        Returns:
            Dict[str, Any]: Dict of devices
        """
        field_list = set()
        field_list.update(self.blk_field_list)
        field_list.update(self.disk_field_list)

         # using the J option to output the blk device hierarchy in json
        lsblk_cmd = ['lsblk', '-bJo', ','.join(field_list)]
        devices = {}
        raw_out, err, rc = call(lsblk_cmd)
        if rc > 0:
            # lsblk cmd failed, can't continue, raise since this is fatal
            raise OSError("OS call to lsblk returned {rc}")

        # call returns the output having passed through splitlines, so we need to re-assemble
        device_json = _js_reformat(raw_out)

        if "blockdevices" not in device_json:
            # can't continue
            raise ValueError("blockdevices section missing from lsblk")

        for dev_data in device_json.get("blockdevices"):

            dev_name = dev_data.get('name')
            if dev_name.startswith(self.excluded_devs):
                continue

            devices[dev_name] = dev_data
        return devices

    def get_lsm_devices(self, dev_name_list: List[str]) -> List[str]:
        """Return a list of device paths that will be compatible with libstoragemgmt

        Args:
            dev_name_list (List[str]): list of device names e.g ['sda','sdb'...]

        Returns:
            List[str]: List of device paths that will work with lsm
        """
        return [f"/dev/{dev_name}" for dev_name in dev_name_list if dev_name.startswith(self.lsm_compatible_devices)]

    def get_device_holders(self, dev_names: List[str]) -> Dict[str, List[str]]:
        """Create a map of devices to their holders (children)

        Args:
            dev_names (List[str]): device name list

        Returns:
            Dict[str, List[str]]: device -> List of child devices (holders)
        """
        return {dev_name: get_holders(dev_name) for dev_name in dev_names}

    def start_workers(self):
        """Create a pool of worker threads
        """
        for t in range(self.worker_pool_size):
            t = threading.Thread(target=worker)
            t.setDaemon = True
            t.start()

    def stop_workers(self):
        """Stop the worker threads by push None items onto the queue
        """
        for t in range(self.worker_pool_size):
            task_queue.put(None)

    def assemble_metadata(self):
        """create a metadata data structure combining all data from lvm, lsblk, udev and blkid
        """
        # use lsblk device name as the key for all metadata
        for dev_name in self.lsblk:
            dev_path = f"/dev/{dev_name}"
            dev_lvs = []
            dev_vgs = []
            dev_pvs = []

            self.disk[dev_name] = {k.upper(): v for k, v in self.lsblk[dev_name].items()}

            dev_list = [dev_path]
            dev_list.extend([f"/dev/{child.get('name')}" for child in self.lsblk[dev_name].get('children', []) if child.get('type', '') == 'part'])
            if dev_name in self._holders:
                dev_lvs = [Volume(**shared_data.lv[lv_name]) for lv_name in self._holders[dev_name]]

            for dev in dev_list:
                if dev in shared_data.pv:
                    dev_pvs.append(PVolume(**shared_data.pv[dev]))
                    vg_name = shared_data.pv[dev].get('vg_name', '')
                    if vg_name:
                        dev_vgs.append(VolumeGroup(**shared_data.vg[vg_name]))

            self.metadata[dev_path] = {
                "disk_api": self.disk.get(dev_name, {}),
                "blkid_api": shared_data.blkid.get(dev_path, {}),
                "lvs": dev_lvs,
                "vgs": dev_vgs,
                "pvs": dev_pvs,
                "udev": shared_data.udev.get(dev_path, {}),
                "lsm": shared_data.lsm.get(dev_path, {}),
            }

    def __str__(self):
        out = {}
        for dev_path in self.metadata:
            dev_metadata = {
                "udev": self.metadata[dev_path]["udev"],
                "blkid_api": self.metadata[dev_path]["blkid_api"],
                "disk_api": self.metadata[dev_path]["disk_api"],
                "lvs": [],
                "pvs": [],
                "vgs": [],
                "lsm": self.metadata[dev_path]["lsm"],
            }

            for lv in self.metadata[dev_path]['lvs']:
                dev_metadata["lvs"].append(lv.as_dict())

            for pv in self.metadata[dev_path]['pvs']:
                dev_metadata["pvs"].append(pv.as_dict())

            for vg in self.metadata[dev_path]['vgs']:
                dev_metadata["vgs"].append(vg.as_dict())


            out[dev_path] = dev_metadata
        return json.dumps(out, indent=2)


if __name__ == "__main__":
    cache = MetadataCache()
    print(cache)
    # print(cache.metadata)
