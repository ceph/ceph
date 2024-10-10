from cephadm.serve import CephadmServe
from typing import List, TYPE_CHECKING, Any, Dict, Set
if TYPE_CHECKING:
    from cephadm import CephadmOrchestrator


class CephVolumeLvmList:
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr: "CephadmOrchestrator" = mgr
        self.data: Dict[str, Any] = {}

    def get_data(self, hostname: str) -> None:
        self.data = self.mgr.wait_async(self.do_run(hostname=hostname))

    async def do_run(self, hostname: str) -> Dict[str, Any]:
        """Execute the `ceph-volume lvm list` command to list LVM-based OSDs.

        This asynchronous method interacts with the Ceph manager to retrieve
        information about the Logical Volume Manager (LVM) devices associated
        with the OSDs. It calls the `ceph-volume lvm list` command in JSON format
        to gather relevant data.

        Returns:
            None: This method does not return a value. The retrieved data is
                  stored in the `self.data` attribute for further processing.

        Raises:
            Exception: Raises an exception if there is an error while executing
                       the command or retrieving data from Ceph.
        """
        result = await CephadmServe(self.mgr)._run_cephadm_json(
            hostname, 'osd', 'ceph-volume',
            [
                '--',
                'lvm', 'list',
                '--format', 'json',
            ])
        return result

    def devices_by_type(self, device_type: str) -> List[str]:
        """Retrieve a list of devices of a specified type across all OSDs.

        This method iterates through all OSDs and collects devices that match
        the specified type (e.g., 'block', 'db', 'wal'). The resulting list
        contains unique device paths.

        Args:
            device_type (str): The type of devices to retrieve. This should
                               be one of the recognized device types such as
                               'block', 'db', or 'wal'.

        Returns:
            List[str]: A list of unique device paths of the specified type
                       found across all OSDs. If no devices of the specified
                       type are found, an empty list is returned.
        """
        result: Set[str] = set()
        for osd in self.osd_ids():
            for lv in self.data.get(osd, []):
                if lv.get('type') == device_type:
                    result.update(lv.get('devices', []))
        return list(result)

    def block_devices(self) -> List[str]:
        """List all block devices used by OSDs.

        This method returns a list of devices that are used as 'block' devices
        for storing the main OSD data.

        Returns:
            List[str]: A list of device paths (strings) that are used as 'block' devices.
        """
        return self.devices_by_type('block')

    def db_devices(self) -> List[str]:
        """List all database (DB) devices used by OSDs.

        This method returns a list of devices that are used as 'db' devices
        for storing the database files associated with OSDs.

        Returns:
            List[str]: A list of device paths (strings) that are used as 'db' devices.
        """
        return self.devices_by_type('db')

    def wal_devices(self) -> List[str]:
        """List all write-ahead log (WAL) devices used by OSDs.

        This method returns a list of devices that are used as 'wal' devices
        for storing write-ahead log data associated with OSDs.

        Returns:
            List[str]: A list of device paths (strings) that are used as 'wal' devices.
        """
        return self.devices_by_type('wal')

    def all_devices(self) -> List[str]:
        """List all devices used by OSDs for 'block', 'db', or 'wal' purposes.

        This method aggregates all devices that are currently used by the OSDs
        in the system for the following device types:
        - 'block' devices: Used to store the OSD's data.
        - 'db' devices: Used for database purposes.
        - 'wal' devices: Used for Write-Ahead Logging.

        The returned list combines devices from all these categories.

        Returns:
            List[str]: A list of device paths (strings) that are used as 'block', 'db', or 'wal' devices.
        """
        return self.block_devices() + self.db_devices() + self.wal_devices()

    def device_osd_mapping(self, device_type: str = '') -> Dict[str, Dict[str, List[str]]]:
        """Create a mapping of devices to their corresponding OSD IDs based on device type.

        This method serves as a 'proxy' function, designed to be called by the *_device_osd_mapping() methods.

        This method iterates over the OSDs and their logical volumes to build a
        dictionary that maps each device of the specified type to the list of
        OSD IDs that use it. The resulting dictionary can be used to determine
        which OSDs share a specific device.

        Args:
            device_type (str): The type of the device to filter by (e.g., 'block', 'db', or 'wal').
                               If an empty string is provided, devices of all types will be included.

        Returns:
            Dict[str, Dict[str, List[str]]]: A dictionary where the keys are device
            names and the values are dictionaries containing a list of OSD IDs
            that use the corresponding device.

        eg:
        ```
            {
                '/dev/vda': {'osd_ids': ['0', '1']},
                '/dev/vdb': {'osd_ids': ['2']}
            }
        ```

        """
        result: Dict[str, Dict[str, List[str]]] = {}
        for osd in self.osd_ids():
            for lv in self.data.get(osd, []):
                if lv.get('type') == device_type or not device_type:
                    for device in lv.get('devices', []):
                        if device not in result:
                            result[device] = {'osd_ids': []}
                        result[device]['osd_ids'].append(osd)
        return result

    def block_device_osd_mapping(self) -> Dict[str, Dict[str, List[str]]]:
        """Get a dictionnary with all block devices and their corresponding
        osd(s) id(s).

        eg:
        ```
        {'/dev/vdb': {'osd_ids': ['0']},
         '/dev/vdc': {'osd_ids': ['1']},
         '/dev/vdf': {'osd_ids': ['2']},
         '/dev/vde': {'osd_ids': ['3', '4']}}
         ```

        Returns:
            Dict[str, Dict[str, List[str]]]: A dict including all block devices with their corresponding
        osd id(s).
        """
        return self.device_osd_mapping('block')

    def db_device_osd_mapping(self) -> Dict[str, Dict[str, List[str]]]:
        """Get a dictionnary with all db devices and their corresponding
        osd(s) id(s).

        eg:
        ```
        {'/dev/vdv': {'osd_ids': ['0', '1', '2', '3']},
         '/dev/vdx': {'osd_ids': ['4']}}
         ```

        Returns:
            Dict[str, Dict[str, List[str]]]: A dict including all db devices with their corresponding
        osd id(s).
        """
        return self.device_osd_mapping('db')

    def wal_device_osd_mapping(self) -> Dict[str, Dict[str, List[str]]]:
        """Get a dictionnary with all wal devices and their corresponding
        osd(s) id(s).

        eg:
        ```
        {'/dev/vdy': {'osd_ids': ['0', '1', '2', '3']},
         '/dev/vdz': {'osd_ids': ['4']}}
         ```

        Returns:
            Dict[str, Dict[str, List[str]]]: A dict including all wal devices with their corresponding
        osd id(s).
        """
        return self.device_osd_mapping('wal')

    def is_shared_device(self, device: str) -> bool:
        """Determines if a device is shared between multiple OSDs.

        This method checks if a given device is shared by multiple OSDs for a specified device type
        (such as 'block', 'db', or 'wal'). If the device is associated with more than one OSD,
        it is considered shared.

        Args:
            device (str): The device path to check (e.g., '/dev/sda').
            device_type (str): The type of the device (e.g., 'block', 'db', 'wal').

        Raises:
            RuntimeError: If the device is not valid or not found in the shared devices mapping.

        Returns:
            bool: True if the device is shared by more than one OSD, False otherwise.
        """
        device_osd_mapping = self.device_osd_mapping()
        if not device or device not in device_osd_mapping:
            raise RuntimeError('Not a valid device path.')
        return len(device_osd_mapping[device]['osd_ids']) > 1

    def is_block_device(self, device: str) -> bool:
        """Check if a specified device is a block device.

        This method checks if the specified device is included in the
        list of block devices used by OSDs.

        Args:
            device (str): The path of the device to check.

        Returns:
            bool: True if the device is a block device,
                  False otherwise.
        """
        return device in self.block_devices()

    def is_db_device(self, device: str) -> bool:
        """Check if a specified device is a DB device.

        This method checks if the specified device is included in the
        list of DB devices used by OSDs.

        Args:
            device (str): The path of the device to check.

        Returns:
            bool: True if the device is a DB device,
                  False otherwise.
        """
        return device in self.db_devices()

    def is_wal_device(self, device: str) -> bool:
        """Check if a specified device is a WAL device.

        This method checks if the specified device is included in the
        list of WAL devices used by OSDs.

        Args:
            device (str): The path of the device to check.

        Returns:
            bool: True if the device is a WAL device,
                  False otherwise.
        """
        return device in self.wal_devices()

    def get_block_devices_from_osd_id(self, osd_id: str) -> List[str]:
        """Retrieve the list of block devices associated with a given OSD ID.

        This method looks up the specified OSD ID in the `data` attribute
        and returns a list of devices that are of type 'block'. If there are
        no devices of type 'block' for the specified OSD ID, an empty list is returned.

        Args:
            osd_id (str): The OSD ID for which to retrieve block devices.

        Returns:
            List[str]: A list of block device paths associated with the
                       specified OSD ID. If no block devices are found,
                       an empty list is returned.
        """
        result: List[str] = []
        for lv in self.data.get(osd_id, []):
            if lv.get('type') == 'block':
                result = lv.get('devices', [])
        return result

    def osd_ids(self) -> List[str]:
        """Retrieve the list of OSD IDs.

        This method returns a list of OSD IDs by extracting the keys
        from the `data` attribute, which is expected to contain
        information about OSDs. If there is no data available, an
        empty list is returned.

        Returns:
            List[str]: A list of OSD IDs. If no data is present,
                       an empty list is returned.
        """
        result: List[str] = []
        if self.data:
            result = list(self.data.keys())
        return result
