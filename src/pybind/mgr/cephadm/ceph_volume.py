from cephadm.serve import CephadmServe
from typing import List, TYPE_CHECKING, Any, Dict, Set, Tuple
if TYPE_CHECKING:
    from cephadm import CephadmOrchestrator


class CephVolume:
    def __init__(self, mgr: "CephadmOrchestrator", _inheritance: bool = False) -> None:
        self.mgr: "CephadmOrchestrator" = mgr
        if not _inheritance:
            self.lvm_list: "CephVolumeLvmList" = CephVolumeLvmList(mgr)

    def run_json(self, hostname: str, command: List[str]) -> Dict[str, Any]:
        """Execute a JSON command on the specified hostname and return the result.

        This method wraps the asynchronous execution of a JSON command on the
        specified hostname, waiting for the command to complete. It utilizes the
        `_run_json` method to perform the actual execution.

        Args:
            hostname (str): The hostname of the target node where the JSON command
                            will be executed.
            command (List[str]): A list of command arguments to be passed to the
                                JSON command.

        Returns:
            Dict[str, Any]: A dictionary containing the JSON response from the
                            executed command, which may include various data
                            based on the command executed.
        """
        return self.mgr.wait_async(self._run_json(hostname, command))

    def run(self, hostname: str, command: List[str], **kw: Any) -> Tuple[List[str], List[str], int]:
        """Execute a command on the specified hostname and return the result.

        This method wraps the asynchronous execution of a command on the
        specified hostname, waiting for the command to complete. It utilizes the
        `_run` method to perform the actual execution.

        Args:
            hostname (str): The hostname of the target node where the command
                            will be executed.
            command (List[str]): A list of command arguments to be passed to the
                                command.
            **kw (Any): Additional keyword arguments to customize the command
                        execution.

        Returns:
            Tuple[List[str], List[str], int]: A tuple containing:
                - A list of strings representing the standard output of the command.
                - A list of strings representing the standard error output of the command.
                - An integer representing the return code of the command execution.
        """
        return self.mgr.wait_async(self._run(hostname, command, **kw))

    async def _run(self,
                   hostname: str,
                   command: List[str],
                   **kw: Any) -> Tuple[List[str], List[str], int]:
        """Execute a ceph-volume command on the specified hostname and return the result.

        This asynchronous method constructs a ceph-volume command and then executes
        it on the specified host.
        The result of the command is returned in JSON format.

        Args:
            hostname (str): The hostname of the target node where the command will be executed.
            command (List[str]): A list of command arguments to be passed to the Ceph command.
            **kw (Any): Additional keyword arguments to customize the command execution.

        Returns:
            Tuple[List[str], List[str], int]: A tuple containing:
                - A list of strings representing the standard output of the command.
                - A list of strings representing the standard error output of the command.
                - An integer representing the return code of the command execution.
        """
        cmd: List[str] = ['--']
        cmd.extend(command)
        result = await CephadmServe(self.mgr)._run_cephadm(
            hostname, 'osd', 'ceph-volume',
            cmd,
            **kw)
        return result

    async def _run_json(self,
                        hostname: str,
                        command: List[str]) -> Dict[str, Any]:
        """Execute a ceph-volume command on a specified hostname.

        This asynchronous method constructs a ceph-volume command and then executes
        it on the specified host.
        The result of the command is returned in JSON format.

        Args:
            hostname (str): The hostname of the target node where the command will be executed.
            command (List[str]): A list of command arguments to be passed to the Ceph command.

        Returns:
            Dict[str, Any]: The result of the command execution as a dictionary parsed from
                            the JSON output.
        """
        cmd: List[str] = ['--']
        cmd.extend(command)
        result = await CephadmServe(self.mgr)._run_cephadm_json(
            hostname, 'osd', 'ceph-volume',
            cmd)
        return result

    def clear_replace_header(self, hostname: str, device: str) -> str:
        """Clear the replacement header on a specified device for a given hostname.

        This method checks if a replacement header exists on the specified device
        and clears it if found. After clearing, it invalidates the cached device
        information for the specified hostname and kicks the serve loop.

        Args:
            hostname (str): The hostname of the device on which the replacement header
                            will be cleared. This is used to identify the specific
                            device within the manager's context.
            device (str): The path to the device (e.g., '/dev/sda') from which the
                          replacement header will be cleared.

        Returns:
            str: A message indicating the result of the operation. It will either confirm
                 that the replacement header was cleared or state that no replacement header
                 was detected on the device.
        """
        output: str = ''
        result = self.run(hostname, ['lvm',
                                     'zap',
                                     '--clear-replace-header',
                                     device],
                          error_ok=True)
        out, err, rc = result
        if not rc:
            output = f'Replacement header cleared on {device}'
            self.mgr.cache.invalidate_host_devices(hostname)
            self.mgr._kick_serve_loop()
        else:
            plain_out: str = '\n'.join(out)
            plain_err: str = '\n'.join(err)
            output = f'No replacement header could be cleared on {device}.\n{plain_out}\n{plain_err}'
        return output


class CephVolumeLvmList(CephVolume):
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        super().__init__(mgr, True)
        self.data: Dict[str, Any] = {}

    def get_data(self, hostname: str) -> None:
        """Execute the `ceph-volume lvm list` command to list LVM-based OSDs.

        This asynchronous method interacts with the Ceph manager to retrieve
        information about the Logical Volume Manager (LVM) devices associated
        with the OSDs. It calls the `ceph-volume lvm list` command in JSON format
        to gather relevant data.

        Returns:
            None: This method does not return a value. The retrieved data is
                  stored in the `self.data` attribute for further processing.
        """
        self.data = self.run_json(hostname,
                                  ['lvm', 'list', '--format', 'json'])

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
