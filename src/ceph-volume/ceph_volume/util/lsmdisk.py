""" 
This module handles the interaction with libstoragemgmt for local disk 
devices. Interaction may fail with LSM for a number of issues, but the
intent here is to make this a soft fail, since LSM related data is not
a critical component of ceph-volume.
"""
import logging

try:
    from lsm import LocalDisk, LsmError
    from lsm import Disk as lsm_Disk
except ImportError:
    lsm_available = False
    transport_map = {}
    health_map = {}
else:
    lsm_available = True
    transport_map = {
            lsm_Disk.LINK_TYPE_UNKNOWN: "Unavailable",
            lsm_Disk.LINK_TYPE_FC: "Fibre Channel",
            lsm_Disk.LINK_TYPE_SSA: "IBM SSA",
            lsm_Disk.LINK_TYPE_SBP: "Serial Bus",
            lsm_Disk.LINK_TYPE_SRP: "SCSI RDMA",
            lsm_Disk.LINK_TYPE_ISCSI: "iSCSI",
            lsm_Disk.LINK_TYPE_SAS: "SAS",
            lsm_Disk.LINK_TYPE_ADT: "ADT (Tape)",
            lsm_Disk.LINK_TYPE_ATA: "ATA/SATA",
            lsm_Disk.LINK_TYPE_USB: "USB",
            lsm_Disk.LINK_TYPE_SOP: "SCSI over PCI-E",
            lsm_Disk.LINK_TYPE_PCIE: "PCI-E",
    }
    health_map = {
        lsm_Disk.HEALTH_STATUS_UNKNOWN: "Unknown",
        lsm_Disk.HEALTH_STATUS_FAIL: "Fail",
        lsm_Disk.HEALTH_STATUS_WARN: "Warn",
        lsm_Disk.HEALTH_STATUS_GOOD: "Good",
    }

logger = logging.getLogger(__name__)


class LSMDisk:
    def __init__(self, dev_path):
        self.dev_path = dev_path
        self.error_list = set()

        if lsm_available:
            self.lsm_available = True
            self.disk = LocalDisk()
        else:
            self.lsm_available = False
            self.error_list.add("libstoragemgmt (lsm module) is unavailable")
            logger.info("LSM information is unavailable: libstoragemgmt is not installed")
            self.disk = None

        self.led_bits = None

    @property
    def errors(self):
        """show any errors that the LSM interaction has encountered (str)"""
        return ", ".join(self.error_list)

    def _query_lsm(self, func, path):
        """Common method used to call the LSM functions, returning the function's result or None"""

        # if disk is None, lsm is unavailable so all calls should return None
        if self.disk is None:
            return None
        
        method = getattr(self.disk, func)
        try:
            output = method(path)
        except LsmError as err:
            logger.error("LSM Error: {}".format(err._msg))
            self.error_list.add(err._msg)
            return None
        else:
            return output

    @property
    def led_status(self):
        """Fetch LED status, store in the LSMDisk object and return current status (int)"""
        if self.led_bits is None:
            self.led_bits = self._query_lsm('led_status_get', self.dev_path) or 1
            return self.led_bits
        else:
            return self.led_bits

    @property
    def led_ident_state(self):
        """Query a disks IDENT LED state to discover when it is On, Off or Unknown (str)"""
        if self.led_status == 1:
            return "Unsupported"
        if self.led_status & lsm_Disk.LED_STATUS_IDENT_ON == lsm_Disk.LED_STATUS_IDENT_ON:
            return "On"
        elif self.led_status & lsm_Disk.LED_STATUS_IDENT_OFF == lsm_Disk.LED_STATUS_IDENT_OFF:
            return "Off"
        elif self.led_status & lsm_Disk.LED_STATUS_IDENT_UNKNOWN == lsm_Disk.LED_STATUS_IDENT_UNKNOWN:
            return "Unknown"
        
        return "Unsupported"

    @property
    def led_fault_state(self):
        """Query a disks FAULT LED state to discover when it is On, Off or Unknown (str)"""
        if self.led_status == 1:
            return "Unsupported"
        if self.led_status & lsm_Disk.LED_STATUS_FAULT_ON == lsm_Disk.LED_STATUS_FAULT_ON:
            return "On"
        elif self.led_status & lsm_Disk.LED_STATUS_FAULT_OFF == lsm_Disk.LED_STATUS_FAULT_OFF:
            return "Off"
        elif self.led_status & lsm_Disk.LED_STATUS_FAULT_UNKNOWN == lsm_Disk.LED_STATUS_FAULT_UNKNOWN:
            return "Unknown"
        
        return "Unsupported"

    @property
    def led_ident_support(self):
        """Query the LED state to determine IDENT support: Unknown, Supported, Unsupported (str)"""
        if self.led_status == 1:
            return "Unknown"

        ident_states = (
            lsm_Disk.LED_STATUS_IDENT_ON + 
            lsm_Disk.LED_STATUS_IDENT_OFF + 
            lsm_Disk.LED_STATUS_IDENT_UNKNOWN
        )

        if (self.led_status & ident_states) == 0:
            return "Unsupported"
        
        return "Supported"

    @property
    def led_fault_support(self):
        """Query the LED state to determine FAULT support: Unknown, Supported, Unsupported (str)"""
        if self.led_status == 1:
            return "Unknown"

        fail_states = (
            lsm_Disk.LED_STATUS_FAULT_ON + 
            lsm_Disk.LED_STATUS_FAULT_OFF + 
            lsm_Disk.LED_STATUS_FAULT_UNKNOWN
        )

        if self.led_status & fail_states == 0:
            return "Unsupported"

        return "Supported"

    @property
    def health(self):
        """Determine the health of the disk from LSM : Unknown, Fail, Warn or Good (str)"""
        _health_int = self._query_lsm('health_status_get', self.dev_path)
        return health_map.get(_health_int, "Unknown")

    @property
    def transport(self):
        """Translate a disks link type to a human readable format (str)"""
        _link_type = self._query_lsm('link_type_get', self.dev_path)
        return transport_map.get(_link_type, "Unknown")


    @property
    def media_type(self):
        """Use the rpm value to determine the type of disk media: Flash or HDD (str)"""
        _rpm = self._query_lsm('rpm_get', self.dev_path)
        if _rpm is not None:
            if _rpm == 0:
                return "Flash"
            elif _rpm > 1:
                return "HDD"

        return "Unknown"

    def json_report(self):
        """Return the LSM related metadata for the current local disk (dict)"""
        if self.lsm_available:
            return {
                "serialNum": self._query_lsm('serial_num_get', self.dev_path) or "Unknown",
                "transport": self.transport,
                "mediaType": self.media_type,
                "rpm": self._query_lsm('rpm_get', self.dev_path) or "Unknown",
                "linkSpeed": self._query_lsm('link_speed_get', self.dev_path) or "Unknown",
                "health": self.health,
                "ledSupport": {
                    "IDENTsupport": self.led_ident_support,
                    "IDENTstatus": self.led_ident_state,
                    "FAILsupport": self.led_fault_support,
                    "FAILstatus": self.led_fault_state,
                },
                "errors": list(self.error_list)
            }
        else:
            return {}
