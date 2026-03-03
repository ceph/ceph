import json
from typing import Any, Dict, List, Union
from urllib.error import HTTPError, URLError

from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem
from ceph_node_proxy.util import get_logger


class RedfishDellSystem(BaseRedfishSystem):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.log = get_logger(__name__)
        self.job_service_endpoint: str = (
            "/redfish/v1/Managers/iDRAC.Embedded.1/Oem/Dell/DellJobService"
        )
        self.create_reboot_job_endpoint: str = (
            f"{self.job_service_endpoint}/Actions/DellJobService.CreateRebootJob"
        )
        self.setup_job_queue_endpoint: str = (
            f"{self.job_service_endpoint}/Actions/DellJobService.SetupJobQueue"
        )

    def device_led_on(self, device: str) -> int:
        data: Dict[str, bool] = {"LocationIndicatorActive": True}
        try:
            result = self.set_device_led(device, data)
        except (HTTPError, KeyError):
            return 0
        return result

    def device_led_off(self, device: str) -> int:
        data: Dict[str, bool] = {"LocationIndicatorActive": False}
        try:
            result = self.set_device_led(device, data)
        except (HTTPError, KeyError):
            return 0
        return result

    def chassis_led_on(self) -> int:
        data: Dict[str, str] = {"IndicatorLED": "Blinking"}
        result = self.set_chassis_led(data)
        return result

    def chassis_led_off(self) -> int:
        data: Dict[str, str] = {"IndicatorLED": "Lit"}
        result = self.set_chassis_led(data)
        return result

    def get_device_led(self, device: str) -> Dict[str, Any]:
        endpoint = self._sys["storage"][device]["redfish_endpoint"]
        try:
            result = self.client.query(method="GET", endpoint=endpoint, timeout=10)
        except HTTPError as e:
            self.log.error(
                f"Couldn't get the ident device LED status for device '{device}': {e}"
            )
            raise
        response_json = json.loads(result[1])
        _result: Dict[str, Any] = {"http_code": result[2]}
        if result[2] == 200:
            _result["LocationIndicatorActive"] = response_json[
                "LocationIndicatorActive"
            ]
        else:
            _result["LocationIndicatorActive"] = None
        return _result

    def set_device_led(self, device: str, data: Dict[str, bool]) -> int:
        try:
            _, _, status = self.client.query(
                data=json.dumps(data),
                method="PATCH",
                endpoint=self._sys["storage"][device]["redfish_endpoint"],
            )
        except (HTTPError, KeyError) as e:
            self.log.error(
                f"Couldn't set the ident device LED for device '{device}': {e}"
            )
            raise
        return status

    def get_chassis_led(self) -> Dict[str, Any]:
        endpoint = list(self.endpoints["chassis"].get_members_endpoints().values())[0]
        try:
            result = self.client.query(method="GET", endpoint=endpoint, timeout=10)
        except HTTPError as e:
            self.log.error(f"Couldn't get the ident chassis LED status: {e}")
            raise
        response_json = json.loads(result[1])
        _result: Dict[str, Any] = {"http_code": result[2]}
        if result[2] == 200:
            _result["LocationIndicatorActive"] = response_json[
                "LocationIndicatorActive"
            ]
        else:
            _result["LocationIndicatorActive"] = None
        return _result

    def set_chassis_led(self, data: Dict[str, str]) -> int:
        # '{"IndicatorLED": "Lit"}'      -> LocationIndicatorActive = false
        # '{"IndicatorLED": "Blinking"}' -> LocationIndicatorActive = true
        try:
            _, _, status = self.client.query(
                data=json.dumps(data),
                method="PATCH",
                endpoint=list(
                    self.endpoints["chassis"].get_members_endpoints().values()
                )[0],
            )
        except HTTPError as e:
            self.log.error(f"Couldn't set the ident chassis LED: {e}")
            raise
        return status

    def shutdown_host(self, force: bool = False) -> int:
        reboot_type: str = (
            "GracefulRebootWithForcedShutdown"
            if force
            else "GracefulRebootWithoutForcedShutdown"
        )

        try:
            job_id: str = self.create_reboot_job(reboot_type)
            status = self.schedule_reboot_job(job_id)
        except (HTTPError, KeyError) as e:
            self.log.error(f"Couldn't create the reboot job: {e}")
            raise
        return status

    def powercycle(self) -> int:
        try:
            job_id: str = self.create_reboot_job("PowerCycle")
            status = self.schedule_reboot_job(job_id)
        except (HTTPError, URLError) as e:
            self.log.error(f"Couldn't perform power cycle: {e}")
            raise
        return status

    def create_reboot_job(self, reboot_type: str) -> str:
        data: Dict[str, str] = dict(RebootJobType=reboot_type)
        try:
            headers, _, _ = self.client.query(
                data=json.dumps(data), endpoint=self.create_reboot_job_endpoint
            )
            job_id: str = headers["Location"].split("/")[-1]
        except (HTTPError, URLError) as e:
            self.log.error(f"Couldn't create the reboot job: {e}")
            raise
        return job_id

    def schedule_reboot_job(self, job_id: str) -> int:
        data: Dict[str, Union[List[str], str]] = dict(
            JobArray=[job_id], StartTimeInterval="TIME_NOW"
        )
        try:
            _, _, status = self.client.query(
                data=json.dumps(data), endpoint=self.setup_job_queue_endpoint
            )
        except (HTTPError, KeyError) as e:
            self.log.error(f"Couldn't schedule the reboot job: {e}")
            raise
        return status
