import json
import time
from typing import Any, Dict
from urllib.error import HTTPError, URLError

from ceph_node_proxy.protocols import SystemForReporter
from ceph_node_proxy.util import BaseThread, get_logger, http_req

DEFAULT_MAX_RETRIES = 30
RETRY_SLEEP_SEC = 5
HEARTBEAT_INTERVAL_SEC = 300


class Reporter(BaseThread):
    def __init__(
        self,
        system: SystemForReporter,
        cephx: Dict[str, Any],
        reporter_scheme: str = "https",
        reporter_hostname: str = "",
        reporter_port: str = "443",
        reporter_endpoint: str = "/node-proxy/data",
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        super().__init__()
        self.system = system
        self.data: Dict[str, Any] = {}
        self.stop: bool = False
        self.cephx = cephx
        self.data["cephx"] = self.cephx["cephx"]
        self.reporter_scheme: str = reporter_scheme
        self.reporter_hostname: str = reporter_hostname
        self.reporter_port: str = reporter_port
        self.reporter_endpoint: str = reporter_endpoint
        self.max_retries: int = max_retries
        self.log = get_logger(__name__)
        self.reporter_url: str = (
            f"{reporter_scheme}://{reporter_hostname}:"
            f"{reporter_port}{reporter_endpoint}"
        )
        self.log.info(f"Reporter url set to {self.reporter_url}")

    def _send_with_retries(self) -> bool:
        """Send data to mgr. Returns True on success, False after max_retries failures."""
        for attempt in range(1, self.max_retries + 1):
            try:
                self.log.debug(
                    f"sending data to {self.reporter_url} (attempt {attempt}/{self.max_retries})"
                )
                http_req(
                    hostname=self.reporter_hostname,
                    port=self.reporter_port,
                    method="POST",
                    headers={"Content-Type": "application/json"},
                    endpoint=self.reporter_endpoint,
                    scheme=self.reporter_scheme,
                    data=json.dumps(self.data),
                )
                return True
            except (HTTPError, URLError) as e:
                self.log.error(
                    f"The reporter couldn't send data to the mgr (attempt {attempt}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries:
                    time.sleep(RETRY_SLEEP_SEC)
        return False

    def main(self) -> None:
        last_heartbeat = time.monotonic()
        while not self.stop:
            self.log.debug("waiting for a lock in reporter loop.")
            with self.system.lock:
                if not self.system.pending_shutdown:
                    self.log.debug("lock acquired in reporter loop.")
                    if self.system.data_ready:
                        self.log.debug("data ready to be sent to the mgr.")
                        if self.system.get_system() != self.system.previous_data:
                            self.log.info("data has changed since last iteration.")
                            self.data["patch"] = self.system.get_system()
                            if self._send_with_retries():
                                self.system.previous_data = self.system.get_system()
                            else:
                                self.log.error(
                                    f"Failed to send data after {self.max_retries} retries; "
                                    "will retry on next cycle."
                                )
                        else:
                            self.log.debug("no diff, not sending data to the mgr.")
            self.log.debug("lock released in reporter loop.")
            now = time.monotonic()
            if now - last_heartbeat >= HEARTBEAT_INTERVAL_SEC:
                self.log.info(
                    "Reporter running (heartbeat), next check in %ds.",
                    HEARTBEAT_INTERVAL_SEC,
                )
                last_heartbeat = now
            time.sleep(5)
        self.log.debug("exiting reporter loop.")
        raise SystemExit(0)
