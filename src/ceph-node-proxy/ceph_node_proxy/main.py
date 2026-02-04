import argparse
import json
import os
import signal
import ssl
import time
from tempfile import _TemporaryFileWrapper
from typing import Any, Dict, Optional
from urllib.error import HTTPError

from ceph_node_proxy.api import NodeProxyApi
from ceph_node_proxy.bootstrap import create_node_proxy_manager
from ceph_node_proxy.config import load_cephadm_config
from ceph_node_proxy.registry import get_system_class
from ceph_node_proxy.reporter import Reporter
from ceph_node_proxy.util import DEFAULTS, Config, get_logger, http_req


class NodeProxyManager:
    def __init__(
        self,
        *,
        mgr_host: str,
        cephx_name: str,
        cephx_secret: str,
        ca_path: str,
        api_ssl_crt: str,
        api_ssl_key: str,
        mgr_agent_port: str,
        config: Optional[Config] = None,
        config_path: Optional[str] = None,
        reporter_scheme: str = "https",
        reporter_endpoint: str = "/node-proxy/data",
    ) -> None:
        self.exc: Optional[Exception] = None
        self.log = get_logger(__name__)
        self.mgr_host = mgr_host
        self.cephx_name = cephx_name
        self.cephx_secret = cephx_secret
        self.ca_path = ca_path
        self.api_ssl_crt = api_ssl_crt
        self.api_ssl_key = api_ssl_key
        self.mgr_agent_port = str(mgr_agent_port)
        self.stop: bool = False
        self.ssl_ctx = ssl.create_default_context()
        self.ssl_ctx.check_hostname = True
        self.ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        self.ssl_ctx.load_verify_locations(self.ca_path)
        self.reporter_scheme = reporter_scheme
        self.reporter_endpoint = reporter_endpoint
        self.cephx = {"cephx": {"name": self.cephx_name, "secret": self.cephx_secret}}
        if config is not None:
            self.config = config
        else:
            path = (
                config_path
                or os.environ.get("NODE_PROXY_CONFIG")
                or "/etc/ceph/node-proxy.yml"
            )
            self.config = Config(path, defaults=DEFAULTS)
        self.username: str = ""
        self.password: str = ""
        self._ca_temp_file: Optional[_TemporaryFileWrapper[Any]] = None

    def run(self) -> None:
        self.init()
        self.loop()

    def init(self) -> None:
        self.init_system()
        self.init_reporter()
        self.init_api()

    def fetch_oob_details(self) -> Dict[str, str]:
        try:
            headers, result, status = http_req(
                hostname=self.mgr_host,
                port=self.mgr_agent_port,
                data=json.dumps(self.cephx),
                endpoint="/node-proxy/oob",
                ssl_ctx=self.ssl_ctx,
            )
        except HTTPError as e:
            msg = f"No out of band tool details could be loaded: {e.code}, {e.reason}"
            self.log.error(msg)
            raise

        result_json = json.loads(result)
        oob_details: Dict[str, str] = {
            "host": result_json["result"]["addr"],
            "username": result_json["result"]["username"],
            "password": result_json["result"]["password"],
            "port": result_json["result"].get("port", "443"),
        }
        return oob_details

    def init_system(self) -> None:
        try:
            oob_details = self.fetch_oob_details()
            self.username = oob_details["username"]
            self.password = oob_details["password"]
        except HTTPError:
            self.log.warning("No oob details could be loaded, exiting...")
            raise SystemExit(1)
        try:
            vendor = self.config.get("system", {}).get("vendor", "generic")
            system_cls = get_system_class(vendor)
            self.system = system_cls(
                host=oob_details["host"],
                port=oob_details["port"],
                username=oob_details["username"],
                password=oob_details["password"],
                config=self.config,
            )
            self.system.start()
        except RuntimeError:
            self.log.error("Can't initialize the redfish system.")
            raise

    def init_reporter(self) -> None:
        try:
            max_retries = self.config.get("reporter", {}).get(
                "push_data_max_retries", 30
            )
            self.reporter_agent = Reporter(
                self.system,
                self.cephx,
                reporter_scheme=self.reporter_scheme,
                reporter_hostname=self.mgr_host,
                reporter_port=self.mgr_agent_port,
                reporter_endpoint=self.reporter_endpoint,
                max_retries=max_retries,
            )
            self.reporter_agent.start()
        except RuntimeError:
            self.log.error("Can't initialize the reporter.")
            raise

    def init_api(self) -> None:
        try:
            self.log.info("Starting node-proxy API...")
            self.api = NodeProxyApi(self)
            self.api.start()
        except Exception as e:
            self.log.error(f"Can't start node-proxy API: {e}")
            raise

    def loop(self) -> None:
        check_interval = 20
        min_interval = 20
        max_interval = 300
        backoff_factor = 1.5
        consecutive_failures = 0
        heartbeat_interval = 300
        last_heartbeat = time.monotonic()

        while not self.stop:
            try:
                for thread in [self.system, self.reporter_agent]:
                    status = thread.check_status()
                    label = "Ok" if status else "Critical"
                    self.log.debug(f"{thread} status: {label}")
                consecutive_failures = 0
                check_interval = min_interval
                self.log.debug(
                    "All threads are alive, next check in %ds.", check_interval
                )
                now = time.monotonic()
                if now - last_heartbeat >= heartbeat_interval:
                    self.log.info(
                        "node-proxy running (heartbeat), next check in %ds.",
                        heartbeat_interval,
                    )
                    last_heartbeat = now
            except Exception as e:
                consecutive_failures += 1
                self.log.error(
                    f"{consecutive_failures} failure(s): thread not running: "
                    f"{e.__class__.__name__}: {e}"
                )
                for thread in [self.system, self.reporter_agent]:
                    thread.shutdown()
                self.init_system()
                self.init_reporter()
                check_interval = min(int(check_interval * backoff_factor), max_interval)
                self.log.info("Next check in %ds (backoff).", check_interval)
            time.sleep(check_interval)

    def shutdown(self) -> None:
        self.stop = True
        # if `self.system.shutdown()` is called before self.start(), it will fail.
        if hasattr(self, "api"):
            self.api.shutdown()
        if hasattr(self, "reporter_agent"):
            self.reporter_agent.shutdown()
        if hasattr(self, "system"):
            self.system.shutdown()


def handler(signum: Any, frame: Any, t_mgr: "NodeProxyManager") -> None:
    if hasattr(t_mgr, "system") and t_mgr.system is not None:
        t_mgr.system.pending_shutdown = True
    t_mgr.log.info("SIGTERM caught, shutting down threads...")
    t_mgr.shutdown()
    if (
        hasattr(t_mgr, "system")
        and t_mgr.system is not None
        and hasattr(t_mgr.system, "client")
        and t_mgr.system.client is not None
    ):
        t_mgr.log.info("Logging out from RedFish API")
        t_mgr.system.client.logout()
    raise SystemExit(0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ceph Node-Proxy for HW Monitoring",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config", help="path of config file in json format", required=True
    )
    parser.add_argument(
        "--debug",
        help="increase logging verbosity (debug level)",
        action="store_true",
    )

    args = parser.parse_args()
    if args.debug:
        DEFAULTS["logging"]["level"] = 10

    try:
        cephadm_config = load_cephadm_config(args.config)
    except FileNotFoundError as e:
        raise SystemExit(f"Config error: {e}")
    except ValueError as e:
        raise SystemExit(f"Config error: {e}")

    node_proxy_mgr = create_node_proxy_manager(cephadm_config)
    signal.signal(
        signal.SIGTERM, lambda signum, frame: handler(signum, frame, node_proxy_mgr)
    )
    node_proxy_mgr.run()


if __name__ == "__main__":
    main()
