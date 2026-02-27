from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ceph_node_proxy.util import DEFAULTS, Config

REQUIRED_CEPHADM_KEYS = (
    "target_ip",
    "target_port",
    "keyring",
    "root_cert.pem",
    "listener.crt",
    "listener.key",
    "name",
)


@dataclass
class CephadmConfig:
    """Parsed cephadm bootstrap config (from --config JSON file)"""

    target_ip: str
    target_port: str
    keyring: str
    root_cert_pem: str
    listener_crt: str
    listener_key: str
    name: str
    node_proxy_config_path: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CephadmConfig:
        for key in REQUIRED_CEPHADM_KEYS:
            if key not in data:
                raise ValueError(f"Missing required cephadm config key: {key}")
        # Normalize key with dot to attribute name
        node_proxy_config_path = data.get("node_proxy_config") or os.environ.get(
            "NODE_PROXY_CONFIG", "/etc/ceph/node-proxy.yml"
        )
        assert node_proxy_config_path is not None
        return cls(
            target_ip=data["target_ip"],
            target_port=data["target_port"],
            keyring=data["keyring"],
            root_cert_pem=data["root_cert.pem"],
            listener_crt=data["listener.crt"],
            listener_key=data["listener.key"],
            name=data["name"],
            node_proxy_config_path=node_proxy_config_path,
        )


def load_cephadm_config(path: str) -> CephadmConfig:
    """
    Load and validate cephadm bootstrap config from a JSON file.
    Raises FileNotFoundError if path does not exist, ValueError if invalid.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON config: {e}") from e
    if not isinstance(data, dict):
        raise ValueError("Config must be a JSON object")
    return CephadmConfig.from_dict(data)


def get_node_proxy_config(
    path: Optional[str] = None,
    defaults: Optional[Dict[str, Any]] = None,
) -> Config:
    effective_path = path or os.environ.get(
        "NODE_PROXY_CONFIG", "/etc/ceph/node-proxy.yml"
    )
    assert effective_path is not None
    return Config(effective_path, defaults=defaults or DEFAULTS)
