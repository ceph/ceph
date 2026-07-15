import json
import tempfile
from pathlib import Path

from ceph_node_proxy.config import CephadmConfig, get_node_proxy_config, load_cephadm_config
from ceph_node_proxy.util import DEFAULTS, _deep_merge


def test_cephadm_config_parses_system_vendor() -> None:
    data = {
        "target_ip": "10.0.0.1",
        "target_port": "8443",
        "keyring": "secret",
        "root_cert.pem": "ca",
        "listener.crt": "crt",
        "listener.key": "key",
        "name": "node-proxy.host01",
        "system": {"vendor": "atollon"},
    }
    config = CephadmConfig.from_dict(data)
    assert config.system == {"vendor": "atollon"}


def test_load_cephadm_config_from_file() -> None:
    payload = {
        "target_ip": "10.0.0.1",
        "target_port": "8443",
        "keyring": "secret",
        "root_cert.pem": "ca",
        "listener.crt": "crt",
        "listener.key": "key",
        "name": "node-proxy.host01",
        "system": {"vendor": "atollon"},
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "node-proxy.json"
        path.write_text(json.dumps(payload))
        config = load_cephadm_config(str(path))
        assert config.system["vendor"] == "atollon"


def test_cephadm_system_vendor_merged_into_node_proxy_config() -> None:
    cephadm_config = CephadmConfig.from_dict(
        {
            "target_ip": "10.0.0.1",
            "target_port": "8443",
            "keyring": "secret",
            "root_cert.pem": "ca",
            "listener.crt": "crt",
            "listener.key": "key",
            "name": "node-proxy.host01",
            "system": {"vendor": "atollon"},
        }
    )
    defaults = _deep_merge(DEFAULTS, {"system": cephadm_config.system})
    with tempfile.TemporaryDirectory() as tmpdir:
        config = get_node_proxy_config(
            path=str(Path(tmpdir) / "missing.yml"),
            defaults=defaults,
        )
        assert config.get("system", {}).get("vendor") == "atollon"
