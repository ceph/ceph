from typing import TYPE_CHECKING

from ceph_node_proxy.config import CephadmConfig, get_node_proxy_config
from ceph_node_proxy.util import DEFAULTS, write_tmp_file

if TYPE_CHECKING:
    from ceph_node_proxy.main import NodeProxyManager


def create_node_proxy_manager(cephadm_config: CephadmConfig) -> "NodeProxyManager":
    """
    Build NodeProxyManager from cephadm bootstrap config.
    Creates temporary CA file and loads node-proxy YAML config.
    """
    from ceph_node_proxy.main import NodeProxyManager

    ca_file = write_tmp_file(
        cephadm_config.root_cert_pem,
        prefix_name="cephadm-endpoint-root-cert-",
    )
    config = get_node_proxy_config(
        path=cephadm_config.node_proxy_config_path,
        defaults=DEFAULTS,
    )

    manager = NodeProxyManager(
        mgr_host=cephadm_config.target_ip,
        cephx_name=cephadm_config.name,
        cephx_secret=cephadm_config.keyring,
        mgr_agent_port=cephadm_config.target_port,
        ca_path=ca_file.name,
        api_ssl_crt=cephadm_config.listener_crt,
        api_ssl_key=cephadm_config.listener_key,
        config_path=cephadm_config.node_proxy_config_path,
        config=config,
    )
    # Keep temp file alive for the lifetime of the manager
    manager._ca_temp_file = ca_file
    return manager
