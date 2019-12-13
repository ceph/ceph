import logging

from teuthology.config import config

from teuthology.provision.cloud import openstack

log = logging.getLogger(__name__)


supported_drivers = dict(
    openstack=dict(
        provider=openstack.OpenStackProvider,
        provisioner=openstack.OpenStackProvisioner,
    ),
)


def get_types():
    types = list()
    if 'libcloud' in config and 'providers' in config.libcloud:
        types = list(config.libcloud['providers'].keys())
    return types


def get_provider_conf(node_type):
    all_providers = config.libcloud['providers']
    provider_conf = all_providers[node_type]
    return provider_conf


def get_provider(node_type):
    provider_conf = get_provider_conf(node_type)
    driver = provider_conf['driver']
    provider_cls = supported_drivers[driver]['provider']
    return provider_cls(name=node_type, conf=provider_conf)


def get_provisioner(node_type, name, os_type, os_version, conf=None):
    provider = get_provider(node_type)
    provider_conf = get_provider_conf(node_type)
    driver = provider_conf['driver']
    provisioner_cls = supported_drivers[driver]['provisioner']
    return provisioner_cls(
        provider=provider,
        name=name,
        os_type=os_type,
        os_version=os_version,
        conf=conf,
    )
