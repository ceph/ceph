from typing import Any
from ceph.deployment.service_spec import ServiceSpec


def get_field_from_spec(spec: ServiceSpec, attr: str, default: Any) -> Any:
    try:
        value = getattr(spec, attr)
        return value if value else default
    except AttributeError:
        return default
