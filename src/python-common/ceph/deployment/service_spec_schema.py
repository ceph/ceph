import inspect
from typing import Any, Callable, Dict, List, Optional, Type, Union, get_args, get_origin

from ceph.deployment.service_spec import ServiceSpec


def _format_type(annotation: Any) -> str:
    if annotation is inspect.Parameter.empty:
        return 'Any'
    if isinstance(annotation, str):
        return annotation
    origin = get_origin(annotation)
    if origin is Union:
        args = get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and len(args) == 2:
            return f'Optional[{_format_type(non_none[0])}]'
        return f"Union[{', '.join(_format_type(a) for a in args)}]"
    if origin in (list, List):
        args = get_args(annotation)
        if args:
            return f'List[{_format_type(args[0])}]'
        return 'List'
    if origin in (dict, Dict):
        args = get_args(annotation)
        if len(args) == 2:
            return f'Dict[{_format_type(args[0])}, {_format_type(args[1])}]'
        return 'Dict'
    if hasattr(annotation, '__name__'):
        return annotation.__name__
    return str(annotation).replace('typing.', '')


def _default_value(default: Any) -> Any:
    if default is inspect.Parameter.empty:
        return None
    if isinstance(default, (str, int, float, bool)) or default is None:
        return default
    if isinstance(default, (list, dict)):
        return default
    return repr(default)


def _init_param_names(cls: Type[ServiceSpec]) -> List[str]:
    try:
        sig = inspect.signature(cls.__init__)
    except (TypeError, ValueError):
        return []
    return [name for name in sig.parameters if name != 'self']


def _field_metadata(cls: Type[ServiceSpec], param_name: str) -> Dict[str, Any]:
    param = inspect.signature(cls.__init__).parameters[param_name]
    default = param.default
    field: Dict[str, Any] = {
        'type': _format_type(param.annotation),
        'required': default is inspect.Parameter.empty,
    }
    if default is not inspect.Parameter.empty:
        field['default'] = _default_value(default)
    else:
        field['default'] = None
    return field


def _class_summary_description(cls: Type[ServiceSpec]) -> Optional[str]:
    doc = inspect.getdoc(cls)
    if not doc:
        return None
    line = doc.strip().splitlines()[0].strip()
    return line or None


def _service_spec_base_params() -> List[str]:
    return [p for p in _init_param_names(ServiceSpec) if p != 'service_type']


def build_service_type_schema(
        service_type: str,
        daemon_type_resolver: Optional[Callable[[str], List[str]]] = None) -> Dict[str, Any]:
    """Build schema metadata for a single service type."""
    if service_type not in ServiceSpec.KNOWN_SERVICE_TYPES:
        raise ValueError(f'Unknown service type: {service_type}')

    spec_class = ServiceSpec._cls(service_type)
    base_params = set(_service_spec_base_params())
    param_names = _init_param_names(spec_class)

    top_level: Dict[str, Any] = {}
    spec_fields: Dict[str, Any] = {}

    for name in param_names:
        if name == 'service_type':
            continue
        meta = _field_metadata(spec_class, name)
        if name in base_params:
            top_level[name] = meta
        else:
            spec_fields[name] = meta

    if daemon_type_resolver:
        daemon_types = daemon_type_resolver(service_type)
    else:
        daemon_types = [service_type]

    cert_meta = ServiceSpec.REQUIRES_CERTIFICATES.get(service_type)
    schema: Dict[str, Any] = {
        'service_type': service_type,
        'spec_class': spec_class.__name__,
        'requires_service_id': service_type in ServiceSpec.REQUIRES_SERVICE_ID
        or service_type == 'osd',
        'daemon_types': daemon_types,
        'description': _class_summary_description(spec_class),
        'fields': top_level,
    }
    if cert_meta:
        schema['requires_certificates'] = dict(cert_meta)
    if spec_fields:
        schema['spec'] = spec_fields
    return schema


def build_service_spec_schema(
        service_type: Optional[str] = None,
        daemon_type_resolver: Optional[Callable[[str], List[str]]] = None) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """Build schema metadata for one or all service types."""
    if service_type:
        return build_service_type_schema(service_type, daemon_type_resolver)
    return [build_service_type_schema(st, daemon_type_resolver)
            for st in ServiceSpec.KNOWN_SERVICE_TYPES]


def _append_field_lines(lines: List[str], fields: Dict[str, Any], prefix: str) -> None:
    for name, meta in sorted(fields.items()):
        req = 'required' if meta.get('required') else 'optional'
        default = meta.get('default')
        default_str = 'null' if default is None else repr(default)
        lines.append(
            f"{prefix}{name}: type={meta['type']}, {req}, default={default_str}")


def format_service_spec_schema_plain(
        schema: Union[Dict[str, Any], List[Dict[str, Any]]]) -> str:
    """Format schema metadata as human-readable plain text."""
    entries = schema if isinstance(schema, list) else [schema]
    chunks: List[str] = []
    for entry in entries:
        lines = [
            f"service_type: {entry['service_type']}",
            f"spec_class: {entry['spec_class']}",
            f"requires_service_id: {str(entry['requires_service_id']).lower()}",
            f"daemon_types: {entry['daemon_types']}",
        ]
        if entry.get('description'):
            lines.append(f"description: {entry['description']}")
        if entry.get('requires_certificates'):
            lines.append(f"requires_certificates: {entry['requires_certificates']}")

        lines.append('fields:')
        _append_field_lines(lines, entry.get('fields', {}), '  ')

        spec_block = entry.get('spec')
        if spec_block:
            lines.append('spec:')
            _append_field_lines(lines, spec_block, '  ')
        chunks.append('\n'.join(lines))
    return '\n---\n'.join(chunks) + ('\n' if chunks else '')
