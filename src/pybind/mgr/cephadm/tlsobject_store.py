from typing import Any, Dict, Union, List, Tuple, Optional, TYPE_CHECKING, Type
from enum import Enum
import json
import logging

from cephadm.tlsobject_types import TLSObjectProtocol, TLSObjectException


if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


TLSOBJECT_STORE_PREFIX = 'cert_store.'


logger = logging.getLogger(__name__)


class TLSObjectScope(Enum):
    HOST = "host"
    SERVICE = "service"
    GLOBAL = "global"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value


class TLSObjectStore():

    def __init__(self, mgr: 'CephadmOrchestrator',
                 tlsobject_class: Type[TLSObjectProtocol],
                 known_entities: Dict[TLSObjectScope, List[str]]) -> None:

        self.mgr: CephadmOrchestrator = mgr
        self.tlsobject_class = tlsobject_class
        all_known_entities = [item for sublist in known_entities.values() for item in sublist]
        self.known_entities: Dict[str, Any] = {key: {} for key in all_known_entities}
        self.per_service_name_tlsobjects = known_entities[TLSObjectScope.SERVICE]
        self.per_host_tlsobjects = known_entities[TLSObjectScope.HOST]
        self.global_tlsobjects = known_entities[TLSObjectScope.GLOBAL]
        self.store_prefix = f'{TLSOBJECT_STORE_PREFIX}{tlsobject_class.STORAGE_PREFIX}.'

    def determine_tlsobject_target(self, entity: str, target: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if entity in self.per_service_name_tlsobjects:
            return (target, None)
        elif entity in self.per_host_tlsobjects:
            return (None, target)
        else:
            return (None, None)

    def get_tlsobject_scope_and_target(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Tuple[TLSObjectScope, Optional[Any]]:
        if entity in self.per_service_name_tlsobjects:
            return TLSObjectScope.SERVICE, service_name
        elif entity in self.per_host_tlsobjects:
            return TLSObjectScope.HOST, host
        elif entity in self.global_tlsobjects:
            return TLSObjectScope.GLOBAL, None
        else:
            return TLSObjectScope.UNKNOWN, None

    def get_tlsobject(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[TLSObjectProtocol]:
        self._validate_tlsobject_entity(entity, service_name, host)
        scope, target = self.get_tlsobject_scope_and_target(entity, service_name, host)
        if scope == TLSObjectScope.GLOBAL:
            return self.known_entities.get(entity)
        else:
            return self.known_entities.get(entity, {}).get(target)

    def save_tlsobject(self, entity: str, tlsobject: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False) -> None:
        self._validate_tlsobject_entity(entity, service_name, host)
        tlsobject = self.tlsobject_class(tlsobject, user_made)
        scope, target = self.get_tlsobject_scope_and_target(entity, service_name, host)
        j: Union[str, Dict[Any, Any], None] = None
        if scope in (TLSObjectScope.SERVICE, TLSObjectScope.HOST):
            self.known_entities[entity][target] = tlsobject
            j = {
                key: self.tlsobject_class.to_json(self.known_entities[entity][key])
                for key in self.known_entities[entity]
            }
            self.mgr.set_store(self.store_prefix + entity, json.dumps(j))
        elif scope == TLSObjectScope.GLOBAL:
            self.known_entities[entity] = tlsobject
            j = self.tlsobject_class.to_json(tlsobject)
            self.mgr.set_store(self.store_prefix + entity, json.dumps(j))
        else:
            logger.error(f'Trying to save entity {entity} with a not-supported/unknown TLSObjectScope scope {scope.value}')

    def rm_tlsobject(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        """Remove a tlsobjectificate for a specific entity, service, or host."""
        self._validate_tlsobject_entity(entity, service_name, host)
        scope, target = self.get_tlsobject_scope_and_target(entity, service_name, host)
        j: Union[str, Dict[Any, Any], None] = None
        if scope in (TLSObjectScope.SERVICE, TLSObjectScope.HOST):
            if entity in self.known_entities and target in self.known_entities[entity]:
                del self.known_entities[entity][target]
                j = {
                    key: self.tlsobject_class.to_json(self.known_entities[entity][key])
                    for key in self.known_entities[entity]
                }
                self.mgr.set_store(self.store_prefix + entity, json.dumps(j))
        elif scope == TLSObjectScope.GLOBAL:
            self.known_entities[entity] = self.tlsobject_class()
            j = self.tlsobject_class.to_json(self.known_entities[entity])
            self.mgr.set_store(self.store_prefix + entity, json.dumps(j))
        else:
            raise TLSObjectException(f'Attempted to remove {self.tlsobject_class.__name__.lower()} for unknown entity {entity}')

    def _validate_tlsobject_entity(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        cred_type = self.tlsobject_class.__name__.lower()
        if entity not in self.known_entities.keys():
            raise TLSObjectException(f'Attempted to access {cred_type} for unknown entity {entity}')
        if entity in self.per_host_tlsobjects and not host:
            raise TLSObjectException(f'Need host to access {cred_type} for entity {entity}')
        if entity in self.per_service_name_tlsobjects and not service_name:
            raise TLSObjectException(f'Need service name to access {cred_type} for entity {entity}')

    def list_tlsobjects(self) -> List[Tuple[str, Type[TLSObjectProtocol], Optional[str]]]:
        """
        Returns a shallow list of all known TLS objects, including their targets.

        Returns:
            List of tuples: (entity, tlsobject, target)
            - entity: The TLS object entity name.
            - tlsobject: The TLS object itself.
            - target: The associated target (service_name, host, or None for global).
        """
        tlsobjects = []
        for known_entity, value in self.known_entities.items():
            if isinstance(value, dict):  # Handle per-service or per-host TLS objects
                for target, tlsobject in value.items():
                    if tlsobject:
                        tlsobjects.append((known_entity, tlsobject, target))
            elif value:  # Handle Global TLS objects
                tlsobjects.append((known_entity, value, None))

        return tlsobjects

    def load(self) -> None:
        for k, v in self.mgr.get_store_prefix(self.store_prefix).items():
            entity = k[len(self.store_prefix):]
            if entity not in self.known_entities:
                logger.warning(f"TLSObjectStore: Discarding unknown entity '{entity}'")
                continue
            entity_targets = json.loads(v)
            if entity in self.per_service_name_tlsobjects or entity in self.per_host_tlsobjects:
                self.known_entities[entity] = {}
                for target in entity_targets:
                    tlsobject = self.tlsobject_class.from_json(entity_targets[target])
                    if tlsobject:
                        self.known_entities[entity][target] = tlsobject
            elif entity in self.global_tlsobjects:
                tlsobject = self.tlsobject_class.from_json(entity_targets)
                if tlsobject:
                    self.known_entities[entity] = tlsobject
            else:
                logger.error(f"TLSObjectStore: Found a known entity {entity} with unknown scope!")
