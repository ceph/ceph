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


class TLSObjectStore():

    def __init__(self, mgr: 'CephadmOrchestrator',
                 tlsobject_class: Type[TLSObjectProtocol],
                 known_entities: List[str],
                 per_service_name_tlsobjects: List[str],
                 per_host_tlsobjects: List[str],
                 general_cred: List[str],
                 ) -> None:
        self.mgr: CephadmOrchestrator = mgr
        self.tlsobject_class = tlsobject_class
        self.known_entities: Dict[str, Any] = {key: {} for key in known_entities}
        self.per_service_name_tlsobjects = per_service_name_tlsobjects
        self.per_host_tlsobjects = per_host_tlsobjects
        self.general_cred = general_cred
        self.store_prefix = f'{TLSOBJECT_STORE_PREFIX}{tlsobject_class.STORAGE_PREFIX}.'

    def get_tlsobject_scope_and_target(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Tuple[TLSObjectScope, Optional[Any]]:
        if entity in self.per_service_name_tlsobjects:
            return TLSObjectScope.SERVICE, service_name
        elif entity in self.per_host_tlsobjects:
            return TLSObjectScope.HOST, host
        else:
            return TLSObjectScope.GLOBAL, None

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
        if scope in {TLSObjectScope.SERVICE, TLSObjectScope.HOST}:
            self.known_entities[entity][target] = tlsobject
            j = {
                key: self.tlsobject_class.to_json(self.known_entities[entity][key])
                for key in self.known_entities[entity]
            }
        else:
            self.known_entities[entity] = tlsobject
            j = self.tlsobject_class.to_json(tlsobject)

        self.mgr.set_store(self.store_prefix + entity, json.dumps(j))

    def rm_tlsobject(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        """Remove a tlsobjectificate for a specific entity, service, or host."""
        self._validate_tlsobject_entity(entity, service_name, host)
        scope, target = self.get_tlsobject_scope_and_target(entity, service_name, host)
        j: Union[str, Dict[Any, Any], None] = None
        if scope in {TLSObjectScope.SERVICE, TLSObjectScope.HOST}:
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

    def list_tlsobjects(self) -> Dict[str, Union[Type[TLSObjectProtocol], Dict[str, Type[TLSObjectProtocol]]]]:
        return self.known_entities

    def load(self) -> None:
        for k, v in self.mgr.get_store_prefix(self.store_prefix).items():
            entity = k[len(self.store_prefix):]
            self.known_entities[entity] = json.loads(v)
            if entity in self.per_service_name_tlsobjects or entity in self.per_host_tlsobjects:
                for k in self.known_entities[entity]:
                    tlsobject = self.tlsobject_class.from_json(self.known_entities[entity][k])
                    self.known_entities[entity][k] = tlsobject
            else:
                tlsobject = self.tlsobject_class.from_json(self.known_entities[entity])
                self.known_entities[entity] = tlsobject
