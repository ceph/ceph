
from typing import Any, Dict, Union, List, Tuple, Optional, TYPE_CHECKING, Type
from enum import Enum
import json
import logging

from cephadm.credential_types import CredentialProtocol, CredentialException


if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator



CERT_STORE_STORAGE_PREFIX = 'cert_store.'


logger = logging.getLogger(__name__)


class CredentialScope(Enum):
    HOST = "host"
    SERVICE = "service"
    GLOBAL = "global"


class CredentialStore():

    def __init__(self, mgr: 'CephadmOrchestrator',
                 cred_class: Type[CredentialProtocol],
                 known_entities: List[str],
                 per_service_name_credentials: List[str],
                 per_host_credentials: List[str],
                 general_cred: List[str],
                 ) -> None:
        self.mgr: CephadmOrchestrator = mgr
        self.cred_class = cred_class
        self.known_entities: Dict[str, Any] = {key: {} for key in known_entities}
        self.per_service_name_credentials = per_service_name_credentials
        self.per_host_credentials = per_host_credentials
        self.general_cred = general_cred
        self.store_prefix = f'{CERT_STORE_STORAGE_PREFIX}{cred_class.STORAGE_PREFIX}.'

    def get_credential_scope_and_target(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Tuple[CredentialScope, Optional[Any]]:
        if entity in self.per_service_name_credentials:
            return CredentialScope.SERVICE, service_name
        elif entity in self.per_host_credentials:
            return CredentialScope.HOST, host
        else:
            return CredentialScope.GLOBAL, None

    def get_credential(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[CredentialProtocol]:
        self._validate_credential_entity(entity, service_name, host)
        scope, target = self.get_credential_scope_and_target(entity, service_name, host)
        if scope == CredentialScope.GLOBAL:
            return self.known_entities.get(entity)
        else:
            return self.known_entities.get(entity, {}).get(target)

    def save_credential(self, entity: str, credential: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False) -> None:
        self._validate_credential_entity(entity, service_name, host)
        credential_obj = self.cred_class(credential, user_made)
        scope, target = self.get_credential_scope_and_target(entity, service_name, host)
        j: Union[str, Dict[Any, Any], None] = None
        if scope in {CredentialScope.SERVICE, CredentialScope.HOST}:
            self.known_entities[entity][target] = credential_obj
            j = {
                key: self.cred_class.to_json(self.known_entities[entity][key])
                for key in self.known_entities[entity]
            }
        else:
            self.known_entities[entity] = credential_obj
            j = self.cred_class.to_json(credential_obj)

        self.mgr.set_store(self.store_prefix + entity, json.dumps(j))

    def rm_credential(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        """Remove a credentialificate for a specific entity, service, or host."""
        self._validate_credential_entity(entity, service_name, host)
        scope, target = self.get_credential_scope_and_target(entity, service_name, host)
        j: Union[str, Dict[Any, Any], None] = None
        if scope in {CredentialScope.SERVICE, CredentialScope.HOST}:
            if entity in self.known_entities and target in self.known_entities[entity]:
                del self.known_entities[entity][target]
                j = {
                    key: self.cred_class.to_json(self.known_entities[entity][key])
                    for key in self.known_entities[entity]
                }
                self.mgr.set_store(self.store_prefix + entity, json.dumps(j))
        elif scope == CredentialScope.GLOBAL:
            self.known_entities[entity] = self.cred_class()
            j = self.cred_class.to_json(self.known_entities[entity])
            self.mgr.set_store(self.store_prefix + entity, json.dumps(j))
        else:
            raise CredentialException(f'Attempted to remove {self.cred_class.__name__.lower()} for unknown entity {entity}')

    def _validate_credential_entity(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        cred_type = self.cred_class.__name__.lower()
        if entity not in self.known_entities.keys():
            raise CredentialException(f'Attempted to access {cred_type} for unknown entity {entity}')
        if entity in self.per_host_credentials and not host:
            raise CredentialException(f'Need host to access {cred_type} for entity {entity}')
        if entity in self.per_service_name_credentials and not service_name:
            raise CredentialException(f'Need service name to access {cred_type} for entity {entity}')

    def credential_ls(self) -> Dict[str, Union[Type[CredentialProtocol], Dict[str, Type[CredentialProtocol]]]]:
        return self.known_entities

    def load(self) -> None:
        for k, v in self.mgr.get_store_prefix(self.store_prefix).items():
            entity = k[len(self.store_prefix):]
            self.known_entities[entity] = json.loads(v)
            if entity in self.per_service_name_credentials or entity in self.per_host_credentials:
                for k in self.known_entities[entity]:
                    credential_obj = self.cred_class.from_json(self.known_entities[entity][k])
                    self.known_entities[entity][k] = credential_obj
            else:
                credential_obj = self.cred_class.from_json(self.known_entities[entity])
                self.known_entities[entity] = credential_obj
