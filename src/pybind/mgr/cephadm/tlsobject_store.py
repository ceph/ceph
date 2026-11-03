from typing import (Any,
                    Dict,
                    List,
                    Set,
                    Tuple,
                    Optional,
                    TYPE_CHECKING,
                    Type,
                    Callable)
import json
import logging

from cephadm.tlsobject_types import TLSObjectProtocol, TLSObjectException, TLSObjectScope, TLSObjectTarget


if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


logger = logging.getLogger(__name__)


TLSOBJECT_STORE_PREFIX = 'cert_store.'

# Service- and host-scoped objects are stored one KV entry per target, as
# '<TLSOBJECT_STORE_TARGET_PREFIX><storage prefix>.<obj_name>/<target>'. A
# single entry holding every target of a name outgrows
# mon_config_key_max_entry_size once the name has a few dozen targets (e.g.
# a cephadm-signed cert per host), and the mon then refuses every save.
# The per-name entry under TLSOBJECT_STORE_PREFIX is kept as an empty map, so
# that the name is still found by load(). Service ids and hostnames cannot
# contain the separator.
TLSOBJECT_STORE_TARGET_PREFIX = 'cert_store.target.'
TLSOBJECT_TARGET_SEPARATOR = '/'


def _kv_preview(key: str, raw: str, n: int = 20) -> Tuple[str, int, str]:
    # Safe, short, escaped preview for logs
    return key, (len(raw) if raw else 0), (raw[:n] if raw else "")


class TLSObjectStore():

    def __init__(self, mgr: 'CephadmOrchestrator',
                 tlsobject_class: Type[TLSObjectProtocol],
                 known_objects_names: Dict[TLSObjectScope, List[str]],
                 cephadm_signed_obj_checker: Callable[[str], bool]) -> None:

        self.mgr: CephadmOrchestrator = mgr
        self.cephadm_signed_object_checker = cephadm_signed_obj_checker
        self.tlsobject_class = tlsobject_class
        self.service_scoped_objects = known_objects_names[TLSObjectScope.SERVICE]
        self.host_scoped_objects = known_objects_names[TLSObjectScope.HOST]
        self.global_scoped_objects = known_objects_names[TLSObjectScope.GLOBAL]
        self.store_prefix = f'{TLSOBJECT_STORE_PREFIX}{tlsobject_class.STORAGE_PREFIX}.'
        self.target_prefix = f'{TLSOBJECT_STORE_TARGET_PREFIX}{tlsobject_class.STORAGE_PREFIX}.'
        # initialize objects by name for the different scopes
        self.objects_by_name: Dict[str, Any] = {}
        for n in self.service_scoped_objects + self.host_scoped_objects:
            self.objects_by_name[n] = {}
        for n in self.global_scoped_objects:
            self.objects_by_name[n] = self.tlsobject_class()

    def _kv_key(self, obj_name: str) -> str:
        return self.store_prefix + obj_name

    def _set_store(self, obj_name: str, payload: Any) -> None:
        self.mgr.set_store(self._kv_key(obj_name), json.dumps(payload))

    def _target_kv_key(self, obj_name: str, target: str) -> str:
        return f'{self.target_prefix}{obj_name}{TLSOBJECT_TARGET_SEPARATOR}{target}'

    def _set_target_store(self, obj_name: str, target: Optional[str],
                          tlsobject: Optional[TLSObjectProtocol]) -> None:
        """Persist (or, with None, remove) the object of one target of a scoped name."""
        if not target:
            raise TLSObjectException(f'Need a target to store TLS object {obj_name}')
        key = self._target_kv_key(obj_name, target)
        if tlsobject is None:
            self.mgr.set_store(key, None)
        else:
            self.mgr.set_store(key, json.dumps(self.tlsobject_class.to_json(tlsobject)))

    def ensure_tombstone(self, obj_name: str, scope: TLSObjectScope) -> None:
        """
        Idempotently ensure a tombstone (empty) KV entry for obj_name so the
        name is rediscovered after manager restarts (see load method).

        - SERVICE/HOST → `{}` (empty per-target map)
        - GLOBAL       → minimal JSON for an empty TLS object
        """

        # Do nothing if the TLS object name already exists in the store
        if self.mgr.get_store_prefix(self._kv_key(obj_name)):
            return

        if scope in (TLSObjectScope.SERVICE, TLSObjectScope.HOST):
            self._set_store(obj_name, {})
        elif scope == TLSObjectScope.GLOBAL:
            empty = self.tlsobject_class()  # falsy empty instance
            self._set_store(obj_name, self.tlsobject_class.to_json(empty))

    def register_object_name(self, obj_name: str, scope: TLSObjectScope) -> None:
        """
        Register a new TLS object name under the specified scope if it does not already exist.
        Args:
            obj_name (str): The name of the TLS object to add.
            scope (TLSObjectScope): The scope of the object (SERVICE, HOST, or GLOBAL).
        Raises:
            ValueError: If an invalid scope is provided.
        """
        if obj_name not in self.objects_by_name:
            # Initialize an empty dictionary/TLSobj to track TLS objects for this obj_name
            if scope in (TLSObjectScope.SERVICE, TLSObjectScope.HOST):
                self.objects_by_name[obj_name] = {}
            elif scope == TLSObjectScope.GLOBAL:
                self.objects_by_name[obj_name] = self.tlsobject_class()
            else:
                raise ValueError(f"Invalid TLSObjectScope '{scope}' for obj_name '{obj_name}'")
            # Initialize an empty tombstone for the TLS object(s) in the store
            self.ensure_tombstone(obj_name, scope)

        # Add to the appropriate scope list
        if scope == TLSObjectScope.SERVICE and obj_name not in self.service_scoped_objects:
            self.service_scoped_objects.append(obj_name)
        elif scope == TLSObjectScope.HOST and obj_name not in self.host_scoped_objects:
            self.host_scoped_objects.append(obj_name)
        elif scope == TLSObjectScope.GLOBAL and obj_name not in self.global_scoped_objects:
            self.global_scoped_objects.append(obj_name)
        elif scope not in [TLSObjectScope.HOST, TLSObjectScope.SERVICE, TLSObjectScope.GLOBAL]:
            raise ValueError(f"Invalid TLSObjectScope '{scope}' for obj_name '{obj_name}'")

    def determine_tlsobject_target(self, obj_name: str, target: Optional[str]) -> TLSObjectTarget:
        if obj_name in self.service_scoped_objects:
            return TLSObjectTarget(service=target, host=None)
        elif obj_name in self.host_scoped_objects:
            return TLSObjectTarget(service=None, host=target)
        else:
            return TLSObjectTarget(service=None, host=None)

    def get_tlsobject_scope_and_target(self, obj_name: str,
                                       service_name: Optional[str] = None,
                                       host: Optional[str] = None) -> Tuple[TLSObjectScope, Optional[Any]]:

        if obj_name in self.service_scoped_objects:
            return TLSObjectScope.SERVICE, service_name
        elif obj_name in self.host_scoped_objects:
            return TLSObjectScope.HOST, host
        elif obj_name in self.global_scoped_objects:
            return TLSObjectScope.GLOBAL, None
        else:
            return TLSObjectScope.UNKNOWN, None

    def get_tlsobject_if_exists(self,
                                obj_name: str,
                                service_name: Optional[str] = None,
                                host: Optional[str] = None) -> Optional[TLSObjectProtocol]:
        try:
            return self.get_tlsobject(obj_name, service_name, host)
        except TLSObjectException:
            return None

    def get_tlsobject(self, obj_name: str,
                      service_name: Optional[str] = None,
                      host: Optional[str] = None) -> Optional[TLSObjectProtocol]:

        self._validate_tlsobject_name(obj_name, service_name, host)
        scope, target = self.get_tlsobject_scope_and_target(obj_name, service_name, host)
        if scope == TLSObjectScope.GLOBAL:
            return self.objects_by_name.get(obj_name)
        else:
            return self.objects_by_name.get(obj_name, {}).get(target)

    def save_tlsobject(self, obj_name: str,
                       tlsobject: str,
                       service_name: Optional[str] = None,
                       host: Optional[str] = None,
                       user_made: bool = False,
                       editable: bool = False) -> None:

        self._validate_tlsobject_name(obj_name, service_name, host)
        tlsobject = self.tlsobject_class(tlsobject, user_made, editable)
        scope, target = self.get_tlsobject_scope_and_target(obj_name, service_name, host)
        if scope in (TLSObjectScope.SERVICE, TLSObjectScope.HOST):
            self.objects_by_name[obj_name][target] = tlsobject
            self._set_target_store(obj_name, target, tlsobject)
        elif scope == TLSObjectScope.GLOBAL:
            self.objects_by_name[obj_name] = tlsobject
            self._set_store(obj_name, self.tlsobject_class.to_json(tlsobject))
        else:
            logger.error(f'Trying to save TLS object name {obj_name} with a not-supported/unknown TLSObjectScope scope {scope.value}')

    def rm_tlsobject(self, obj_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        """
        Remove a TLS object from the in-memory registry and persist the change.

        Behavior by scope:
          - SERVICE / HOST: Removes the entry for the given target (service_name/host).
            If this was the last target for that name, the name remains registered with
            an empty per-target map.
          - GLOBAL: Resets the object for `obj_name` to an empty instance of
            `tlsobject_class` and writes it back to the store. The store key is NOT
            deleted; the empty object acts as a tombstone.

            Keeping the KV key stable allows watchers/consumers to distinguish
            "known-but-empty" from "unknown", and lets future `save_tlsobject(...)`
            calls reuse the same name without recreating metadata.

        Args:
            obj_name: Registered TLS object name to remove.
            service_name: Required when the name is service-scoped; identifies the target.
            host: Required when the name is host-scoped; identifies the target.

        Returns:
            True if a change was persisted to the store; False if there was nothing to remove.

        Raises:
            TLSObjectException: If `obj_name` is unknown, or the required `service_name`/`host`
                target is missing for a scoped name, or the name resolves to an
                unsupported/unknown scope.

        Notes:
            - An “empty” TLS object is falsy and serializes to the minimal JSON defined
              by `tlsobject_class.to_json`.
        """
        self._validate_tlsobject_name(obj_name, service_name, host)
        scope, target = self.get_tlsobject_scope_and_target(obj_name, service_name, host)
        if scope in (TLSObjectScope.SERVICE, TLSObjectScope.HOST):
            if obj_name in self.objects_by_name and target in self.objects_by_name[obj_name]:
                del self.objects_by_name[obj_name][target]
                self._set_target_store(obj_name, target, None)
                return True
        elif scope == TLSObjectScope.GLOBAL:
            self.objects_by_name[obj_name] = self.tlsobject_class()
            serialized_obj = self.tlsobject_class.to_json(self.objects_by_name[obj_name])
            self._set_store(obj_name, serialized_obj)
            return True
        else:
            raise TLSObjectException(f'Attempted to remove {self.tlsobject_class.__name__.lower()} for unknown obj_name {obj_name}')
        return False

    def tlsobject_exists(self, obj_name: str) -> bool:
        return obj_name in self.objects_by_name

    def _validate_tlsobject_name(self, obj_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        cred_type = self.tlsobject_class.__name__.lower()
        if obj_name not in self.objects_by_name:
            raise TLSObjectException(f'Attempted to access {cred_type} for unknown TLS object name {obj_name}')
        if obj_name in self.host_scoped_objects and not host:
            raise TLSObjectException(f'Need host to access {cred_type} for TLS object {obj_name}')
        if obj_name in self.service_scoped_objects and not service_name:
            raise TLSObjectException(f'Need service name to access {cred_type} for TLS object {obj_name}')

    def list_tlsobjects(self) -> List[Tuple[str, TLSObjectProtocol, Optional[str]]]:
        """
        Returns a shallow list of all known TLS objects, including their targets.

        Returns:
            List of tuples: (obj_name, tlsobject, target)
            - obj_name: The TLS object name.
            - tlsobject: The TLS object itself.
            - target: The associated target (service_name, host, or None for global).
        """
        tlsobjects = []
        for known_obj_name, value in self.objects_by_name.items():
            if isinstance(value, dict):  # Handle per-service or per-host TLS objects
                for target, tlsobject in value.items():
                    if tlsobject:
                        tlsobjects.append((known_obj_name, tlsobject, target))
            elif value:  # Handle Global TLS objects
                tlsobjects.append((known_obj_name, value, None))

        return tlsobjects

    def load(self) -> None:
        # Scoped names whose objects were reset by this load
        loaded: Set[str] = set()
        # Targets found in a per-name entry, and the raw payloads of those that
        # could not be decoded there, by name; see _move_legacy_targets()
        legacy_targets: Dict[str, Dict[str, TLSObjectProtocol]] = {}
        undecodable: Dict[str, Dict[str, Any]] = {}

        for k, v in self.mgr.get_store_prefix(self.store_prefix).items():
            obj_name = k[len(self.store_prefix):]
            is_cephadm_signed_object = self.cephadm_signed_object_checker(obj_name)
            if not is_cephadm_signed_object and obj_name not in self.objects_by_name:
                logger.warning("TLSObjectStore: Discarding unknown obj_name %r", obj_name)
                continue

            try:
                tls_object_targets = json.loads(v)
                if not isinstance(tls_object_targets, dict):
                    key_preview, vlen, vstart = _kv_preview(k, v)
                    logger.error(
                        "TLSObjectStore: Invalid data structure for object %r. "
                        "Expected dict but got %s. key=%r, len=%d, startswith=%r",
                        obj_name, type(tls_object_targets).__name__,
                        key_preview, vlen, vstart,
                    )
                    continue
            except json.JSONDecodeError as e:
                key_preview, vlen, vstart = _kv_preview(k, v)
                logger.warning(
                    "TLSObjectStore: Cannot parse JSON for %r: key=%r, len=%d, startswith=%r, error=%r",
                    obj_name, key_preview, vlen, vstart, e,
                )
                continue
            except Exception as e:
                key_preview, vlen, vstart = _kv_preview(k, v)
                logger.error(
                    "TLSObjectStore: Unexpected error while parsing %r: key=%r, len=%d, startswith=%r, error=%r",
                    obj_name, key_preview, vlen, vstart, e,
                    exc_info=True,  # include traceback for unexpected errors
                )
                continue

            if is_cephadm_signed_object or (obj_name in self.service_scoped_objects) or (obj_name in self.host_scoped_objects):
                if is_cephadm_signed_object and obj_name not in self.host_scoped_objects:
                    self.host_scoped_objects.append(obj_name)
                self.objects_by_name[obj_name] = {}
                loaded.add(obj_name)
                for target, payload in tls_object_targets.items():
                    try:
                        tlsobject = self.tlsobject_class.from_json(payload)
                        if tlsobject:  # skip tombstones
                            legacy_targets.setdefault(obj_name, {})[target] = tlsobject
                        else:
                            logger.debug("TLSObjectStore: Skipping tombstone for %r (target %r)", obj_name, target)
                    except Exception as e:
                        undecodable.setdefault(obj_name, {})[target] = payload
                        key_preview, vlen, vstart = _kv_preview(k, str(payload))
                        logger.warning(
                            "TLSObjectStore: Failed to decode scoped TLS object %r (target %r): %r. "
                            "key=%r, len=%d, startswith=%r",
                            obj_name, target, e, key_preview, vlen, vstart
                        )
            elif obj_name in self.global_scoped_objects:
                try:
                    self.objects_by_name[obj_name] = self.tlsobject_class.from_json(tls_object_targets)
                except Exception as e:
                    key_preview, vlen, vstart = _kv_preview(k, v)
                    logger.warning(
                        "TLSObjectStore: Failed to decode global TLS object %r: %r. "
                        "key=%r, len=%d, startswith=%r",
                        obj_name, e, key_preview, vlen, vstart
                    )
            else:
                logger.error("TLSObjectStore: Found a known TLS object name %r with unknown scope!", obj_name)

        self._load_targets(loaded)
        self._move_legacy_targets(legacy_targets, undecodable)

    def _load_targets(self, loaded: Set[str]) -> None:
        for k, v in self.mgr.get_store_prefix(self.target_prefix).items():
            obj_name, sep, target = k[len(self.target_prefix):].partition(TLSOBJECT_TARGET_SEPARATOR)
            if not (obj_name and sep and target):
                logger.warning("TLSObjectStore: Ignoring malformed key %r", k)
                continue
            is_cephadm_signed_object = self.cephadm_signed_object_checker(obj_name)
            if not (is_cephadm_signed_object
                    or obj_name in self.service_scoped_objects
                    or obj_name in self.host_scoped_objects):
                logger.warning("TLSObjectStore: Discarding unknown obj_name %r (target %r)", obj_name, target)
                continue
            if is_cephadm_signed_object and obj_name not in self.host_scoped_objects:
                self.host_scoped_objects.append(obj_name)
            if obj_name not in loaded:
                self.objects_by_name[obj_name] = {}
                loaded.add(obj_name)
            try:
                tlsobject = self.tlsobject_class.from_json(json.loads(v))
            except Exception as e:
                key_preview, vlen, vstart = _kv_preview(k, v)
                logger.warning(
                    "TLSObjectStore: Failed to decode scoped TLS object %r (target %r): %r. "
                    "key=%r, len=%d, startswith=%r",
                    obj_name, target, e, key_preview, vlen, vstart
                )
                continue
            if tlsobject:
                self.objects_by_name[obj_name][target] = tlsobject

    def _move_legacy_targets(self, legacy_targets: Dict[str, Dict[str, TLSObjectProtocol]],
                             undecodable: Dict[str, Dict[str, Any]]) -> None:
        """
        Move targets found in per-name entries to entries of their own.

        Per-name entries held every target until targets got entries of their
        own, and an older mgr may have written one since. Either way such an
        entry is newer than any per-target entry for the same name, so its
        targets win. Once moved, the per-name entry keeps only the targets that
        could not be decoded (left for inspection), so moved targets are never
        applied again on a later load.
        """
        for obj_name, targets in legacy_targets.items():
            self.objects_by_name[obj_name].update(targets)
            for target, tlsobject in targets.items():
                self._set_target_store(obj_name, target, tlsobject)
            self._set_store(obj_name, undecodable.get(obj_name, {}))
            logger.info("TLSObjectStore: Moved %d target(s) of %r to per-target entries",
                        len(targets), obj_name)
