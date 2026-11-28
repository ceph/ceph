# -*- coding: utf-8 -*-
"""
Persistence of the multi-cluster configuration.

The configuration holds credentials (JWTs of remote clusters and Prometheus
access info), so it must not be kept in a regular module option: module options
are readable by anyone allowed to read the ``config-opt`` scope and show up in
``ceph config dump``. It is kept in the mgr module's private KV store instead.

Configurations saved by previous releases in the ``MULTICLUSTER_CONFIG`` module
option are migrated transparently (and the plain text copy is removed).
"""
import copy
import json
import logging
import threading
from ast import literal_eval
from typing import Any, Dict, Optional, Tuple

from .. import mgr

logger = logging.getLogger(__name__)

STORE_KEY = 'multicluster_config'
# Module option used by previous releases. It is still declared in
# ``Module.MODULE_OPTIONS`` so it can be read, migrated and removed.
LEGACY_OPTION = 'MULTICLUSTER_CONFIG'
LOCAL_CLUSTER_ALIAS = 'local-cluster'
# Fields of a cluster entry that must never be returned by the REST API.
SECRET_FIELDS = ('token', 'prometheus_access_info')

_lock = threading.RLock()
_legacy_checked = False


def _parse(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw or not isinstance(raw, str):
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError:
        try:
            parsed = literal_eval(raw)
        except (ValueError, SyntaxError):
            return {}
    return parsed if isinstance(parsed, dict) else {}


def _strip_local_cluster_tokens(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Previous releases stored the token of every login in the 'local-cluster'
    entry. Nothing uses it, so it is not carried over.
    """
    config = copy.deepcopy(config)
    for clusters in (config.get('config') or {}).values():
        for cluster in clusters:
            if cluster.get('cluster_alias') == LOCAL_CLUSTER_ALIAS:
                cluster.pop('token', None)
    return config


def save(config: Dict[str, Any]) -> None:
    mgr.set_store(STORE_KEY, json.dumps(config))


def migrate_legacy() -> None:
    """
    Move the configuration saved in the legacy module option to the KV store
    and delete the option, so credentials are not left behind in the config DB.
    """
    global _legacy_checked  # pylint: disable=global-statement
    with _lock:
        raw = mgr.get_module_option(LEGACY_OPTION, '')
        if raw:
            legacy = _parse(raw)
            # A legacy value can only be newer than the stored one (e.g. after
            # a downgrade followed by a new upgrade), so it takes precedence.
            if legacy:
                save(_strip_local_cluster_tokens(legacy))
            mgr.set_module_option(LEGACY_OPTION, None)
            logger.info('Multi-cluster configuration migrated from the %s module '
                        'option to the KV store', LEGACY_OPTION)
        _legacy_checked = True


def load() -> Dict[str, Any]:
    """Return the full (unredacted) configuration. For internal use only."""
    if not _legacy_checked:
        try:
            migrate_legacy()
        except Exception:  # pylint: disable=broad-except
            logger.exception('Failed to migrate the multi-cluster configuration')
    return _parse(mgr.get_store(STORE_KEY))


def redact(config: Dict[str, Any],
           reveal_token_for: Optional[Tuple[str, str]] = None) -> Dict[str, Any]:
    """
    Return a copy of the configuration that is safe to send to API clients.

    :param reveal_token_for: optional ``(url, user)`` of the remote cluster the
        caller is switching to, whose token the browser needs in order to talk
        to it. Every other secret is removed.
    """
    redacted = copy.deepcopy(config)
    for clusters in (redacted.get('config') or {}).values():
        for cluster in clusters:
            keep_token = (
                reveal_token_for is not None
                and cluster.get('cluster_alias') != LOCAL_CLUSTER_ALIAS
                and (cluster.get('url'), cluster.get('user')) == tuple(reveal_token_for)
            )
            for field in SECRET_FIELDS:
                if field == 'token' and keep_token:
                    continue
                cluster.pop(field, None)
    return redacted


def find_token(config: Dict[str, Any], url: str, user: str) -> Optional[str]:
    for clusters in (config.get('config') or {}).values():
        for cluster in clusters:
            if cluster.get('url') == url and cluster.get('user') == user:
                return cluster.get('token')
    return None
