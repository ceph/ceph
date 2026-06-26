# -*- coding: utf-8 -*-
import json
import logging
from typing import TypedDict

from .. import mgr
from ..plugins.ttl_cache import ttl_cache

logger = logging.getLogger('services.telemetry')


class UserPersonaDistribution(TypedDict):
    admin: int
    read_only: int
    block_storage_operator: int
    file_system_operator: int
    object_storage_operator: int
    monitoring: int
    primary_usage_persona: str
    persona_diversity: int


class TelemetryMetricsResponse(TypedDict):
    user_persona_distribution: UserPersonaDistribution


class DashboardTelemetryService:
    KV_USER_PERSONA = 'telemetry/metrics/user_persona_distribution'

    ROLE_TO_PERSONA = {
        'administrator': 'admin',
        'read-only': 'read_only',
        'block-manager': 'block_storage_operator',
        'cephfs-manager': 'file_system_operator',
        'rgw-manager': 'object_storage_operator',
        'cluster-manager': 'monitoring',
    }

    PERSONA_PRIORITY = [
        'administrator',
        'cluster-manager',
        'block-manager',
        'cephfs-manager',
        'rgw-manager',
        'read-only',
    ]

    @classmethod
    @ttl_cache(300, label='user_persona_distribution')
    def get_user_persona_distribution(cls) -> UserPersonaDistribution:
        persona_raw = mgr.get_store(cls.KV_USER_PERSONA)
        if not persona_raw:
            return cls._detect_and_cache_user_personas()
        try:
            return json.loads(persona_raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning(
                'telemetry: invalid cached persona data; recomputing')
            return cls._detect_and_cache_user_personas()

    @classmethod
    def _derive_persona_insights(cls, persona_counts):
        active_personas = {
            key: value
            for key, value in persona_counts.items()
            if value > 0
        }

        if active_personas:
            primary_persona = max(
                active_personas, key=lambda k: active_personas[k])
        else:
            primary_persona = "none"

        return {
            "primary_usage_persona": primary_persona,
            "persona_diversity": len(active_personas),
        }

    @classmethod
    def _detect_and_cache_user_personas(cls) -> UserPersonaDistribution:
        try:
            access_db = mgr.ACCESS_CTRL_DB
            users = access_db.users
        except Exception as e:  # pylint: disable=broad-except
            logger.error('telemetry: failed to read access control DB: %s', e)
            return {
                'admin': 0,
                'read_only': 0,
                'block_storage_operator': 0,
                'file_system_operator': 0,
                'object_storage_operator': 0,
                'monitoring': 0,
                'primary_usage_persona': 'none',
                'persona_diversity': 0
            }

        persona_counts = {
            'admin': 0,
            'read_only': 0,
            'block_storage_operator': 0,
            'file_system_operator': 0,
            'object_storage_operator': 0,
            'monitoring': 0
        }

        for user in users.values():
            user_role_names = [
                role.name if hasattr(role, 'name') else str(role)
                for role in user.roles
            ]

            classified = False
            for priority_role in cls.PERSONA_PRIORITY:
                if priority_role in user_role_names:
                    persona = cls.ROLE_TO_PERSONA.get(priority_role)
                    if persona:
                        persona_counts[persona] += 1
                        classified = True
                        break

            if not classified:
                for role_name in user_role_names:
                    persona = cls.ROLE_TO_PERSONA.get(role_name)
                    if persona:
                        persona_counts[persona] += 1
                        break

        insights = cls._derive_persona_insights(persona_counts)

        result: UserPersonaDistribution = {
            'admin': persona_counts['admin'],
            'read_only': persona_counts['read_only'],
            'block_storage_operator': persona_counts['block_storage_operator'],
            'file_system_operator': persona_counts['file_system_operator'],
            'object_storage_operator': persona_counts['object_storage_operator'],
            'monitoring': persona_counts['monitoring'],
            'primary_usage_persona': insights['primary_usage_persona'],
            'persona_diversity': insights['persona_diversity']
        }

        mgr.set_store(cls.KV_USER_PERSONA, json.dumps(result))
        return result

    @classmethod
    def refresh_user_personas(cls) -> UserPersonaDistribution:
        return cls._detect_and_cache_user_personas()

    @classmethod
    def get_metrics(cls) -> TelemetryMetricsResponse:
        return {
            'user_persona_distribution': cls.get_user_persona_distribution()
        }
