# -*- coding: utf-8 -*-
import json
import logging
from typing import TypedDict

from .. import mgr
from ..plugins.ttl_cache import ttl_cache

logger = logging.getLogger('services.telemetry')

# TypedDicts for proper typing
class ProtocolsEnabled(TypedDict):  # pylint: disable=inherit-non-class
    block: bool
    file: bool
    object: bool

class TelemetryMetrics(TypedDict):  # pylint: disable=inherit-non-class
    protocols_enabled: ProtocolsEnabled

# Service class
class DashboardTelemetryService:
    KV_PROTOCOLS = 'telemetry/metrics/protocols_enabled'
    
    @classmethod
    @ttl_cache(300, label='protocols_enabled')  # 5 min cache
    def get_protocols_enabled(cls) -> ProtocolsEnabled:
        """Get cached or detect enabled protocols"""
        prot_raw = mgr.get_store(cls.KV_PROTOCOLS)
        if prot_raw:
            return json.loads(prot_raw)
        return cls._detect_and_cache_protocols()
    
    @classmethod
    def _detect_and_cache_protocols(cls) -> ProtocolsEnabled:
        """
        Inspect osd_map pool application_metadata to determine which protocols
        are in use:
            'rbd'    → Block  (RBD)
            'cephfs' → File   (CephFS)
            'rgw'    → Object (RGW / S3)
        Result is cached in KV store so telemetry and prometheus can read it
        without re-inspecting osd_map every scrape.
        """
        try:
            osd_map = mgr.get('osd_map') or {}
            pools = osd_map.get('pools', [])
        except Exception as e:  # pylint: disable=broad-except
            logger.error('telemetry: failed to read osd_map: %s', e)
            return {'block': False, 'file': False, 'object': False}

        # Single-pass detection with early exit 
        protocols_found = {'rbd': False, 'cephfs': False, 'rgw': False}
        
        for pool in pools:
            app_meta = pool.get('application_metadata') or {}
            
            for protocol in protocols_found:
                if not protocols_found[protocol] and protocol in app_meta:
                    protocols_found[protocol] = True
            
            if all(protocols_found.values()):
                break
        
        result: ProtocolsEnabled = {
            'block': protocols_found['rbd'],
            'file': protocols_found['cephfs'],
            'object': protocols_found['rgw']
        }
        
        mgr.set_store(cls.KV_PROTOCOLS, json.dumps(result))
        return result

    @classmethod
    def refresh_protocols(cls) -> ProtocolsEnabled:
        """Force refresh of protocol detection (bypasses cache)"""
        return cls._detect_and_cache_protocols()
        
    @classmethod
    def get_metrics(cls) -> TelemetryMetrics:
        """Get all telemetry metrics"""
        return {
            'protocols_enabled': cls.get_protocols_enabled()
        }
