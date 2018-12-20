# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .ceph_service import CephService


class CephX(object):
    @classmethod
    def _entities_map(cls, entity_type=None):
        auth_dump = CephService.send_command("mon", "auth list")
        result = {}
        for auth_entry in auth_dump['auth_dump']:
            entity = auth_entry['entity']
            if not entity_type or entity.startswith('{}.'.format(entity_type)):
                entity_id = entity[entity.find('.')+1:]
                result[entity_id] = auth_entry
        return result

    @classmethod
    def _clients_map(cls):
        return cls._entities_map("client")

    @classmethod
    def list_clients(cls):
        return [client for client in cls._clients_map()]

    @classmethod
    def get_client_key(cls, client_id):
        return cls._clients_map()[client_id]['key']
