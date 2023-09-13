import json
import pytest
import unittest
from unittest import mock

import telemetry
from typing import cast, Any, DefaultDict, Dict, List, Optional, Tuple, TypeVar, TYPE_CHECKING, Union

OptionValue = Optional[Union[bool, int, float, str]]

Collection = telemetry.module.Collection
ALL_CHANNELS = telemetry.module.ALL_CHANNELS
MODULE_COLLECTION = telemetry.module.MODULE_COLLECTION

COLLECTION_BASE = ["basic_base", "device_base", "crash_base", "ident_base"]

class TestTelemetry:
    @pytest.mark.parametrize("preconfig,postconfig,prestore,poststore,expected",
            [
                (
                    # user is not opted-in
                    {
                        'last_opt_revision': 1,
                        'enabled': False,
                    },
                    {
                        'last_opt_revision': 1,
                        'enabled': False,
                    },
                    {
                        # None
                    },
                    {
                        'collection': []
                    },
                    {
                        'is_opted_in': False,
                        'is_enabled_collection':
                        {
                            'basic_base': False,
                            'basic_mds_metadata': False,
                        },
                    },
                ),
                (
                    # user is opted-in to an old revision
                    {
                        'last_opt_revision': 2,
                        'enabled': True,
                    },
                    {
                        'last_opt_revision': 2,
                        'enabled': True,
                    },
                    {
                        # None
                    },
                    {
                        'collection': []
                    },
                    {
                        'is_opted_in': False,
                        'is_enabled_collection':
                        {
                            'basic_base': False,
                            'basic_mds_metadata': False,
                        },
                    },
                ),
                (
                    # user is opted-in to the latest revision
                    {
                        'last_opt_revision': 3,
                        'enabled': True,
                    },
                    {
                        'last_opt_revision': 3,
                        'enabled': True,
                    },
                    {
                        # None
                    },
                    {
                        'collection': COLLECTION_BASE
                    },
                    {
                        'is_opted_in': True,
                        'is_enabled_collection':
                        {
                            'basic_base': True,
                            'basic_mds_metadata': False,
                        },
                    },
                ),
            ])
    def test_upgrade(self,
                preconfig: Dict[str, Any], \
                postconfig: Dict[str, Any], \
                prestore: Dict[str, Any], \
                poststore: Dict[str, Any], \
                expected: Dict[str, Any]) -> None:

        m = telemetry.Module('telemetry', '', '')

        if preconfig is not None:
            for k, v in preconfig.items():
                # no need to mock.patch since _ceph_set_module_option() which
                # is called from set_module_option() is already mocked for
                # tests, and provides setting default values for all module
                # options
                m.set_module_option(k, v)

        m.config_update_module_option()
        m.load()

        collection = json.loads(m.get_store('collection'))

        assert collection == poststore['collection']
        assert m.is_opted_in() == expected['is_opted_in']
        assert m.is_enabled_collection(Collection.basic_base) == expected['is_enabled_collection']['basic_base']
        assert m.is_enabled_collection(Collection.basic_mds_metadata) == expected['is_enabled_collection']['basic_mds_metadata']
