"""
Unit tests for ec_parity_consistency.py

Tests can be run by invoking pytest from the qa directory:

cd ceph/qa
python3 -m venv tasks/tests/venv
source tasks/tests/venv/bin/activate
pip install -r tasks/tests/requirements.txt
pytest tasks/tests/test_parity_consistency.py
deactivate (when done)

"""
import os
import random
import tempfile
from typing import List
from unittest.mock import Mock, MagicMock
from tasks.ec_parity_consistency import ErasureCodeObject, ErasureCodeObjects


class TestParityConsistency:
    """
    Tests for the classes contributing to ec_parity_consistency.py
    Mostly covers basic functionality of the ErasureCodeObject class,
    other classes are quite reliant on a functioning Ceph cluster
    and would be difficult to meaningfully test with unit tests
    """
    def create_dummy_ec_obj(self, k: int,
                            m: int,
                            shards: List[bytearray]) -> ErasureCodeObject:
        """
        Return an ErasureCodeObject created with the specified k + m
        values and the supplied shards
        """
        rnd = random.randint(0, 999)
        oid = 'rbd_data.4.1061d2ba6457.000000000000' + str(rnd)
        snapid = -2
        ec_obj = ErasureCodeObject(oid, snapid, {'k': k, 'm': m})
        for i in range(0, k + m):
            ec_obj.update_shard(i, shards[i])
        return ec_obj

    def create_random_shards(self, count: int) -> List[bytearray]:
        """
        Return a List of shards (bytearrays) filled with random data
        """
        shards = []
        chunk_size = random.randint(1024, 1048576)
        for _ in range(0, count):
            shards.append(bytearray(os.urandom(chunk_size)))
        return shards

    def test_ec_profile_jerasure(self):
        """
        Test the EC profile dict is processed and parsed properly for Jerasure pools
        """
        profile_dict = {
            'crush-device-class': '',
            'crush-failure-domain': 'osd',
            'crush-num-failure-domains': 3,
            'crush-osds-per-failure-domain': 3,
            'crush-root': 'default',
            'jerasure-per-chunk-alignment': 'false',
            'k': 4,
            'm': 2,
            'plugin': 'jerasure',
            'technique': '',
            'w': 8
        }
        supported_techniques = [ 'reed_sol_van', 'reed_sol_r6_op', 'cauchy_orig',
                                  'cauchy_good', 'liberation', 'blaum_roth', 'liber8tion' ]
        for technique in supported_techniques:
            profile_dict['technique'] = technique
            ec_obj = ErasureCodeObject('', 0, profile_dict)
            profile_str = ec_obj.get_ec_tool_profile()
            expected_str = ('crush-device-class=,crush-failure-domain=osd,'
                            'crush-num-failure-domains=3,'
                            'crush-osds-per-failure-domain=3,'
                            'crush-root=default,'
                            'jerasure-per-chunk-alignment=false,k=4,m=2,'
                            'plugin=jerasure,technique=' + technique + ',w=8')
            assert profile_str == expected_str

    def test_ec_profile_isa(self):
        """
        Test the EC profile dict is processed and parsed properly for ISA-L pools
        """
        profile_dict = {
            'crush-device-class': '',
            'crush-failure-domain': 'host',
            'crush-num-failure-domains': 2,
            'crush-osds-per-failure-domain': 1,
            'crush-root': 'default',
            'k': 8,
            'm': 3,
            'plugin': 'isa',
            'technique': ''
        }
        supported_techniques = [ 'reed_sol_van', 'cauchy' ]
        for technique in supported_techniques:
            profile_dict['technique'] = technique
            ec_obj = ErasureCodeObject('', 0, profile_dict)
            profile_str = ec_obj.get_ec_tool_profile()
            expected_str = ('crush-device-class=,crush-failure-domain=host,'
                            'crush-num-failure-domains=2,'
                            'crush-osds-per-failure-domain=1,'
                            'crush-root=default,k=8,m=3,'
                            'plugin=isa,technique=' + technique)
            assert profile_str == expected_str

    def test_want_to_encode_str(self):
        """
        Test the want to encode string for the EC tool is formatted correctly
        """
        ec_obj = ErasureCodeObject('test', 0, {'k': 1, 'm': 0})
        strout = ec_obj.get_shards_to_encode_str()
        assert strout == '0'
        ec_obj = ErasureCodeObject('test', 0, {'k': 8, 'm': 3})
        strout = ec_obj.get_shards_to_encode_str()
        assert strout == '0,1,2,3,4,5,6,7,8,9,10'
        ec_obj = ErasureCodeObject('test', 0, {'k': 5, 'm': 1})
        strout = ec_obj.get_shards_to_encode_str()
        assert strout == '0,1,2,3,4,5'
        ec_obj = ErasureCodeObject('test', 0, {'k': 12, 'm': 0})
        strout = ec_obj.get_shards_to_encode_str()
        assert strout == '0,1,2,3,4,5,6,7,8,9,10,11'
        ec_obj = ErasureCodeObject('test', 0, {'k': 2, 'm': 0})
        strout = ec_obj.get_shards_to_encode_str()
        assert strout == '0,1'

    def test_shard_updates(self):
        """
        Check that shards are correctly updated and returned
        for an EC object.
        """
        m = random.randint(1, 3)
        k = random.randint(m + 1, 12)
        data_shards = self.create_random_shards(k)
        parity_shards = self.create_random_shards(m)
        shards = data_shards + parity_shards
        ec_obj = self.create_dummy_ec_obj(k, m, shards)
        obj_data_shards = ec_obj.get_data_shards()
        obj_parity_shards = ec_obj.get_parity_shards()
        assert len(obj_data_shards) == k
        assert len(obj_parity_shards) == m
        assert data_shards == obj_data_shards
        assert parity_shards == obj_parity_shards

    def test_delete_shards(self):
        """
        Test that the delete shards function sets the
        shard list elements back to None.
        """
        m = random.randint(1, 3)
        k = random.randint(m + 1, 12)
        shards = self.create_random_shards(k + m)
        ec_obj = self.create_dummy_ec_obj(k, m, shards)
        shards = ec_obj.get_data_shards() + ec_obj.get_parity_shards()
        for shard in shards:
            assert shard is not None
        ec_obj.delete_shards()
        shards = ec_obj.get_data_shards() + ec_obj.get_parity_shards()
        for shard in shards:
            assert shard is None

    def test_comparing_parity_shards(self):
        """
        Test the compare parity shards function
        """
        m = random.randint(1, 3)
        k = random.randint(m + 1, 12)
        shards = self.create_random_shards(k + m)
        ec_obj = self.create_dummy_ec_obj(k, m, shards)
        parity_shards = ec_obj.get_parity_shards()
        tmpdir = tempfile.gettempdir()
        tmpname = 'ec_obj'
        for i, shard in enumerate(parity_shards):
            filepath = tmpdir + '/' + tmpname + '.' + str(i + k)
            with open(filepath, "wb") as binary_file:
                binary_file.write(shard)
                binary_file.close()
        filepath = tmpdir + '/' + tmpname
        shards_match = ec_obj.compare_parity_shards_to_files(filepath)
        assert shards_match is True
        shards = self.create_random_shards(1)
        ec_obj.update_shard(k, shards[0])
        shards_match = ec_obj.compare_parity_shards_to_files(filepath)
        assert shards_match is False

    def test_writing_data_shards(self):
        """
        Test the data shard write function produces a single file
        of an object's data shards in order
        """
        m = random.randint(1, 3)
        k = random.randint(m + 1, 12)
        shards = self.create_random_shards(k + m)
        data_shard = bytearray()
        for i, shard in enumerate(shards):
            if i < k:
                data_shard += shard
        ec_obj = self.create_dummy_ec_obj(k, m, shards)
        tmpdir = tempfile.gettempdir()
        filepath = tmpdir + '/ec_obj'
        ec_obj.write_data_shards_to_file(filepath)
        with open(filepath, "rb") as binary_file:
            contents = binary_file.read()
        assert bytearray(contents) == data_shard
        shards = self.create_random_shards(k)
        data_shard = bytearray()
        for shard in shards:
            data_shard += shard
        assert bytearray(contents) != data_shard

    def test_get_object_by_uid(self):
        """
        Test an object can be retrieved from ErasureCodeObjects
        by its UID
        """
        manager = Mock()
        manager.get_filepath = MagicMock(return_value='/tmp/')
        manager.get_osd_dump_json = MagicMock(return_value={'pools': ''})
        ec_objs = ErasureCodeObjects(manager, {})
        ec_objs.create_ec_object('test', -2, {'k': 2, 'm': 2, 'test_key': 20})
        ec_obj = ec_objs.get_object_by_uid('test_-2')
        assert ec_obj.ec_profile['test_key'] == 20

    def test_only_isa_and_jerasure_pools_are_supported(self):
        """
        Test only Jerasure and ISA are supported by the consistency checker
        """
        manager = Mock()
        manager.get_filepath = MagicMock(return_value='/tmp/')
        manager.get_osd_dump_json = MagicMock(return_value={'pools': ''})
        ec_objs = ErasureCodeObjects(manager, {})
        supported_plugins = ['jerasure', 'isa']
        for plugin in supported_plugins:
            ec_objs.get_ec_profile_for_pool = MagicMock(return_value={'plugin': plugin})
            is_supported = ec_objs.is_ec_plugin_supported({'pool': 3})
            assert is_supported == True
        unsupported_plugins = ['lrc', 'shec', 'clay']
        for plugin in unsupported_plugins:
            ec_objs.get_ec_profile_for_pool = MagicMock(return_value={'plugin': plugin})
            is_supported = ec_objs.is_ec_plugin_supported({'pool': 3})
            assert is_supported == False
