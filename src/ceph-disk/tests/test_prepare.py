#!/bin/bash
#
# Copyright (C) 2015, 2016 Red Hat <contact@redhat.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
import argparse
import configobj
import mock
import os
import pytest
import shutil
import tempfile

from ceph_disk import main


class Base(object):

    def setup_class(self):
        main.setup_logging(True, False)
        os.environ['PATH'] = "..:" + os.environ['PATH']

    def setup(self):
        _, self.conf_file = tempfile.mkstemp()
        os.environ['CEPH_CONF'] = self.conf_file
        self.conf = configobj.ConfigObj(self.conf_file)
        self.conf['global'] = {}

    def teardown(self):
        os.unlink(self.conf_file)

    def save_conf(self):
        self.conf.write(open(self.conf_file, 'w'))


class TestPrepare(Base):

    def test_init_dir(self):
        parser = argparse.ArgumentParser('ceph-disk')
        subparsers = parser.add_subparsers()
        main.Prepare.set_subparser(subparsers)

        data = tempfile.mkdtemp()
        main.setup_statedir(data)
        args = parser.parse_args([
            'prepare',
            data,
        ])

        def set_type(self):
            self.type = self.FILE
        with mock.patch.multiple(main.PrepareData,
                                 set_type=set_type):
            prepare = main.Prepare.factory(args)
        assert isinstance(prepare.data, main.PrepareFilestoreData)
        assert prepare.data.is_file()
        assert isinstance(prepare.journal, main.PrepareJournal)
        assert prepare.journal.is_none()
        prepare.prepare()
        assert os.path.exists(os.path.join(data, 'fsid'))
        shutil.rmtree(data)

    @mock.patch('stat.S_ISBLK')
    @mock.patch('ceph_disk.main.is_partition')
    def test_init_dev(self, m_is_partition, m_s_isblk):
        m_s_isblk.return_value = True

        parser = argparse.ArgumentParser('ceph-disk')
        subparsers = parser.add_subparsers()
        main.Prepare.set_subparser(subparsers)

        m_is_partition.return_value = False
        _, data = tempfile.mkstemp()

        args = parser.parse_args([
            'prepare',
            data,
        ])
        prepare = main.Prepare.factory(args)
        assert isinstance(prepare.data, main.PrepareData)
        assert prepare.data.is_device()
        assert isinstance(prepare.journal, main.PrepareJournal)
        assert prepare.journal.is_device()

    def test_set_subparser(self):
        parser = argparse.ArgumentParser('ceph-disk')
        subparsers = parser.add_subparsers()
        main.Prepare.set_subparser(subparsers)
        osd_uuid = 'OSD_UUID'
        journal_uuid = 'JOURNAL_UUID'
        fs_type = 'xfs'
        data = 'DATA'
        args = parser.parse_args([
            'prepare',
            '--osd-uuid', osd_uuid,
            '--journal-uuid', journal_uuid,
            '--fs-type', fs_type,
            data,
        ])
        assert args.journal_uuid == journal_uuid
        assert args.osd_uuid == osd_uuid
        assert args.fs_type == fs_type
        assert args.data == data


class TestDevice(Base):

    @mock.patch('ceph_disk.main.is_partition')
    def test_init(self, m_is_partition):
        m_is_partition.return_value = False
        device = main.Device('/dev/wholedisk', argparse.Namespace())
        assert device.dev_size is None

    @mock.patch('ceph_disk.main.is_partition')
    @mock.patch('ceph_disk.main.get_free_partition_index')
    @mock.patch('ceph_disk.main.update_partition')
    @mock.patch('ceph_disk.main.get_dm_uuid')
    @mock.patch('ceph_disk.main.get_dev_size')
    @mock.patch('ceph_disk.main.command_check_call')
    def test_create_partition(self,
                              m_command_check_call,
                              m_get_dev_size,
                              m_get_dm_uuid,
                              m_update_partition,
                              m_get_free_partition_index,
                              m_is_partition):
        m_is_partition.return_value = False
        partition_number = 1
        m_get_free_partition_index.return_value = partition_number
        path = '/dev/wholedisk'
        device = main.Device(path, argparse.Namespace(dmcrypt=False))
        uuid = 'UUID'
        m_get_dm_uuid.return_value = uuid
        size = 200
        m_get_dev_size.return_value = size + 100
        name = 'journal'
        actual_partition_number = device.create_partition(
            uuid=uuid, name=name, size=size)
        assert actual_partition_number == partition_number
        command = ['sgdisk',
                   '--new=%d:0:+%dM' % (partition_number, size),
                   '--change-name=%d:ceph %s' % (partition_number, name),
                   '--partition-guid=%d:%s' % (partition_number, uuid),
                   '--typecode=%d:%s' % (
                       partition_number,
                       main.PTYPE['regular']['journal']['ready']),
                   '--mbrtogpt', '--', path]
        m_command_check_call.assert_called_with(command)
        m_update_partition.assert_called_with(path, 'created')

        actual_partition_number = device.create_partition(
            uuid=uuid, name=name)
        command = ['sgdisk',
                   '--largest-new=%d' % partition_number,
                   '--change-name=%d:ceph %s' % (partition_number, name),
                   '--partition-guid=%d:%s' % (partition_number, uuid),
                   '--typecode=%d:%s' % (
                       partition_number,
                       main.PTYPE['regular']['journal']['ready']),
                   '--mbrtogpt', '--', path]
        m_command_check_call.assert_called_with(command)


class TestDevicePartition(Base):

    def test_init(self):
        partition = main.DevicePartition(argparse.Namespace())
        for name in ('osd', 'journal'):
            assert (main.PTYPE['regular'][name]['ready'] ==
                    partition.ptype_for_name(name))

    def test_get_uuid(self):
        partition = main.DevicePartition(argparse.Namespace())
        uuid = 'UUID'
        with mock.patch.multiple(main,
                                 get_partition_uuid=lambda path: uuid):
            assert uuid == partition.get_uuid()
        assert uuid == partition.get_uuid()

    def test_get_ptype(self):
        partition = main.DevicePartition(argparse.Namespace())
        ptype = main.PTYPE['regular']['osd']['tobe']
        with mock.patch.multiple(main,
                                 get_partition_type=lambda path: ptype):
            assert ptype == partition.get_ptype()
        assert ptype == partition.get_ptype()

    def test_partition_number(self):
        partition = main.DevicePartition(argparse.Namespace())
        num = 123
        assert num != partition.get_partition_number()
        partition.set_partition_number(num)
        assert num == partition.get_partition_number()

    def test_dev(self):
        partition = main.DevicePartition(argparse.Namespace())
        dev = '/dev/sdbFOo'
        assert dev != partition.get_dev()
        assert dev != partition.get_rawdev()
        partition.set_dev(dev)
        assert dev == partition.get_dev()
        assert dev == partition.get_rawdev()

    @mock.patch('ceph_disk.main.is_mpath')
    def test_factory(self, m_is_mpath):
        parser = argparse.ArgumentParser('ceph-disk')
        subparsers = parser.add_subparsers()
        main.Prepare.set_subparser(subparsers)

        path = 'DATA'
        m_is_mpath.return_value = False

        #
        # Device partition
        #
        args = parser.parse_args([
            'prepare',
            path,
        ])
        partition = main.DevicePartition.factory(
            path=path, dev=None, args=args)
        assert isinstance(partition, main.DevicePartition)

        #
        # Multipath device partition
        #
        m_is_mpath.return_value = True
        args = parser.parse_args([
            'prepare',
            path,
        ])
        partition = main.DevicePartition.factory(
            path=path, dev=None, args=args)
        assert isinstance(partition, main.DevicePartitionMultipath)
        m_is_mpath.return_value = False

        #
        # Device partition encrypted via dmcrypt luks
        #
        args = parser.parse_args([
            'prepare',
            '--dmcrypt',
            path,
        ])
        partition = main.DevicePartition.factory(
            path=path, dev=None, args=args)
        assert isinstance(partition, main.DevicePartitionCryptLuks)

        #
        # Device partition encrypted via dmcrypt plain
        #
        self.conf['global']['osd dmcrypt type'] = 'plain'
        self.save_conf()
        args = parser.parse_args([
            'prepare',
            '--dmcrypt',
            path,
        ])
        partition = main.DevicePartition.factory(
            path=path, dev=None, args=args)
        assert isinstance(partition, main.DevicePartitionCryptPlain)


class TestDevicePartitionMultipath(Base):

    def test_init(self):
        partition = main.DevicePartitionMultipath(argparse.Namespace())
        for name in ('osd', 'journal'):
            assert (main.PTYPE['mpath'][name]['ready'] ==
                    partition.ptype_for_name(name))


class TestDevicePartitionCrypt(Base):

    @mock.patch('ceph_disk.main.get_conf')
    def test_luks(self, m_get_conf):
        parser = argparse.ArgumentParser('ceph-disk')
        subparsers = parser.add_subparsers()
        main.Prepare.set_subparser(subparsers)
        key_size = 256

        def get_conf(**kwargs):
            if kwargs['variable'] == 'osd_dmcrypt_key_size':
                return key_size
            elif kwargs['variable'] == 'osd_dmcrypt_type':
                return 'luks'
            elif kwargs['variable'] == 'osd_cryptsetup_parameters':
                return 'PARAMETERS'
            else:
                assert 0

        m_get_conf.side_effect = get_conf
        data = 'DATA'
        args = parser.parse_args([
            'prepare',
            data,
            '--dmcrypt',
        ])
        partition = main.DevicePartitionCryptLuks(args)
        assert partition.luks()
        assert partition.osd_dm_key is None
        uuid = 'UUID'
        with mock.patch.multiple(main,
                                 _dmcrypt_map=mock.DEFAULT,
                                 get_dmcrypt_key=mock.DEFAULT,
                                 get_partition_uuid=lambda path: uuid) as m:
            partition.map()
            assert m['_dmcrypt_map'].called
            m['get_dmcrypt_key'].assert_called_with(
                uuid, '/etc/ceph/dmcrypt-keys', True)


class TestCryptHelpers(Base):

    @mock.patch('ceph_disk.main.get_conf')
    def test_get_dmcrypt_type(self, m_get_conf):
        args = argparse.Namespace(dmcrypt=False)
        assert main.CryptHelpers.get_dmcrypt_type(args) is None

        m_get_conf.return_value = 'luks'
        args = argparse.Namespace(dmcrypt=True, cluster='ceph')
        assert main.CryptHelpers.get_dmcrypt_type(args) is 'luks'

        m_get_conf.return_value = None
        args = argparse.Namespace(dmcrypt=True, cluster='ceph')
        assert main.CryptHelpers.get_dmcrypt_type(args) is 'luks'

        m_get_conf.return_value = 'plain'
        args = argparse.Namespace(dmcrypt=True, cluster='ceph')
        assert main.CryptHelpers.get_dmcrypt_type(args) is 'plain'

        invalid = 'INVALID'
        m_get_conf.return_value = invalid
        args = argparse.Namespace(dmcrypt=True, cluster='ceph')
        with pytest.raises(main.Error) as err:
            main.CryptHelpers.get_dmcrypt_type(args)
        assert invalid in str(err)


class TestPrepareData(Base):

    def test_set_variables(self):
        parser = argparse.ArgumentParser('ceph-disk')
        subparsers = parser.add_subparsers()
        main.Prepare.set_subparser(subparsers)

        osd_uuid = 'OSD_UUID'
        cluster_uuid = '571bb920-6d85-44d7-9eca-1bc114d1cd75'
        data = 'data'
        args = parser.parse_args([
            'prepare',
            '--osd-uuid', osd_uuid,
            '--cluster-uuid', cluster_uuid,
            data,
        ])

        def set_type(self):
            self.type = self.FILE
        with mock.patch.multiple(main.PrepareData,
                                 set_type=set_type):
            data = main.PrepareData(args)
        assert data.args.osd_uuid == osd_uuid
        assert data.args.cluster_uuid == cluster_uuid

        data = 'data'
        args = parser.parse_args([
            'prepare',
            data,
        ])

        with mock.patch.multiple(main.PrepareData,
                                 set_type=set_type):
            data = main.PrepareData(args)
        assert 36 == len(data.args.osd_uuid)
        assert 36 == len(data.args.cluster_uuid)

        self.conf['global']['fsid'] = cluster_uuid
        self.save_conf()
        data = 'data'
        args = parser.parse_args([
            'prepare',
            data,
        ])

        with mock.patch.multiple(main.PrepareData,
                                 set_type=set_type):
            data = main.PrepareData(args)
        assert data.args.cluster_uuid == cluster_uuid
