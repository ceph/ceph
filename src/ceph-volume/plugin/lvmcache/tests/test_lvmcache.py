import pytest
# TODO flake8 complains here
from ceph_volume.tests.conftest import capture, Capture
from ceph_volume import process
from ceph_volume.api import lvm as api
from ceph_volume.util import disk
import plugin.lvmcache.ceph_volume_lvmcache.main as lvmcache

class TestLVMCache(object):

    def setup(self):
        # LV for the cache's data
        self.data_lv = api.Volume(lv_name='cache_block_osd.0', lv_path='/dev/vg/datalv',
            lv_tags=''
        )
        # LV for the cache's metadata
        self.md_lv = api.Volume(lv_name='cache_block_md_osd.0', lv_path='/dev/vg/mdlv',
            lv_tags=''
        )
        # The LV that we want to cache, ie. the OSD's LV pre-cache.
        self.osd_block_lv = api.Volume(lv_name='osd_block_lv', lv_path='/dev/vg/osdblocklv',
            lv_tags=''
        )
        # An LV that is already cached
        # Post-cache, to be used for rm_lvmcache and set_caching_mode
        self.cached_lv = api.Volume(lv_name='cached_lv', lv_path='/dev/originvg/cachedlv', vg_name='originvg',
            lv_tags='ceph.type=block,ceph.osd_id=0,ceph.cluster_fsid="fsid",ceph.cluster_name="dummy",ceph.lvmcache_lv=cacheLV'
        )
        # The newly created hidden cache LV - serves as the actual cache
        self.cache_lv = api.Volume(lv_name='[cache_block_osd.0]', lv_path='', lv_tags='')

    def test_create_lvmcache_pool(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        lvmcache.create_lvmcache_pool(self.data_lv, self.md_lv)
        expected = [
            'lvconvert',
            '--yes',
            '--poolmetadataspare', 'n',
            '--type', 'cache-pool',
            '--poolmetadata', '/dev/vg/mdlv', '/dev/vg/datalv',
        ]
        assert capture.calls[0]['args'][0] == expected

    def test_create_lvmcache(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        lvmcache.create_lvmcache(self.data_lv, self.osd_block_lv)
        expected = [
            'lvconvert',
            '--yes',
            '--type', 'cache',
            '--cachepool', '/dev/vg/datalv', '/dev/vg/osdblocklv'
        ]
        assert capture.calls[0]['args'][0] == expected


    def test__create_lvmcache(self, monkeypatch, capture):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)

        def get_lv(lv_name, vg_name):
            if '[cache_block_osd.0]' in lv_name:
                return self.cache_lv
            return None

        monkeypatch.setattr(api, 'get_lv', get_lv)
        lvmcache._create_lvmcache('vg', self.osd_block_lv, self.md_lv, self.data_lv)

        expected = [
            [
                'lvconvert',
                '--yes',
                '--poolmetadataspare', 'n',
                '--type', 'cache-pool',
                '--poolmetadata', '/dev/vg/mdlv', '/dev/vg/datalv'
            ],
            [
                'lvconvert',
                '--yes',
                '--type', 'cache',
                '--cachepool', '/dev/vg/datalv', '/dev/vg/osdblocklv'
            ],
            [
                'lvchange',
                '--addtag', 'ceph.lvmcache_lv=cache_block_osd.0', '/dev/vg/osdblocklv'
            ],
            [
                "lvchange",
                "--cachemode",
                "writeback",
                "/dev/vg/osdblocklv"
            ]
        ]

        assert len(capture.calls) == len(expected)
        for i in range(0, len(expected)):
            assert capture.calls[i]['args'][0] == expected[i]


    def test_set_caching_mode(self, monkeypatch, capture, capsys):
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        lvmcache.set_lvmcache_caching_mode('writethrough', self.cached_lv)
        expected = [
            'lvchange',
            '--cachemode', 'writethrough', '/dev/originvg/cachedlv'
        ]
        assert capture.calls[0]['args'][0] == expected

        lvmcache.set_lvmcache_caching_mode('writeback', self.cached_lv)
        expected = [
            'lvchange',
            '--cachemode', 'writeback', '/dev/originvg/cachedlv'
        ]
        assert capture.calls[1]['args'][0] == expected

        lvmcache.set_lvmcache_caching_mode('bogus', self.cached_lv)
        assert len(capture.calls) == 2
        captured = capsys.readouterr()
        assert 'Unknown caching mode: bogus' in captured.out


    def test_get_cache_mode(self, monkeypatch, capture):
        capture = Capture(return_values = [[], [],
            [
                # stdout
                ['  ceph.block_device=/dev/originvg/cachedlv,ceph.block_uuid=MYUUID,ceph.lvmcache_lv=cache_block_osd.0,ceph.cluster_fsid=myfsid,ceph.cluster_name=ceph,ceph.osd_fsid=otherfsid,ceph.osd_id=0,ceph.type=block";"/dev/originvg/cachedlv";"cached_lv";"originvg";"writethrough";"smq";"'],
                # stderr
                '',
                # return code
                0
            ]
        ])
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        assert lvmcache.get_caching_mode(self.cached_lv) == 'writethrough'


    def test__create_cache_lvs(self, monkeypatch, capture):
        md_partition_output = 'NAME="sda" MAJ:MIN="8:0" RM="0" SIZE="20G" RO="0" TYPE="disk" MOUNTPOINT=""'
        data_partition_output = 'NAME="sdb" MAJ:MIN="8:0" RM="0" SIZE="238.5G" RO="0" TYPE="disk" MOUNTPOINT=""'
        small_md_partition_output = 'NAME="sdc" MAJ:MIN="8:0" RM="0" SIZE="1.5G" RO="0" TYPE="disk" MOUNTPOINT=""'
        small_data_partition_output = 'NAME="sdd" MAJ:MIN="8:0" RM="0" SIZE="900M" RO="0" TYPE="disk" MOUNTPOINT=""'
        def _lsblk(partition):
            if partition == '/dev/md':
                return disk._lsblk_parser(md_partition_output)
            if partition == '/dev/data':
                return disk._lsblk_parser(data_partition_output)
            if partition == '/dev/smallmd':
                return disk._lsblk_parser(small_md_partition_output)
            if partition == '/dev/smalldata':
                return disk._lsblk_parser(small_data_partition_output)
        monkeypatch.setattr(disk, 'lsblk', _lsblk)
        with pytest.raises(Exception) as e:
            lvmcache._create_cache_lvs('vg', '/dev/smallmd', '/dev/data', '', '')
        assert 'Metadata partition is too small' in str(e)

        with pytest.raises(Exception) as e:
            lvmcache._create_cache_lvs('vg', '/dev/md', '/dev/smalldata', '', '')
        assert 'Data partition is too small' in str(e)

        monkeypatch.setattr('ceph_volume.api.lvm.create_lv', lambda *a, **kw: kw)
        cache_md_lv, cache_data_lv = lvmcache._create_cache_lvs('vg', '/dev/md', '/dev/data', '0', self.cached_lv)
        assert cache_md_lv == {
            'name': 'cache_block_md_osd.0',
            'group': 'vg',
            # 19G
            'size': str(disk.Size(gb=19)._b) + 'B',
            'tags': {
                'ceph.osd_id': '0',
                'ceph.type': 'lvmcache_metadata',
                'ceph.partition': '/dev/md',
                'ceph.cluster_fsid': '"fsid"',
                'ceph.cluster_name': '"dummy"'
            },
            'pv': '/dev/md'
        }
        assert cache_data_lv == {
            'name': 'cache_block_osd.0',
            'group': 'vg',
            # 237.5G
            'size': str(disk.Size(gb=237.5)._b) + 'B',
            'tags': {
                'ceph.osd_id': '0',
                'ceph.type': 'lvmcache_data',
                'ceph.partition': '/dev/data',
                'ceph.cluster_fsid': '"fsid"',
                'ceph.cluster_name': '"dummy"'
            },
            'pv': '/dev/data'
        }


    def test_rm_cache(self, monkeypatch, capture):
        cdata_lv = api.Volume(lv_name='[cache_block_osd.0_cdata]', group='',
            lv_path='',
            lv_tags='ceph.partition="/dev/data"'
        )
        cmeta_lv = api.Volume(lv_name='[cache_block_osd.0_cmeta]', group='',
            lv_path='',
            lv_tags='ceph.partition="/dev/md"'
        )
        cached_lv_vg = api.VolumeGroup(vg_name='originvg')

        def get_lv(lv_name, vg_name):
            if 'cdata' in lv_name:
                return cdata_lv
            if 'cmeta' in lv_name:
                return cmeta_lv
            if 'cachedlv' in lv_name:
                return self.cached_lv
            return None

        # rm_lvmcache() relies on the return values of process.call, so we
        # have to mock the return values
        # ['', '', 0] stand for [stdout, stderr, returncode]
        capture = Capture(return_values = [[], [], ['', '', 0]])
        monkeypatch.setattr(process, 'run', capture)
        monkeypatch.setattr(process, 'call', capture)
        monkeypatch.setattr(api, 'get_lv', get_lv)
        monkeypatch.setattr(api, 'get_vg', lambda *a, **kw: cached_lv_vg)

        lvmcache.rm_lvmcache(self.cached_lv)
        expected = [
            [
                "lvremove",
                "-v",
                "-f",
                "originvg/cacheLV"
            ],
            [
                "vgreduce",
                "--force",
                "--yes",
                "originvg",
                "\"/dev/data\"",
                "\"/dev/md\""
            ],
            [
                "pvremove",
                "-v",
                "-f",
                "-f",
                "\"/dev/data\""
            ],
            [
                "pvremove",
                "-v",
                "-f",
                "-f",
                "\"/dev/md\""
            ],
            [
                "lvchange",
                "--deltag",
                "ceph.lvmcache_lv=cacheLV",
                "/dev/originvg/cachedlv"
            ]
        ]

        assert len(capture.calls) == len(expected)
        for i in range(0, len(expected)):
            assert capture.calls[i]['args'][0] == expected[i]
