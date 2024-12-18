ceph_bluestore_tool_output = '''
{
    "/dev/sdb": {
        "osd_uuid": "d5a496bc-dcb9-4ad0-a12c-393d3200d2b6",
        "size": 1099511627776,
        "btime": "2021-07-23T16:02:22.809186+0000",
        "description": "main",
        "bfm_blocks": "268435456",
        "bfm_blocks_per_key": "128",
        "bfm_bytes_per_block": "4096",
        "bfm_size": "1099511627776",
        "bluefs": "1",
        "ceph_fsid": "sdb-fsid",
        "ceph_version_when_created": "ceph version 19.3.0-5537-gb9ba4e48 (b9ba4e48633d6d90d5927a4e66b9ecbb4d7e6e73) squid (dev)",
        "kv_backend": "rocksdb",
        "magic": "ceph osd volume v026",
        "mkfs_done": "yes",
        "osd_key": "AQAO6PpgK+y4CBAAixq/X7OVimbaezvwD/cDmg==",
        "ready": "ready",
        "require_osd_release": "16",
        "type": "bluestore",
        "whoami": "0"
    },
    "/dev/vdx": {
        "osd_uuid": "d5a496bc-dcb9-4ad0-a12c-393d3200d2b6",
        "size": 214748364800,
        "btime": "2024-10-16T10:51:05.955279+0000",
        "description": "main",
        "bfm_blocks": "52428800",
        "bfm_blocks_per_key": "128",
        "bfm_bytes_per_block": "4096",
        "bfm_size": "214748364800",
        "bluefs": "1",
        "ceph_fsid": "2d20bc8c-8a0c-11ef-aaba-525400e54507",
        "ceph_version_when_created": "ceph version 19.3.0-5537-gb9ba4e48 (b9ba4e48633d6d90d5927a4e66b9ecbb4d7e6e73) squid (dev)",
        "created_at": "2024-10-16T10:51:09.121455Z",
        "elastic_shared_blobs": "1",
        "epoch": "16",
        "kv_backend": "rocksdb",
        "magic": "ceph osd volume v026",
        "multi": "yes",
        "osd_key": "AQCZmg9nxOKTCBAA6EQftuqMuKMHqypSAfqBsQ==",
        "ready": "ready",
        "type": "bluestore",
        "whoami": "5"
    },
    "/dev/vdy": {
        "osd_uuid": "d5a496bc-dcb9-4ad0-a12c-393d3200d2b6",
        "size": 214748364800,
        "btime": "2024-10-16T10:51:05.961279+0000",
        "description": "bluefs db"
    },
    "/dev/vdz": {
        "osd_uuid": "d5a496bc-dcb9-4ad0-a12c-393d3200d2b6",
        "size": 214748364800,
        "btime": "2024-10-16T10:51:05.961279+0000",
        "description": "bluefs wal"
    }
}
'''.split('\n')

lsblk_all = ['NAME="/dev/sdb" KNAME="/dev/sdb" PKNAME="" PARTLABEL=""',
             'NAME="/dev/sdx" KNAME="/dev/sdx" PKNAME="" PARTLABEL=""',
             'NAME="/dev/sdy" KNAME="/dev/sdy" PKNAME="" PARTLABEL=""',
             'NAME="/dev/sdz" KNAME="/dev/sdz" PKNAME="" PARTLABEL=""']

blkid_output = ['/dev/ceph-1172bba3-3e0e-45e5-ace6-31ae8401221f/osd-block-5050a85c-d1a7-4d66-b4ba-2e9b1a2970ae: TYPE="ceph_bluestore" USAGE="other"']

udevadm_property = '''DEVNAME=/dev/sdb
DEVTYPE=disk
ID_ATA=1
ID_BUS=ata
ID_MODEL=SK_hynix_SC311_SATA_512GB
ID_PART_TABLE_TYPE=gpt
ID_PART_TABLE_UUID=c8f91d57-b26c-4de1-8884-0c9541da288c
ID_PATH=pci-0000:00:17.0-ata-3
ID_PATH_TAG=pci-0000_00_17_0-ata-3
ID_REVISION=70000P10
ID_SERIAL=SK_hynix_SC311_SATA_512GB_MS83N71801150416A
TAGS=:systemd:
USEC_INITIALIZED=16117769'''.split('\n')