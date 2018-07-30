
# mount flags
mount = dict(
    xfs=['rw', 'noatime' , 'inode64']
)


# format flags
mkfs = dict(
    xfs=[
        # force overwriting previous fs
        '-f',
        # set the inode size to 2kb
        '-i', 'size=2048',
    ],
)

# The fantastical world of ceph-disk labels, they should give you the
# collywobbles
ceph_disk_guids = {
    # luks
    '45b0969e-9b03-4f30-b4c6-35865ceff106': {'type': 'journal', 'encrypted': True, 'encryption_type': 'luks'},
    'cafecafe-9b03-4f30-b4c6-35865ceff106': {'type': 'block', 'encrypted': True, 'encryption_type': 'luks'},
    '166418da-c469-4022-adf4-b30afd37f176': {'type': 'block.db', 'encrypted': True, 'encryption_type': 'luks'},
    '86a32090-3647-40b9-bbbd-38d8c573aa86': {'type': 'block.wal', 'encrypted': True, 'encryption_type': 'luks'},
    '4fbd7e29-9d25-41b8-afd0-35865ceff05d': {'type': 'osd', 'encrypted': True, 'encryption_type': 'luks'},
    # plain
    '45b0969e-9b03-4f30-b4c6-5ec00ceff106': {'type': 'journal', 'encrypted': True, 'encryption_type': 'plain'},
    'cafecafe-9b03-4f30-b4c6-5ec00ceff106': {'type': 'block', 'encrypted': True, 'encryption_type': 'plain'},
    '93b0052d-02d9-4d8a-a43b-33a3ee4dfbc3': {'type': 'block.db', 'encrypted': True, 'encryption_type': 'plain'},
    '306e8683-4fe2-4330-b7c0-00a917c16966': {'type': 'block.wal', 'encrypted': True, 'encryption_type': 'plain'},
    '4fbd7e29-9d25-41b8-afd0-5ec00ceff05d': {'type': 'osd', 'encrypted': True, 'encryption_type': 'plain'},
}
