
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
    '4fbd7e29-9d25-41b8-afd0-35865ceff05d': {'type': 'data', 'encrypted': True, 'encryption_type': 'luks'},
    # plain
    '45b0969e-9b03-4f30-b4c6-5ec00ceff106': {'type': 'journal', 'encrypted': True, 'encryption_type': 'plain'},
    'cafecafe-9b03-4f30-b4c6-5ec00ceff106': {'type': 'block', 'encrypted': True, 'encryption_type': 'plain'},
    '93b0052d-02d9-4d8a-a43b-33a3ee4dfbc3': {'type': 'block.db', 'encrypted': True, 'encryption_type': 'plain'},
    '306e8683-4fe2-4330-b7c0-00a917c16966': {'type': 'block.wal', 'encrypted': True, 'encryption_type': 'plain'},
    '4fbd7e29-9d25-41b8-afd0-5ec00ceff05d': {'type': 'data', 'encrypted': True, 'encryption_type': 'plain'},
    # regular guids that differ from plain
    'fb3aabf9-d25f-47cc-bf5e-721d1816496b': {'type': 'lockbox', 'encrypted': False, 'encryption_type': None},
    '30cd0809-c2b2-499c-8879-2d6b78529876': {'type': 'block.db', 'encrypted': False, 'encryption_type': None},
    '5ce17fce-4087-4169-b7ff-056cc58473f9': {'type': 'block.wal', 'encrypted': False, 'encryption_type': None},
    '4fbd7e29-9d25-41b8-afd0-062c0ceff05d': {'type': 'data', 'encrypted': False, 'encryption_type': None},
    'cafecafe-9b03-4f30-b4c6-b4b80ceff106': {'type': 'block', 'encrypted': False, 'encryption_type': None},
    # multipath
    '01b41e1b-002a-453c-9f17-88793989ff8f': {'type': 'block.wal', 'encrypted': False, 'encryption_type': None},
    'ec6d6385-e346-45dc-be91-da2a7c8b3261': {'type': 'block.wal', 'encrypted': False, 'encryption_type': None},
    '45b0969e-8ae0-4982-bf9d-5a8d867af560': {'type': 'journal', 'encrypted': False, 'encryption_type': None},
    '4fbd7e29-8ae0-4982-bf9d-5a8d867af560': {'type': 'data', 'encrypted': False, 'encryption_type': None},
    '7f4a666a-16f3-47a2-8445-152ef4d03f6c': {'type': 'lockbox', 'encrypted': False, 'encryption_type': None},
    'cafecafe-8ae0-4982-bf9d-5a8d867af560': {'type': 'block', 'encrypted': False, 'encryption_type': None},
}
