
# mount flags
mount = dict(
    xfs='noatime,inode64',
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

