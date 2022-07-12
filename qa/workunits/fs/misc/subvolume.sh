#!/bin/sh -x

expect_failure() {
	if "$@"; then return 1; else return 0; fi
}

set -e

mkdir group
mkdir group/subvol1

setfattr -n ceph.dir.subvolume -v 1 group/subvol1

# rename subvolume
mv group/subvol1 group/subvol2

# move file out of the subvolume
touch group/subvol2/file1
expect_failure python3 -c "import os; os.rename('group/subvol2/file1', 'group/file1')"
# move file into the subvolume
touch group/file2
expect_failure python3 -c "import os; os.rename('group/file2', 'group/subvol2/file2')"

# create hardlink within subvolume
ln group/subvol2/file1 group/subvol2/file1_

# create hardlink out of subvolume
expect_failure ln group/subvol2/file1  group/file1_
expect_failure ln group/file2 group/subvol1/file2_

# create snapshot at subvolume root
mkdir group/subvol2/.snap/s1

# create snapshot at descendent dir of subvolume
mkdir group/subvol2/dir
expect_failure mkdir group/subvol2/dir/.snap/s2

mkdir group/subvol3
setfattr -n ceph.dir.subvolume -v 1 group/subvol3

# move file across subvolumes
expect_failure python3 -c "import os; os.rename('group/subvol2/file1', 'group/subvol3/file1')"

# create hardlink across subvolumes
expect_failure ln group/subvol2/file1 group/subvol3/file1

# create subvolume inside existing subvolume
expect_failure setfattr -n ceph.dir.subvolume -v 1 group/subvol2/dir

# clear subvolume flag
setfattr -n ceph.dir.subvolume -v 0 group/subvol2
mkdir group/subvol2/dir/.snap/s2

# parent subvolume override child subvolume
setfattr -n ceph.dir.subvolume -v 1 group/subvol2/dir
setfattr -n ceph.dir.subvolume -v 1 group/subvol2
expect_failure mkdir group/subvol2/dir/.snap/s3

rmdir group/subvol2/.snap/s1
rmdir group/subvol2/dir/.snap/s2
rm -rf group

echo OK
