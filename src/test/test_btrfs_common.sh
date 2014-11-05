#!/bin/bash 
#
# Author: Federico Gimenez <fgimenez@coit.es>
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

#
# Removes btrfs subvolumes under the given directory param
#
function teardown_btrfs() {
    local btrfs_base_dir=$1

    btrfs_dirs=`ls -l $btrfs_base_dir | egrep '^d' | awk '{print $9}'`
    for btrfs_dir in $btrfs_dirs
    do
        btrfs_subdirs=`ls -l $btrfs_base_dir/$btrfs_dir | egrep '^d' | awk '{print $9}'` 
        for btrfs_subdir in $btrfs_subdirs
        do
	    btrfs subvolume delete $btrfs_base_dir/$btrfs_dir/$btrfs_subdir
        done
    done
}
