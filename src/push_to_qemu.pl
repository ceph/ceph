#!/usr/bin/perl

use strict;

my $usage = "./push_to_qemu.pl <path_to_qemu>\n";

my $qemu = shift @ARGV || die $usage;
die $usage unless -d $qemu;

die "not in a git tree" unless `cd $qemu && git rev-parse HEAD`;

my $dir = '.';
until (-d "$dir/.git") {
    $dir .= "/..";
}

print "pushing changed shared files from $dir to $qemu...\n";
system "cat $dir/src/include/rbd_types.h | sed 's/__u32/uint32_t/g; s/__u8/uint8_t/g; s/__.*16/uint16_t/g; s/__.*32/uint32_t/g; s/__.*64/uint64_t/g; s/_FS_CEPH_RBD/QEMU_BLOCK_RBD_TYPES_H/g; s/^\t/    /g' | expand | grep -v \"linux/types.h\" > $qemu/block/rbd_types.h";

print "done.\n";

