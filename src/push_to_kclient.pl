#!/usr/bin/perl

use strict;

my $usage = "./push_to_client.pl <path_to_kernel_git_tree>\n";

my $kernel = shift @ARGV || die $usage;
die $usage unless -d $kernel;
die $usage unless -e "$kernel/fs/ceph/README";

die "not in a git tree" unless `git-rev-parse HEAD`;

my $dir = '.';
until (-d "$dir/.git") {
    $dir .= "/..";
}

print "copy changed shared files from $dir to $kernel...\n";
my @files = split(/\n/, `cat $kernel/fs/ceph/README`);
for (@files) {
    next if /^#/;
    my ($orig, $new) = split(/\s+/, $_);
    #print "$dir/$orig -> $new\n";
    system "cp -uv $dir/$orig $kernel/$new";
}

print "done.\n";

