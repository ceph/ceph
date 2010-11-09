#!/usr/bin/perl
use strict;

#
# ps-ceph.pl: Displays a list of ceph processes running locally
#
# Copyright (C) 2010, Dreamhost
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#

sub is_ceph_proc {
        my $cmdline = @_[0];
        return 1 if $cmdline =~ /\bceph\b/;
        return 1 if $cmdline =~ /\bcfuse\b/;
        return 1 if $cmdline =~ /\bcmds\b/;
        return 1 if $cmdline =~ /\bcmon\b/;
        return 1 if $cmdline =~ /\bcosd\b/;
        return 1 if $cmdline =~ /\bosdmaptool\b/;
        return 1 if $cmdline =~ /\brados\b/;
        return 1 if $cmdline =~ /test_/;
        return 1 if $cmdline =~ /\bvstart.sh\b/;

        return 0;
}

opendir PROC, "/proc";
while(my $pid = readdir PROC) {
        next if $pid =~ /\D/;        # not a pid
        next if !-o "/proc/$pid";    # not ours
        open CMDLINE, "/proc/$pid/cmdline" or next;
        my $cmdline = <CMDLINE>;
        $cmdline =~ s/[^\x20-\x7e]/ /g;
        close CMDLINE;
        if (is_ceph_proc($cmdline)) {
                print "$pid\t$cmdline\n";
        }
}
