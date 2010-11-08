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

opendir PROC, "/proc";
while(my $pid = readdir PROC) {
        next if $pid =~ /\D/;        # not a pid
        next if !-o "/proc/$pid";    # not ours
        open CMDLINE, "/proc/$pid/cmdline" or next;
        my $cmdline = <CMDLINE>;
        $cmdline =~ s/[^\x20-\x7e]/ /g;
        close CMDLINE;
        next unless $cmdline =~ /\b(ceph|cfuse|cmds|cmon|cosd|osdmaptool|rados|vstart\.sh)\b/;
        print "$pid\t$cmdline\n";
}
