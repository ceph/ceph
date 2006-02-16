#!/usr/bin/perl

my $rank = shift @ARGV;
my $args = join(' ',@ARGV);
if ($rank == $ENV{MPD_JRANK}) {
	$c = "LD_PRELOAD=$ENV{'HOME'}/csl/obsd/src/pmds/gprof-helper.so ./tcpsyn $args";

} else {
	$c = "./tcpsyn.nopg $args";
}

#print "$rank: $c\n";
system $c;
