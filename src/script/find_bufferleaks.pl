#!/usr/bin/perl

use strict;
my %buffers;

my $l = 1;
while (<>) {
    #print "$l: $_";
    
    if (/^raw_(\w+) alloc (\S+)/) {
	#print "alloc $2\n";
	$buffers{$2} = "$l: $_";
    }
    if (/^raw_(\w+) free (\S+)/) {
	#print "free $2\n";
	print "free without alloc at $l: $_" unless $buffers{$2};
	delete $buffers{$2};
    }
    $l++;
}

for my $x (sort {$buffers{$a} <=> $buffers{$b}} keys %buffers) {
    print "leaked $buffers{$x}";
}
