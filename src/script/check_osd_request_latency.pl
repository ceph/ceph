#!/usr/bin/perl

use strict;

my %r;      # reqid -> start
my %lat_req;  # latency -> request
my %desc;

sub tosec($) {
    my $v = shift;
    my ($h, $m, $s) = split(/:/, $v);
    my $r = $s + 60 * ($m + (60 * $h));
    #print "$v = $h  $m  $s = $r\n";
    return $r;
}

while (<>) {
    chomp;
    my ($stamp) = /^\S+ (\S+) /;

    my ($who,$tid,$desc) = /osd\d+ <.. (\D+\d+) \S+ \d+ \S+ osd_op\((\S+) ([^\)]+)/;
    if (defined $tid) {
	my $req = "$who:$tid";
	$r{$req} = $stamp unless exists $r{$req};
	$desc{$req} = $desc;
	next;
    }

    my ($who,$tid) = /\d+ -- \S+ osd\d+ --> (\D+\d+) \S+ \S+ osd_op_reply\((\S+) /;
    if (defined $tid) {
	my $req = "$who:$tid";
	if (exists $r{$req}) {
	    my $len = tosec($stamp) - tosec($r{$req});
	    
	    #print "$req $len ($r{$req} - $stamp)\n";
	    $lat_req{$len} = $req;
	    
	    delete $r{$req};
	}
	next;
    }
    
}


for my $len (sort {$b <=> $a} keys %lat_req) {
    print "$len\t$lat_req{$len}\t$desc{$lat_req{$len}}\n";    
}
