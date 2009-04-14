#!/usr/bin/perl

use strict;

my %r;      # reqid -> start
my %what;
my %lat_req;  # latency -> request

sub tosec($) {
    my $v = shift;
    my ($h, $m, $s) = split(/:/, $v);
    my $r = $s + 60 * ($m + (60 * $h));
    #print "$v = $h  $m  $s = $r\n";
    return $r;
}

while (<>) {
    chomp;
    my ($stamp) = /^\S+ (\S+)/;

    my ($req,$desc) = /\d+ -- \S+ mds\d+ ... client\d+ \S+ \d+ \S+ client_request\((\S+) ([^\)]+)/;
    
    if (defined $req) {
	#print "$req $len ($r{$req} - $stamp)\n";
	$r{$req} = $stamp;
	$what{$req} = $desc;
	next;
    }

    ($req,$desc) = /\d+ -- \S+ mds\d+ ... client\d+ \S+ \S+ client_reply\((\S+) ([^\)]+)/;
    if (defined $req) {
	if (exists $r{$req}) {
	    my $len = tosec($stamp) - tosec($r{$req});
	    
	    #print "$req $len ($r{$req} - $stamp)\n";
	    $lat_req{$len} = $req;
	    
	    delete $r{$req};
	}
    }
}


for my $len (sort {$b <=> $a} keys %lat_req) {
    my $req = $lat_req{$len};
    print "$len\t$req\t$what{$req}\n";    
}
