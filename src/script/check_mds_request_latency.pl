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

    my ($what,$req,$desc) = /\d+ -- \S+ mds\d+ ... client\d+ \S+ \S+ client_(request|reply)\((\S+) ([^\)]+)/;
    
    if (defined $req) {
	#print "$what $req at $stamp $desc\n";
	if ($what eq 'request') {
	    $r{$req} = $stamp;
	    $what{$req} = $desc;
	} elsif ($what eq 'reply') {
	    if (exists $r{$req}) {
		my $len = tosec($stamp) - tosec($r{$req});

		#print "$req $len ($r{$req} - $stamp)\n";
		$lat_req{$len} = $req;

		delete $r{$req};
	    }
	} else {
	    die;
	}
    }

    
}


for my $len (sort {$b <=> $a} keys %lat_req) {
    my $req = $lat_req{$len};
    print "$len\t$req\t$what{$req}\n";    
}
