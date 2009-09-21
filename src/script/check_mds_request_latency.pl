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

    my ($req,$desc) = /\<\=\= client\d+ \S+ \d+ \=\=\=\= client_request\((\S+) ([^\)]+)/;
    
    if (defined $req) {
	#print "$req\n"; # ($r{$req} - $stamp)\n";
	$r{$req} = $stamp;
	$what{$req} = $desc;
	next;
    }

    my $who;
    ($who, $req,$desc) = /\-\-\> (client\d+) \S+ \-\- client_reply\((\S+) ([^\)]+)/;
    if (defined $req) {
	$req =~ s/\?\?\?/$who/;
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
