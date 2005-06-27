#!/usr/bin/perl

use strict;
my %buffers;
my %bufferlists;
my %ref;
my %mal;
my $l = 1;
while (<>) {
	#print "$l: $_";

	# cinode:auth_pin on inode [1000000002625 /gnu/blah_client_created. 0x89b7700] count now 1 + 0

	if (/^buffer\.cons /) {
		my ($x) = /(0x\S+)/;
		$buffers{$x} = 1;
	}
	if (/^buffer\.des /) {
		my ($x) = /(0x\S+)/;
		die "des without cons at $l: $_" unless $buffers{$x};
		delete $buffers{$x};
		die "des with ref>0 at $l: $_" unless $ref{$x} == 0;
		delete $ref{$x};
	}

	if (/^bufferlist\.cons /) {
		my ($x) = /(0x\S+)/;
		$bufferlists{$x} = 1;
	}
	if (/^bufferlist\.des /) {
		my ($x) = /(0x\S+)/;
		warn "des without cons at $l: $_" unless $bufferlists{$x};
		delete $bufferlists{$x};
	}


	if (/^buffer\.malloc /) {
		my ($x) = /(0x\S+)/;
		$mal{$x} = 1;
	}
	if (/^buffer\.free /) {
		my ($x) = /(0x\S+)/;
		die "free with malloc at $l: $_" unless $mal{$x};
		delete $mal{$x};
	}

	if (/^buffer\.get /) {
		my ($x) = /(0x\S+)/;
		$ref{$x}++;
	}
	if (/^buffer\.get /) {
		my ($x) = /(0x\S+)/;
		$ref{$x}--;
	}

$l++;
}

for my $x (keys %bufferlists) {
	print "leaked bufferlist $x\n";
}

for my $x (keys %buffers) {
	print "leaked buffer $x ref $ref{$x}\n";
}

for my $x (keys %mal) {
	print "leaked buffer dataptr $x ref $ref{$x}\n";
}
