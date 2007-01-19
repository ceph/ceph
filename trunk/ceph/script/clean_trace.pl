#!/usr/bin/perl

my $n = 0;
while (<>) {
	next unless /trace: /;
	my $l = $';  $';
	print $l;
}
