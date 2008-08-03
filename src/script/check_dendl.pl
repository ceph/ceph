#!/usr/bin/perl

my $in = 0;
my $line = 1;
while (<>) {
    chomp;
    my $l = $_;
    my @w = split(/\s+/, $_);
    for (@w) {
	if (/^(\/\/)?dout\(\s*\-?\s*\d+\s*\)$/) {
	    warn "$line: dout without dendl: $l\n" if $in;
	    $in = 1;
	}
	elsif (/^derr\(\s*\-?\s*\d+\s*\)$/) {
	    warn "$line: dout without dendl: $l\n" if $in;
	    $in = 1;
	}
	elsif (/^dendl/) {
	    warn "$line: dendl without dout: $l\n" unless $in;
	    $in = 0;
	}
    }
    $line++;
}
