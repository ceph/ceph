#!/usr/bin/perl

my $offset = shift @ARGV;

while (<>) {
    #    next unless / \d\d bdev /;
    my $rest = $_;
    my @hit;
    while ($rest =~ /([\da-f]+)~([\da-f]+)/) {
	my ($o, $l) = $rest =~ /([\da-f]+)~([\da-f]+)/;
	$rest = $';
	if (hex($offset) >= hex($o) &&
	    hex($offset) < hex($o) + hex($l)) {
	    my $rel = hex($offset) - hex($o);
	    push(@hit, sprintf("%x",$rel));
	}
    }
    print join(',',@hit) . "\t$_" if @hit;
}
