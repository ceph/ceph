#!/usr/bin/perl

my $offset = shift @ARGV;

while (<>) {
    #    next unless / \d\d bdev /;
    my $rest = $_;
    my @hit;
    while ($rest =~ /(\d+)~(\d+)/) {
	my ($o, $l) = $rest =~ /(\d+)~(\d+)/;
	$rest = $';
	if ($offset >= $o &&
	    $offset < $o + $l) {
	    my $rel = $offset - $o;
	    push(@hit, $rel);
	}
    }
    print join(',',@hit) . "\t$_" if @hit;
}
