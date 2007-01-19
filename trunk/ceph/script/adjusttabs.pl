#!/usr/bin/perl

my $tablen = shift @ARGV;
my $fn = shift @ARGV;

my $tab = ' ' x $tablen;
open(I, $fn);
my $f;
my $oldtab = ' ' x 4;
while (<I>) {
	if (my ($oldlen) = /\-\*\- .*tab-width:(\d)/) {
		print "old length was $oldlen\n";
		$oldtab = ' ' x $oldlen;
		s/tab-width:\d/tab-width:$tablen/;
	}
	s/\t/$oldtab/g;
	$f .= $_;
}
close I;
open(O, ">$fn.new");
print O $f;
close O;

rename "$fn.new", $fn;
