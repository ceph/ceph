#!/usr/bin/perl

use strict;
my $fn = shift @ARGV;
my $old = `cat $fn`;
my $header = `cat doc/modeline.txt`;

# strip existing modeline
my $new = $old;
$new =~ s/^\/\/ \-\*\- ([^\n]+) \-\*\-([^\n]*)\n//s; # emacs
$new =~ s/^\/\/ vim: ([^\n]*)\n//s; # vim;
$new =~ s/^\/\/ \-\*\- ([^\n]+) \-\*\-([^\n]*)\n//s; # emacs
$new =~ s/^\/\/ vim: ([^\n]*)\n//s; # vim;
$new =~ s/^\/\/ \-\*\- ([^\n]+) \-\*\-([^\n]*)\n//s; # emacs
$new =~ s/^\/\/ vim: ([^\n]*)\n//s; # vim;

# add correct header
$new = $header . $new;

if ($new ne $old) {
	print "$fn\n";
	open(O, ">$fn.new");
	print O $new;
	close O;
	system "diff $fn $fn.new";
	rename "$fn.new", $fn;
	#unlink "$fn.new";
}

