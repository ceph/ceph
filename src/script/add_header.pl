#!/usr/bin/perl

use strict;
my $fn = shift @ARGV;
my $old = `cat $fn`;

my $header = `cat doc/header.txt`;

# strip existing header
my $new = $old;
if ($new =~ /^(.*)\* Ceph - scalable distributed file system/s) {
	my ($a,@b) = split(/\*\/\n/, $new);
	$new = join("*/\n",@b);
}
$new = $header . $new;

if ($new ne $old) {
	open(O, ">$fn.new");
	print O $new;
	close O;
	system "diff $fn $fn.new";
	rename "$fn.new", $fn;
	#unlink "$fn.new";

}

