#!/usr/bin/perl

use strict;

my @file = <>;
sub get_op {
	my @op = shift @file;
	while (@file && 
		   $file[0] !~ /^[a-z]+$/) {
		push( @op, shift @file );
	}
	#print "op = ( @op )\n";
	return @op;
}

my $n = 0;
while (@file) {
	my ($op, @args) = &get_op;
	while ($op eq "read\n" ||
		   $op eq "write\n") {
		die unless scalar(@args) == 3;
		my ($nop, @nargs) = &get_op;
		if ($nop eq $op 
			&& ($args[0] == $nargs[0] )
			&& ($args[2] + $args[1] == $nargs[2])
			) {
			die unless scalar(@nargs) == 3;
			$args[1] += $nargs[1];
			$args[1] .= "\n";
			die unless scalar(@args) == 3;
			#print STDOUT "combining $n $op @args\n";
			$n++;
		} else {
#			print STDERR "not combinging\n";
			unshift( @file, $nop, @nargs );
			die unless scalar(@args) == 3;
			last;
		}
	}
	print $op;
	print join('', @args);
}
