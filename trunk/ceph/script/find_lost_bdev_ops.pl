#!/usr/bin/perl

use strict;
my %op;

my $line = 0;
while (<>) {
	#print $line . $_ if /0x8d4f6a0/;
	chomp;
	$line++;

	#bdev(./ebofsdev/0)._submit_io bio(wr 269~1 usemap 0x4de33cc0)
	if (my ($bio) = /_submit_io bio\(.*(0x\w+)\)/) {
		$op{$bio} = $line;
	}

	# cancel
	#bdev(./ebofsdev/3)._cancel_io bio(wr 1525~1 bh_write 0x8a437b8)
	if (my ($bio) = /_cancel_io bio\(.*(0x\w+)\)/ &&
		!(/FAILED/)) {
		delete $op{$bio};
	}
	
	# finish
	#bdev(./ebofsdev/3).complete_thread finishing bio(wr 1131~1 write_cnode 0x832c1f8)
	if (my ($bio) = /complete_thread finishing bio\(.*(0x\w+)\)/) {
		delete $op{$bio};
	}
	
}

for my $bio (keys %op) {
	print "---- lost bio $bio\n";
}
