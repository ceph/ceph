#!/usr/bin/perl

use strict;
my %op;

my $line = 0;
while (<>) {
	#print "$line: $_";
	$line++;

	#osd3 do_op MOSDOp(client0.933 oid 100000000000008 0x84b4480) in pg[pginfo(4020000000d v 5662/0 e 2/1) r=0 active (0,5662]]
	if (my ($from, $opno, $oid, $op) = /do_op MOSDOp\((\S+) op (\d+) oid (\d+) (\w+)\)/) {
#		print "$op\n";
		if ($opno == 2 || $opno == 11 || $opno == 12 || $opno == 14 || $opno == 15) {
			$op{$op} = $from;
		}
	}

	# commits
	#osd1 op_modify_commit on op MOSDOp(client1.289 oid 100000100000002 0x51a2f788)
	if (my ($op) = /op_modify_commit.* (\w+)\)/) {
		delete $op{$op};
	}
	#osd4 rep_modify_commit on op MOSDOp(osd3.289 oid 100000000000008 0x84b0980)
	if (my ($op) = /rep_modify_commit.* (\w+)\)/) {
		delete $op{$op};
	}

	# forwarded?
	if (my ($op) = /sending (\w+) to osd/) {
		delete $op{$op};
	}

}

for my $op (keys %op) {
	print "---- lost op $op $op{$op}\n";
}
