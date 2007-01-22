#!/usr/bin/perl

use strict;
my %ack;
my %commit;

my $line = 0;
while (<>) {
	#print "$line: $_";
	$line++;

	#client0.objecter writex_submit tid 21 osd0  oid 100000000000001 851424~100000
	if (my ($who, $tid) = /(\S+)\.objecter writex_submit tid\D+(\d+)\D+osd/) {
#		print "$who.$tid\n";
		$ack{"$who.$tid"} = $line;
		$commit{"$who.$tid"} = $line;
	}

	#client1.objecter handle_osd_write_reply 304 commit 0
	#client1.objecter handle_osd_write_reply 777 commit 1
	if (my ($who, $tid, $commit) = /(\S+)\.objecter handle_osd_write_reply\D+(\d+)\D+commit\D+(\d)/) {
#		print "$who.$tid\n";
		delete $ack{"$who.$tid"};
		delete $commit{"$who.$tid"} if $commit;
	}

}

for my $op (keys %commit) {
	print "---- lost commit $op $commit{$op}\n";
}
for my $op (keys %ack) {
	print "---- lost ack $op $commit{$op}\n";
}
