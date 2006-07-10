#!/usr/bin/perl

use strict;
my %ack;
my %commit;

my $line = 0;
while (<>) {
	#print "$line: $_";
	$line++;

	#client1.objecter  write tid 305 osd1  oid 100000100000002 922848~10000
	if (my ($who, $tid) = /^(\S+)\.objecter  write tid\D+(\d+)\D+osd/) {
#		print "$who.$tid\n";
		$ack{"$who.$tid"} = $line;
		$commit{"$who.$tid"} = $line;
	}

	#client1.objecter handle_osd_write_reply 304 commit 0
	#client1.objecter handle_osd_write_reply 777 commit 1
	if (my ($who, $tid, $commit) = /^(\S+)\.objecter handle_osd_write_reply\D+(\d+)\D+commit\D+(\d)/) {
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
