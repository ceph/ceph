#!/usr/bin/perl

my %pin;
my %hist;     
my $l = 1;
while (<>) {

	# cinode:auth_pin on inode [1000000002625 /gnu/blah_client_created. 0x89b7700] count now 1 + 0

	if (/auth_pin /) {
		my ($what) = / on (.*\[\S+ )/;
#		print "add_waiter $c $what\n";
		$pin{$what}++;
		$hist{$what} .= "$l: $_";
	}

	# cinode:auth_unpin on inode [1000000002625 (dangling) 0x89b7700] count now 0 + 0

	if (/auth_unpin/) {
		my ($what) = / on (.*\[\S+ )/;# / on (.*\])/;
		$pin{$what}--;
		$hist{$what} .= "$l: $_";
		unless ($pin{$what}) {
			delete $hist{$what};
			delete $pin{$what};
		}
	}
	$l++;
}

for my $what (keys %pin) {
	print "---- count $pin{$what} on $what
$hist{$what}
";
}
