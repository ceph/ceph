#!/usr/bin/perl

my %pin;
my %hist;     
my $l = 1;
while (<>) {
	if (/auth_pin /) {
		my ($what) = / on (.*\])/;
#		print "add_waiter $c $what\n";
		$pin{$what}++;
		$hist{$what} .= "$l: $_";
	}
	if (/auth_unpin/) {
		my ($what) = / on (.*\])/;
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
