#!/usr/bin/perl

my %waiting;  # context => what where what is "inode ..." or "dir ..."
my %hist;     # context => history since waited

while (<>) {
	if (/add_waiter/) {
		my ($c,$what) = /(0x\w+) on (.*\])/;
#		print "add_waiter $c $what\n";
		$waiting{$c} = $what;
		$hist{$c} .= $_;
	}
	if (/take_waiting/) {
		if (/SKIPPING/) {
			my ($c) = /SKIPPING (0x\w+)/;
			$hist{$c} .= $_;
		} elsif (/took/) {
			my ($c) = /took (0x\w+)/;
			delete $waiting{$c};
			delete $hist{$c};
		} else {
			die "i don't understand: $_";
		}
	}
}

for my $c (keys %waiting) {
	print "---- lost waiter $c $waiting{$c}
$hist{$c}

";
}
