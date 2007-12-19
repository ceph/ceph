#!/usr/bin/perl

my %pin;
my %hist;     
my $l = 1;
my @pins;
while (<>) {

	#cdir:adjust_nested_auth_pins on [dir 163 /foo/ rep@13 | child] count now 0 + 1

	if (/adjust_nested_auth_pins/) {
		my ($what) = / (\w+)\]/;
		$what =~ s/ 0x/ /;
		$hist{$what} .= "$l: $_"
			if defined $pin{$what};
	}

	# cinode:auth_pin on inode [1000000002625 /gnu/blah_client_created. 0x89b7700] count now 1 + 0

	elsif (/auth_pin / && !/waiting/) {
		#my ($what) = /\[(\w+ \w+) /;
		my ($what) = / (\w+)\]/;
		$what =~ s/ 0x/ /;
		#print "$_ add_waiter $c $what\n";
		$pin{$what}++;
		$hist{$what} .= "$l: $_";
		push( @pins, $what ) unless grep {$_ eq $what} @pins;
	}

	# cinode:auth_unpin on inode [1000000002625 (dangling) 0x89b7700] count now 0 + 0

	elsif (/auth_unpin/) {
		#my ($what) = /\[(\w+ \w+) /;# / on (.*\])/;
		my ($what) = / (\w+)\]/;
		$what =~ s/ 0x/ /;
		$pin{$what}--;
		$hist{$what} .= "$l: $_";
		unless ($pin{$what}) {
			delete $hist{$what};
			delete $pin{$what};
			@pins = grep {$_ ne $what} @pins;
		}
	}
	$l++;
}

for my $what (@pins) {
	print "---- count $pin{$what} on $what
$hist{$what}
";
}
