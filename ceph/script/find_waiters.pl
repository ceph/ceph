#!/usr/bin/perl

my %waiting;  # context => what where what is "inode ..." or "dir ..."
my %hist;     # context => history since waited
my @waiting;

my $line = 0;
while (<>) {
	$line++;
	if (/add_waiter/) {
		my ($c) = /(0x\w+)/;
		my ($what) = / on (.*\])/;
#		print "add_waiter $c $what\n";
		$waiting{$c} = $what
			if $what && !$waiting{$c};
		$hist{$c} .= "$line: $_";
		unless (grep {$_ eq $c} @waiting) {
			push( @waiting, $c );
		}
	}
	#if (/finish_waiting/) {
	#	my ($c) = /(0x\w+)/;
	#	$hist{$c} .= "$line: $_";
	#}
	if (/take_waiting/) {
		if (/SKIPPING/) {
			my ($c) = /SKIPPING (0x\w+)/;
			$hist{$c} .= "$line: $_";
		} elsif (/took/) {
			my ($c) = /took (0x\w+)/;
			delete $waiting{$c};
			delete $hist{$c};
			@waiting = grep {$_ ne $c} @waiting;
		} else {
			die "i don't understand: $_";
		}
	}
}

for my $c (@waiting) {
	print "---- lost waiter $c $waiting{$c}
$hist{$c}
";
}
