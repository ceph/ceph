#!/usr/bin/perl

my %waiting;  # context => what where what is "inode ..." or "dir ..."
my %hist;     # context => history since waited
my @waiting;

my $line = 0;
while (<>) {
	#print $line . $_ if /0x8d4f6a0/;
	$line++;
	if (/add_waiter/) {
		my ($c) = /(0x\w+)/;
		my ($what) = / on (.*\])/;
		#print "$line add_waiter $c $what\n" if /0x8d4f6a0/;
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
		my ($c) = /(0x\w+)/;
		if (/SKIPPING/) {
			#print "skipping\n" if /0x8d4f6a0/;
			$hist{$c} .= "$line: $_";
		} elsif (/took/) {
			#print "took\n" if /0x8d4f6a0/;
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
