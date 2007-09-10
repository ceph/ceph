#!/usr/bin/perl

my $n = shift @ARGV || 2;

my %v;  # t -> [..]
while (<>) {
    chomp;
    my @l = split(/\t/,$_);
    my $t = shift @l;
    if (int $t) {
	$v{$t} = \@l;
    } else {
	print "$_\n";
    }
}

for my $t (sort {$a <=> $b} keys %v) {
    my $s = $t - $n/2;
    my @v;
    my $c = 0;
    for (my $a=0; $a < $n; $a++) {
	my $x = $t + $a;
	next unless ($v{$x});
	my @o = @{$v{$x}};
	#print "$t: $x o @o\n";
	if (@v) {
	    for (my $y=0; $y<=$#o; $y++) {
		$v[$y] += $o[$y];
	    }
	} else {
	    @v = @o;
	}
	#print "$t: $x v @v\n";
	$c++;
    }
    print "$t";
    for my $sum (@v) {
	print "\t" . ($sum / $c);
    }
    print "\n";    
}
