#!/usr/bin/perl

my %sum;  # time -> name -> val
my %col;  # colnum -> name   .. colnums start at 0 (time doesn't count)
my %min;
my %max;
my %avg;
while (<>) {
	chomp;
	my @r = split(/\s+/,$_);
	my $r = shift @r;
	
	# column headings?
	if ($r eq '#') {
		my $num = 0;
		while (my $name = shift @r) {
			$col{$num} = $name;
			$num++;
		}
		next;
	}

	next unless int $r;
	#print "$r: @r\n";
	my $i = 0;
	while (@r) {
		my $v = shift @r;
		$sum{$r}->{$col{$i}} += $v; # if $v > 0;

		$min{$col{$i}} = $v
			if ($min{$col{$i}} > $v || !(defined $min{$col{$i}}));
		$max{$col{$i}} = $v 
			if ($max{$col{$i}} < $v);

		$avg{$col{$i}} += $v;
		$i++;
	}
}

## dump
my @c = sort {$a <=> $b} keys %col;
# cols
print join("\t",'#', map { $col{$_} } @c) . "\n";
my $n = 0;
for my $k (sort {$a <=> $b} keys %sum) {
	print join("\t",$k, map { $sum{$k}->{$col{$_}} } @c ) . "\n";
	$n++;
}


print "\n";
print join("\t", 'minval', map { $min{$col{$_}} } @c ) . "\n";
print join("\t", 'maxval', map { $max{$col{$_}} } @c ) . "\n";
print join("\t", 'avgsum', map int, map { $_ / $n } map { $avg{$col{$_}} } @c ) . "\n";
