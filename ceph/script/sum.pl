#!/usr/bin/perl

use strict;
my $starttime = 1;
my $endtime = -1;

my $avgrows = 0;

while ($ARGV[0] =~ /^-/) {
	$_ = shift @ARGV;
	if ($_ eq '-avg') {
		$avgrows = 1;
	}
	elsif ($_ eq '-start') {
		$starttime = shift @ARGV;
	}
	elsif ($_ eq '-end') {
		$endtime = shift @ARGV;
	}
	else {
		die "i don't understand arg $_";
	}
}
my @files = @ARGV;

my @data;
for my $f (@files) {
	open(I,$f);
	push( @data, <I> );
	close I;
}

my %sum;  # time -> name -> val
my %col;  # colnum -> name   .. colnums start at 0 (time doesn't count)
my %min;
my %max;
my %avg;
my %tcount;
for (@data) {
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
	next if $r < $starttime;
	next if $endtime > 0 && $r > $endtime;

	$tcount{$r}++;
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
	if ($avgrows) {
		print join("\t",$k, map int, map { $sum{$k}->{$col{$_}}/$tcount{$k} } @c ) . "\n";
	} else {
		print join("\t",$k, map { $sum{$k}->{$col{$_}} } @c ) . "\n";
	}
	$n++;
}

my $rows = $n;
my $files = $tcount{$starttime};

print "\n";
print join("\t", 'minval', map { $min{$col{$_}} } @c ) . "\n";
print join("\t", 'maxval', map { $max{$col{$_}} } @c ) . "\n";
print join("\t", 'rows', map { $rows } @c) . "\n";
print join("\t", 'files', map { $files } @c) . "\n";
print join("\t", 'avgval', map int, map { $_ / ($rows*$files) } map { $avg{$col{$_}} } @c ) . "\n";
print join("\t", 'avgsum', map int, map { $_ / $rows } map { $avg{$col{$_}} } @c ) . "\n";
