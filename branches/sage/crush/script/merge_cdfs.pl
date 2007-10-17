#!/usr/bin/perl

my %rows;  # val -> [ count1, count2, ... ]

my $filen = 0;
for my $file (@ARGV) {
    open(I,"$file");
    while (<I>) {
	next if /^\#/;
	chomp;
	my ($v, $c) = split(/\t/,$_);
	$rows{$v}->[$filen] = $c;
    }
    $filen++;
}

for my $v (sort {$a <=> $b} keys %rows) {
    print "$v";
    for (my $i=0; $i < $filen; $i++) {
	print "\t" . int($rows{$v}->[$i]);
    }
    print "\n";
    #print join("\t", $v, @{$rows{$v}}) . "\n";
}
