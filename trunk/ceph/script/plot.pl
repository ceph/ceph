#!/usr/bin/perl

use strict;

my $dir = shift @ARGV;
my $type = shift @ARGV;

# list files
my @files;
my %fields;
for my $f (`ls $dir/$type*`) {
    chomp $f;
    next unless $f =~ /$type(\d+)$/;
    push(@files, $f);
    unless (%fields) {
	open(I,$f);
	while (<I>) {
	    next unless /^\#/;
	    my @f = split(/\t/,$_);
	    for (my $n=1; @f; $n++) {
		my $f = shift @f;
		$fields{$f} = $n;
		#print "$f = $n\n";
	    }
	    last;
	}
	close I;
    }
}
#print "#files @files\n";

# get field names
print "set style data lines\n";
print "set title \"$dir\"\n";
my $var = shift @ARGV;
my $rest = join(' ', @ARGV);
#for my $var (@ARGV) {
    my @p;
    for my $f (@files) {
	my ($lastbit) = $f =~ /\/([^\/]+)$/;
	push(@p, "\"$f\" u 1:$fields{$var} $rest t \"$lastbit\"");
    } 
    print "plot " . join(',', @p) . "\n";
#}
print "pause 100000000\n";
