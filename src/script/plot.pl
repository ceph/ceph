#!/usr/bin/perl

use strict;

my $dir = shift @ARGV;
my ($type,$subtype) = split(/\./, shift @ARGV);
$subtype = '.' . $subtype if $subtype;

# list files
my @files;
my %fields;
for my $f (`ls $dir/$type*$subtype`) {
    chomp $f;
    next unless $f =~ /$type(\d+)$subtype$/;
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
my $var = shift @ARGV;
my $rest = join(' ', @ARGV);

print "set style data lines\nset grid\n";
print "set title \"$dir .. $var\"\n";
if (scalar(@files) > 30) { print "set key off\n"; }
#for my $var (@ARGV) {
    my @p;
    for my $f (@files) {
	my ($lastbit) = $f =~ /\/([^\/]+)$/;
	push(@p, "\"$f\" u 1:$fields{$var} $rest t \"$lastbit\"");
    } 
    print "plot " . join(',', @p) . "\n";
#}
#print "pause 60000\n";
