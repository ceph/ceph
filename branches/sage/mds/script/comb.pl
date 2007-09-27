#!/usr/bin/perl

use strict;

my $xaxis = shift @ARGV;
my @vars;
while (@ARGV) {
	$_ = shift @ARGV;
	last if ($_ eq '-');
	push(@vars, $_);
}
my @dirs;
while (@ARGV) {
	$_ = shift @ARGV;
	last if ($_ eq '-');
	push(@dirs, $_) if -d $_;
}
my @filt = @ARGV;
push( @filt, '.' ) unless @filt;

print "#xaxis $xaxis
#vars @vars
#dirs @dirs
#filt @filt
";

sub load_sum {
	my $fn = shift @_;

	open(I, "$fn");
	my $k = <I>;
	chomp($k);
	my @k = split(/\s+/,$k);
	shift @k;

	my $s;
	while (<I>) {
		chomp;
		s/^\#//;
		next unless $_;
		my @l = split(/\s+/,$_);
		my $k = shift @l;
		for my $f (@k) {
			$s->{$k}->{$f} = shift @l;
		}

		# clnode latency?
		if ($fn =~ /cl/) {
			$s->{$k}->{'wrlat'} = $s->{$k}->{'wrlsum'} / $s->{$k}->{'wrlnum'} if $s->{$k}->{'wrlnum'} > 0;
			$s->{$k}->{'rlat'} = $s->{$k}->{'rlsum'} / $s->{$k}->{'rlnum'} if $s->{$k}->{'rlnum'} > 0;
			$s->{$k}->{'lat'} = $s->{$k}->{'lsum'} / $s->{$k}->{'lnum'} if $s->{$k}->{'lnum'} > 0;
			$s->{$k}->{'latw'} = $s->{$k}->{'lwsum'} / $s->{$k}->{'lwnum'} if $s->{$k}->{'lwnum'} > 0;
			$s->{$k}->{'latr'} = $s->{$k}->{'lrsum'} / $s->{$k}->{'lrnum'} if $s->{$k}->{'lrnum'} > 0;
			$s->{$k}->{'statlat'} = $s->{$k}->{'lstatsum'} / $s->{$k}->{'lstatnum'} if $s->{$k}->{'lstatnum'} > 0;
			$s->{$k}->{'dirlat'} = $s->{$k}->{'ldirsum'} / $s->{$k}->{'ldirnum'} if $s->{$k}->{'ldirnum'} > 0;
		}
	}		
	return $s;
}


my %res;
my @key;
my %didkey;
for my $f (@filt) {
	my @reg = split(/,/, $f);
	#print "reg @reg\n";
   	for my $d (@dirs) {
		if ($f ne '.') {
			my $r = (split(/\//,$d))[-1];
			my @db = split(/,/, $r);
			#print "db @db\n";
			my $ok = 1;
			for my $r (@reg) {
				
				$ok = 0 unless grep {$_ eq $r} @db;
			}
			next unless $ok;
		}
		#next if ($f ne '.' && $d !~ /$reg/);			
		#print "$d\n";
		my ($x) = $d =~ /$xaxis=([\d\.]+)/;
		
		for my $v (@vars) {
			my ($what, $field) = $v =~ /^(.+)\.([^\.]+)$/;
			#print "$what $field .. $v  .. $f.$field\n";
			my $s = &load_sum("$d/sum.$what");
			
			#print "\t$v";
			if ($field =~ /^sum=/) {
				#warn "SUM field $field\n";
				push( @{$res{$x}}, $s->{'sum'}->{$'} ); #'});
			} else {
				#warn "avg field $field\n";
				push( @{$res{$x}}, $s->{'avgval'}->{$field} );
			}

			push( @key, "$f.$field" ) unless $didkey{"$f.$field"};
			$didkey{"$f.$field"} = 1;

			if (0 && exists $s->{'avgvaldevt'}) {
				push( @{$res{$x}}, $s->{'avgvaldevt'}->{$field} );
				push( @key, "$f.$field.dev" ) unless $didkey{"$f.$field.dev"};
				$didkey{"$f.$field.dev"} = 1;
			}
		}
	}
}

print join("\t", "#", @key) . "\n";
for my $x (sort {$a <=> $b} keys %res) {
	print join("\t", $x, @{$res{$x}}) . "\n";
}
