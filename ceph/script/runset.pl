#!/usr/bin/perl

use strict;
use Data::Dumper;

=item sample input file

# hi there
{
	'n' => 30,     
	'nummds' => 1,
	'numosd' => 8,
	'numclient' => 400,#[10, 50, 100, 200, 400],
	'fs' => [ 'ebofs', 'fakestore' ],
	'until' => 150,
	'writefile' => 1,
	'writefile_size' => [ 4096, 65526, 256000, 1024000, 2560000 ],
	'writefile_mb' => 1000,
	'start' => 30,
	'end' => 120
};

=cut


my $in = shift || die;
my $out = shift || die;

die "$out exists" if -d $out;

my $raw = `cat $in`;
my $sim = eval $raw;
die "bash input" unless ref $sim;



sub iterate {
	my $sim = shift @_;
	my $fix = shift @_ || {};
	my $vary;
	my @r;

	my $this;
	for my $k (keys %$sim) {
		if ($fix->{$k}) {
			$this->{$k} = $fix->{$k};
		}
		elsif (!(ref $sim->{$k})) {
			$this->{$k} = $sim->{$k};
		}
		else {
			if (!(defined $vary)) {
				$vary = $k;
			}
		}
	}

	if ($vary) {
		for my $v (@{$sim->{$vary}}) {
			$this->{$vary} = $v;
			push(@r, &iterate($sim, $this));
		}
	} else {
		push(@r, $this);
	}
	return @r;
}

sub run {
	my $h = shift @_;

	my $fn = join(",", map {"$_=$h->{$_}"} sort keys %$h);
	$fn = $out . '/' . $fn if $out;

	my $c = "mpiexec -l -n $h->{'n'} ./tcpsyn --mkfs --nummds $h->{'nummds'} --numclient $h->{'numclient'} --numosd $h->{'numosd'}";
	$c .= " --$h->{'fs'}";

	$c .= " --syn until $h->{'until'}" if $h->{'until'};
	$c .= " --syn writefile $h->{'writefile_mb'} $h->{'writefile_size'}" if $h->{'writefile'};
	$c .= " --log $fn";
	
	print "-> $c\n";
	#system "$c > o";

}


my @r = &iterate($sim);
my $n = scalar(@r);
my $c = 1;
for my $h (@r) {
	print "$c/$n: ";
	&run($h);
	$c++;
}

