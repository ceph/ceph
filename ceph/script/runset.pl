#!/usr/bin/perl

use strict;
use Data::Dumper;

=item sample input file

# hi there
{
	# startup
	'n' => 30,          # mpi nodes
	'sleep' => 10,      # seconds between runs
	'nummds' => 1,
	'numosd' => 8,
	'numclient' => 400,#[10, 50, 100, 200, 400],

	# parameters
	'fs' => [ 'ebofs', 'fakestore' ],
	'until' => 150,     # --syn until $n    ... when to stop clients
	'writefile' => 1,
	'writefile_size' => [ 4096, 65526, 256000, 1024000, 2560000 ],
	'writefile_mb' => 1000,

	'custom' => '--tcp_skip_rank0 --osd_maxthreads 0';

	# for final summation (script/sum.pl)
	'start' => 30,
	'end' => 120
};

=cut

my $in = shift || die;
my $out = shift || die;
$out = $in . "." . $out;
my $fake = shift;

print "in $in
out $out/
";


# get input
my $raw = `cat log/$in`;
my $sim = eval $raw;
unless (ref $sim) {
	print "bad input: log/$in\n";
	system "perl -c log/$in";
	exit 1;
}

open(W, "log/$out/in");
print W $raw;
close W;


# prep output
system "mkdir log/$out" unless -d "log/$out";


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

	my @fn;
	for my $k (keys %$sim) {
		next unless ref $sim->{$k};
		push(@fn, "$k=$h->{$k}");
	}
	my $fn = join(",", @fn);
	$fn =~ s/ /_/g;
	$fn = $out . '/' . $fn if $out;

	if (-e "log/$fn/.done") {
		print "already done.\n";
		return 1;
	}
	system "rm -r log/$fn" if -d "log/$fn";
	system "mkdir log/$fn" unless -d "log/$fn";

	my $e = './tcpsyn';
	$e = './tcpsynobfs' if $h->{'fs'} eq 'obfs';
	my $c = "mpiexec -l -n $h->{'n'} $e --mkfs --nummds $h->{'nummds'} --numclient $h->{'numclient'} --numosd $h->{'numosd'}";
	$c .= " --$h->{'fs'}";
	$c .= " --syn until $h->{'until'}" if $h->{'until'};
	$c .= " --syn writefile $h->{'writefile_mb'} $h->{'writefile_size'}" if $h->{'writefile'};
	$c .= ' ' . $h->{'custom'} if $h->{'custom'};
	$c .= " --log_name $fn";
	
	print "-> $c\n";
	my $r;
	unless ($fake) {
		$r = system "$c > log/$fn/o";
		system "script/sum.pl -start $h->{'start'} -end $h->{'end'} log/$fn/osd* > log/$fn/sum.osd";
		system "script/sum.pl -start $h->{'start'} -end $h->{'end'} log/$fn/mds* > log/$fn/sum.mds"
			if -e "log/$fn/mds1";
		system "script/sum.pl -start $h->{'start'} -end $h->{'end'} log/$fn/clnode* > log/$fn/sum.cl"
			if -e "log/$fn/clnode.1";
		if ($r) {
			print "r = $r\n";
		} else {
			system "touch log/$fn/.done";
		}
	}
	return $r;
}


my @r = &iterate($sim);
my $n = scalar(@r);
my $c = 1;
my %r;
my $nfailed = 0;
for my $h (@r) {
	print "$c/$n";
	print " ($nfailed failed)" if $nfailed;
	print ": ";
	my $r = &run($h);

	if ($r != 1) {
		print "sleep $h->{'sleep'}\n";
		sleep $h->{'sleep'};
	} elsif ($r == 1) {
		# already done
	} elsif ($r) {
		$nfailed++;
	}

	$c++;
}
print "$nfailed failed\n";

