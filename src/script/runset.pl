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
	'fs' => [ 'fakestore' ],
	'until' => 150,     # --syn until $n    ... when to stop clients
	'writefile' => 1,
	'writefile_size' => [ 4096, 65526, 256000, 1024000, 2560000 ],
	'writefile_mb' => 1000,

	'custom' => '--tcp_skip_rank0 --osd_maxthreads 0';

	# for final summation (script/sum.pl)
	'start' => 30,
	'end' => 120,

	'_psub' => 'alc.tp'   # switch to psub mode!
};

=cut

my $usage = "script/runset.pl [--clean] jobs/some/job blah\n";

my $clean;
my $use_srun;
my $nobg = '&';
my $in = shift || die $usage;
if ($in eq '--clean') {
	$clean = 1;
	$in = shift || die $usage;
}
if ($in eq '--srun') {
	$use_srun = 1;
	$in = shift || die $usage;
}
if ($in eq '--nobg') {
	$nobg = '';
	$in = shift || die $usage;
}
my $tag = shift || die $usage;
my $fake = shift;


my ($job) = $in =~ /^jobs\/(.*)/;
my ($jname) = $job =~ /\/(\w+)$/;
$jname ||= $job;
die "not jobs/?" unless defined $job;
my $out = "log/$job.$tag";
my $relout = "$job.$tag";


my $cwd = `/bin/pwd`;
chomp($cwd);



print "# --- job $job, tag $tag ---\n";


# get input
my $raw = `cat $in`;
my $sim = eval $raw;
unless (ref $sim) {
	print "bad input: $in\n";
	system "perl -c $in";
	exit 1;
}

# prep output
system "mkdir -p $out" unless -d "$out";

open(W, ">$out/in");
print W $raw;
close W;

my $comb = $sim->{'comb'};
delete $sim->{'comb'};
my %filters;
my @fulldirs;



sub reset {
	print "reset: restarting mpd in 3 seconds\n";
	system "sleep 3 && (mpiexec -l -n 32 killall newsyn ; restartmpd.sh)";
	print "reset: done\n";
}


if (`hostname` =~ /alc/ && !$use_srun) {
	print "# this looks like alc\n";
	$sim->{'_psub'} = 'jobs/alc.tp';
}


sub iterate {
	my $sim = shift @_;
	my $fix = shift @_ || {};
	my $vary;
	my @r;

	my $this;
	for my $k (sort keys %$sim) {
		next if $k =~ /^_/;
		if (defined $fix->{$k}) {
			$this->{$k} = $fix->{$k};
		}
		elsif (ref $sim->{$k} eq 'HASH') {
			# nothing
		}
		elsif (!(ref $sim->{$k})) {
			$this->{$k} = $sim->{$k};
		}
		else {
			#print ref $sim->{$k};
			if (!(defined $vary)) {
				$vary = $k;
			}
		}
	}

	if ($vary) {
		#print "vary $vary\n";
		for my $v (@{$sim->{$vary}}) {
			$this->{$vary} = $v;
			push(@r, &iterate($sim, $this));
		}
	} else {

		if ($sim->{'_dep'}) {
			my @s = @{$sim->{'_dep'}};
			while (@s) {
				my $dv = shift @s;
				my $eq = shift @s;

				$eq =~ s/\$(\w+)/"\$this->{'$1'}"/eg;
				$this->{$dv} = eval $eq;
				#print "$dv : $eq -> $this->{$dv}\n";
			}
		}

		push(@r, $this);
	}
	return @r;
}



sub run {
	my $h = shift @_;

	my @fn;
	my @filt;
	my @vals;
	for my $k (sort keys %$sim) {
		next if $k =~ /^_/;
		next unless ref $sim->{$k} eq 'ARRAY';
		push(@fn, "$k=$h->{$k}");
		push(@vals, $h->{$k});
		next if $comb && $k eq $comb->{'x'};
		push(@filt, "$k=$h->{$k}");
	}
	my $keys = join(",", @fn);
	$keys =~ s/ /_/g;
	my $fn = $out . '/' . $keys;
	my $name = $jname . '_' . join('_',@vals); #$tag . '_' . $keys;

	push( @fulldirs, "" . $fn );

	
	# filters
	$filters{ join(',', @filt) } = 1;


	#system "sh $fn/sh.post" if -e "$fn/sh.post";# && !(-e "$fn/.post");
	if (-e "$fn/.done") {
		print "already done.\n";
		return;
	}
	system "rm -r $fn" if $clean && -d "$fn";
	system "mkdir $fn" unless -d "$fn";

	my $e = './newsyn';
	#$e = './tcpsynobfs' if $h->{'fs'} eq 'obfs';
	my $c = "$e";
	$c .= " --mkfs" unless $h->{'no_mkfs'};
	$c .= " --$h->{'fs'}" if $h->{'fs'};
	$c .= " --syn until $h->{'until'}" if $h->{'until'};

	$c .= " --syn writefile $h->{'writefile_mb'} $h->{'writefile_size'}" if $h->{'writefile'};
	$c .= " --syn rw $h->{'rw_mb'} $h->{'rw_size'}" if $h->{'rw'};
	$c .= " --syn readfile $h->{'readfile_mb'} $h->{'readfile_size'}" if $h->{'readfile'};
	$c .= " --syn makedirs $h->{'makedirs_dirs'} $h->{'makedirs_files'} $h->{'makedirs_depth'}" if $h->{'makedirs'};

	for my $k ('nummds', 'numclient', 'numosd', 'kill_after',
			   'osd_maxthreads', 'osd_object_layout', 'osd_pg_layout','osd_pg_bits',
			   'mds_bal_rep', 'mds_bal_interval', 'mds_bal_max','mds_decay_halflife',
			   'mds_bal_hash_rd','mds_bal_hash_wr','mds_bal_unhash_rd','mds_bal_unhash_wr',
			   'mds_cache_size','mds_log_max_len',
			   'mds_local_osd',
			   'osd_age_time','osd_age',
			   'osd_rep',
			   'osd_pad_pg_log',
			   'osd_balance_reads',
			   'tcp_multi_out',
			   'client_cache_stat_ttl','client_cache_readdir_ttl',
			   'client_oc',
			   'fake_osdmap_updates',
			   'bdev_el_bidir',
			   'file_layout_ssize','file_layout_scount','file_layout_osize','file_layout_num_rep',
			   'meta_dir_layout_ssize','meta_dir_layout_scount','meta_dir_layout_osize','meta_dir_layout_num_rep',
			   'meta_log_layout_ssize','meta_log_layout_scount','meta_log_layout_osize','meta_log_layout_num_rep') {
		$c .= " --$k $h->{$k}" if defined $h->{$k};
	}

	$c .= ' ' . $h->{'custom'} if $h->{'custom'};

	$c .= " --log_name $relout/$keys";

	my $post = "#!/bin/sh
script/sum.pl -start $h->{'start'} -end $h->{'end'} $fn/osd\\* > $fn/sum.osd
script/sum.pl -start $h->{'start'} -end $h->{'end'} $fn/mds? $fn/mds?? > $fn/sum.mds
script/sum.pl -start $h->{'start'} -end $h->{'end'} $fn/mds*.log > $fn/sum.mds.log
script/sum.pl -start $h->{'start'} -end $h->{'end'} $fn/clnode* > $fn/sum.cl
touch $fn/.post
";
	open(O,">$fn/sh.post");
	print O $post;
	close O;

	my $killmin = 1 + int ($h->{'kill_after'} / 60);
	
	$c = "bash -c \"ulimit -c 0 ; $c\"";
	#$c = "bash -c \"$c\"";

	my $srun = "srun --wait=600 --exclude=jobs/ltest.ignore -l -t $killmin -N $h->{'n'} -p ltest";
	my $mpiexec = "mpiexec -l -n $h->{'n'}";
	my $launch;
	if ($use_srun)  {
		$launch = $srun;
	} else {
		$launch = $mpiexec;
	}
	
	if ($sim->{'_psub'}) {
		# template!
		my $tp = `cat $sim->{'_psub'}`;
		$tp =~ s/\$CWD/$cwd/g;
		$tp =~ s/\$NAME/$name/g;
		$tp =~ s/\$NUM/$h->{'n'}/g;
		$tp =~ s/\$OUT/$fn\/o/g;
		$tp =~ s/\$DONE/$fn\/.done/g;
		$tp =~ s/\$CMD/$c/g;
		open(O,">$out/$name");
		print O $tp;
		close O;
		print "\npsub $out/$name\n";
		return;
	} else {
		# run
		my $cmd = "\n$launch $c > $fn/o && touch $fn/.done";#
		#my $cmd = "\n$launch $c > $fn/o ; touch $fn/.done";
		print "$cmd $nobg\n";
		my $r = undef;
		unless ($fake) {
			if ($sim->{'_pre'}) {
				print "pre: $launch $sim->{'_pre'}\n";
				system "$launch $sim->{'_pre'}";
			}
			$r = system $cmd;
			if ($sim->{'_post'}) {
				print "post: $launch $sim->{'_post'}\n";
				system "$launch $sim->{'_post'}";
			}
			if ($r) {
				print "r = $r\n";
				#&reset;
			}
			system "sh $fn/sh.post";
		}
		return $r;
	}
}



my @r = &iterate($sim);
my $n = scalar(@r);
my $c = 1;
my %r;
my $nfailed = 0;
for my $h (@r) {
	my $d = `date`;
	chomp($d);
	$d =~ s/ P.T .*//;
	print "# === $c/$n";
	print " ($nfailed failed)" if $nfailed;
	print " $d: ";
	my $r = &run($h);

	if (!(defined $r)) {
		# already done
	} else {
		if ($r) {
			$nfailed++;
		}
		print "sleep $h->{'sleep'}\n";
		sleep $h->{'sleep'};
	}

	$c++;
}
print "$nfailed failed\n";


my @comb;
if ($comb) {
	my $x = $comb->{'x'};
	my @vars = @{$comb->{'vars'}};

	print "\n\n# post\n";
	for my $p (@fulldirs) {
		print "sh $p/sh.post\n";
	}

	my @filters = sort keys %filters;
	my $cmd = "script/comb.pl $x @vars - @fulldirs - @filters > $out/c";
	print "$cmd\n";
	open(O,">$out/comb");
	print O "$cmd\n";
	close O;
	system $cmd;

	print "\n\n";

	my $plot;
	$plot .= "set data style linespoints;\n";
	my $s = 2;
	for my $v (@vars) {
		my $c = $s;
		$s++;
		my @p;
		for my $f (@filters) {
			my $t = $f;
			if ($comb->{'maptitle'}) {
				for my $a (keys %{$comb->{'maptitle'}}) {
					my $b = $comb->{'maptitle'}->{$a};
					$t =~ s/$a/$b/;
				}
			}
			push (@p, "\"$out/c\" u 1:$c t \"$t\"" );
			$c += scalar(@vars);
		}
		$plot .= "# $v\nplot " . join(", ", @p) . ";\n\n";
	}
	print $plot;
	open(O,">$out/plot");
	print O $plot;
	close O;
}

