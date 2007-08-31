#!/usr/bin/perl

use strict;
use Data::Dumper;


my $usage = "script/runset.pl [--clean] jobs/some/job blah\n";

my $clean;
my $use_srun = 0;
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

my $comb = $sim->{'_comb'};
delete $sim->{'_comb'};
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
	#next if $k =~ /^_/;
	if (defined $fix->{$k}) {
	    $this->{$k} = $fix->{$k};
	}
	elsif (ref $sim->{$k} eq 'HASH') {
	    # nothing
	}
	elsif ($k =~ /^_/ || !(ref $sim->{$k})) {
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
	system "mkdir $fn/out" unless -d "$fn/out";

	my $e = './newsyn';
	#$e = './tcpsynobfs' if $h->{'fs'} eq 'obfs';
	my $c = "$e";
	$c .= " --mkfs" unless $h->{'_no_mkfs'};

	for my $k (keys %$h) {
	    next if $k =~ /^_/;
	    next if $h->{'_noarg'} && grep {$k eq $_} @{$h->{'_noarg'}};
	    next if $h->{'_subst'} && grep {$k eq $_} @{$h->{'_subst'}};
	    $c .= " --$k $h->{$k}";
	}

	if ($h->{'_custom'}) {
	    if ($h->{'_subst'}) {
		for my $var (@{$h->{'_subst'}}) {
		    $h->{'_custom'} =~ s/\$$var/$h->{$var}/g;
		}
	    }
	    $c .= ' ' . $h->{'_custom'};
	}

	$c .= " --log_name $relout/$keys";
	$c .= " --doutdir log/$relout/$keys/out";

	my $post = "#!/bin/sh
script/sum.pl -start $h->{'_start'} -end $h->{'_end'} $fn/osd\\* > $fn/sum.osd
script/sum.pl -start $h->{'_start'} -end $h->{'_end'} $fn/mds? $fn/mds?? > $fn/sum.mds
script/sum.pl -start $h->{'_start'} -end $h->{'_end'} $fn/mds*.log > $fn/sum.mds.log
script/sum.pl -start $h->{'_start'} -end $h->{'_end'} $fn/clnode* > $fn/sum.cl
touch $fn/.post
";
	open(O,">$fn/sh.post");
	print O $post;
	close O;

	my $killmin;
	if ($h->{'_kill_after'}) {
	    $killmin = 1 + int ($h->{'_kill_after'} / 60);
	    $killmin = "-t $killmin";
	}
	
	$c = "bash -c \"ulimit -c 0 ; $c\"";
	#$c = "bash -c \"$c\"";

	#print "h keys are " . join(' ', sort keys %$h) . "\n";

	my $srun = "srun --wait=600 -x jobs/ltest.ignore -l $killmin -N $h->{'_n'} -p ltest";
	my $mpiexec = "mpiexec -l -n $h->{'_n'}";
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
		$tp =~ s/\$NUM/$h->{'_n'}/g;
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
		print "sleep $h->{'_sleep'}\n";
		sleep $h->{'_sleep'};
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

