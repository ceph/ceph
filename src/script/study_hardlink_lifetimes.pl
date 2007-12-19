#!/usr/bin/perl

use strict;

my %ns;     # parent -> fn -> ino
my %nlink;  # num links to each ino
my %since;  # when it got its second link

my @ignore = ('ll_getattr','ll_setattr','ll_forget','ll_fsync','ll_readlink','ll_statfs','ll_opendir','ll_releasedir','ll_flush','ll_release','ll_open','ll_read','ll_write');

my $when;

my $sumage;
my $numage;

sub unlink {
    my ($p,$n) = @_;
    my $i = $ns{$p}->{$n};
    my $new = --$nlink{$i};
    if ($new == 1) {
	my $age = $when - $since{$i};
	#print "$since{$i} to $when on $i\t$age\n";
	delete $since{$i};

	$numage++;
	$sumage += $age;

    } elsif ($new == 0) {
	delete $nlink{$i};
    }
    delete $ns{$p}->{$n};	
}


my ($sec, $usec, $cmd);
$_ = <>;
while (1) {
    # read trace record
    chomp;
    last unless $_ eq '@';

    chomp(my $sec = <>);
    chomp(my $usec = <>);
    $when = sprintf("%d.%06d",$sec,$usec);# + ($usec / 1000000);
    #$when = "$sec.$usec";

    chomp($cmd = <>);

    #print "cmd $cmd\n";

    if ($cmd eq 'll_lookup') {
	chomp(my $p = <>);
	chomp(my $n = <>);
	chomp(my $r = <>);
	$ns{$p}->{$n} = $r;
    }

    elsif ($cmd eq 'll_create') {
	chomp(my $p = <>);
	chomp(my $n = <>);
	<>; <>; <>; 
	chomp(my $r = <>);
	$ns{$p}->{$n} = $r;
	$nlink{$r} = 1;
    }
    elsif ($cmd eq 'll_mknod') {
	chomp(my $p = <>);
	chomp(my $n = <>);
	<>; <>; 
	chomp(my $r = <>);
	$ns{$p}->{$n} = $r;
	$nlink{$r} = 1;
    }
    elsif ($cmd eq 'll_mkdir') {
	chomp(my $p = <>);
	chomp(my $n = <>);
	<>;
	chomp(my $r = <>);
	$ns{$p}->{$n} = $r;
	$nlink{$r} = 1;
    }
    elsif ($cmd eq 'll_symlink') {
	chomp(my $p = <>);
	chomp(my $n = <>);
	<>;
	chomp(my $r = <>);
	$ns{$p}->{$n} = $r;
	$nlink{$r} = 1;
    }
    elsif ($cmd eq 'll_link') {
	chomp(my $i = <>);
   	chomp(my $p = <>);
	chomp(my $n = <>);
	$ns{$p}->{$n} = $i;
	if (++$nlink{$i} == 2) {
	    $since{$i} = $when;
	}
    }
    elsif ($cmd eq 'll_unlink' ||
	   $cmd eq 'll_rmdir') {
   	chomp(my $p = <>);
	chomp(my $n = <>);
	&unlink($p, $n);
    }
    elsif ($cmd eq 'll_rename') {
   	chomp(my $p = <>);
	chomp(my $n = <>);
   	chomp(my $np = <>);
	chomp(my $nn = <>);
	if ($ns{$np}->{$nn}) {
	    &unlink($np, $nn);
	}
	$ns{$np}->{$nn} = $ns{$p}->{$n};
	delete $ns{$p}->{$n};	
    }
    
    # skip to @
    while (<>) {
	last if $_ eq "@\n";
	print "$cmd: $_"
	    unless grep {$_ eq $cmd} @ignore;
    }
}

print "num $numage .. sum $sumage .. avg lifetime " . ($sumage / $numage) . "\n";

# dump hard link inos
for my $ino (keys %nlink) {
    next if $nlink{$ino} < 2;
    print "$ino\t$nlink{$ino}\n";
}
