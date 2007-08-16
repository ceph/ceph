#!/usr/bin/perl

use strict;

my $base = shift @ARGV;
my $dsfn = shift @ARGV;
my $logfn = shift @ARGV;

die unless -d $base;

my @q = ($base);
my $nfiles = 0;
my $ndirs = 0;
my $nreg = 0;
my $nhardlinks = 0;
my %nlinks;
my %names;
my %dirsize;

my $mask = 00170000;
my $ifdir = 0040000;
my $ifreg = 0100000;

while (@q) {
    my $dir = shift @q;
    unless (-d $dir) {
	warn "** $dir went away\n";
	next;
    }

    my $numindir = 0;
    opendir(D, $dir);
    for my $f (readdir(D)) {
	next if $f eq '.';
	next if $f eq '..';
	next if $f eq '.snapshot';
	$numindir++;
	my $file = "$dir/$f";

	$nfiles++; 
	my ($ino, $mode, $nlink) = (lstat($file))[1, 2,3];

	if (($mode & $mask) == $ifdir) {
	    $ndirs++;
	    push(@q, $file);
	} else {
	    $nreg++ if ($mode & $mask) == $ifreg;
	    if ($nlink > 1) {
		#system "ls -aldi $file";
		$nhardlinks++;
		$nlinks{$nlink}++;
		push(@{$names{$ino}->{$dir}}, $file);
	    }
	}
    }    
    closedir(D);
    $dirsize{$numindir}++;
}

my $nsamedir = 0;
open(LOG, ">>$logfn") if $logfn;
for my $ino (keys %names) {
    print LOG "#$base : $ino\n";
    my @dirs = keys %{$names{$ino}};
    my $insamedir = 1 if scalar(@dirs) == 1;
    for my $dir (@dirs) {
	print LOG "#\t$dir\n";
	for my $fn (@{$names{$ino}->{$dir}}) {
	    print LOG "#\t\t$fn\n";
	    $nsamedir++ if $insamedir;
	}
    }
}
close LOG;

# avg, medium dir size
my $avgdirsize = sprintf("%.2f",($nfiles + $ndirs) / $ndirs);
my $p = 0;
my $mediandirsize = 0;
open(DSLOG, ">$dsfn");
print DSLOG "# $base\n# size\tnumdirs\n";

for my $ds (sort {$a <=> $b} keys %dirsize) {
    print DSLOG "$ds\t$dirsize{$ds}\n";
    $p += $dirsize{$ds};
    if ($mediandirsize == undef &&
	$p >= ($ndirs/2)) {
	$mediandirsize = $ds;
    }
}
close DSLOG;

# stat fs
my $df = `df $base`;
my $line = (split(/\n/,$df))[1]; # second line
my ($kb) = $df =~ /\s+\d+\s+(\d+)/;
my $gb = sprintf("%.1f",($kb / 1024 / 1024));

# final line
my $pad = '# ' . (' ' x (length($base)-2));
print "$pad\tgb\tfiles\tdirs\tavgd\tmedd\treg\tnl>1\tsmdr\tnlink=2\t=3\t=4\t...\n";
print "$base\t$gb\t$nfiles\t$ndirs\t$avgdirsize\t$mediandirsize\t$nreg\t$nhardlinks\t$nsamedir";
my $i = 2;
for (sort {$a <=> $b} keys %nlinks) {
    while ($_ < $i) {
	print "\t0";
    }
    print "\t$nlinks{$_}";
    $i = $_ + 1;
}
print "\n";
