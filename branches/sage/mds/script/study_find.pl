#!/usr/bin/perl

use strict;

my $name = shift @ARGV;
my $dsfn = shift @ARGV;
my $fnlenfn = shift @ARGV;
my $logfn = shift @ARGV;

my $nfiles = 0;
my $ndirs = 0;
my $nreg = 0;
my $nhardlinks = 0;
my %nlinks;
my %names;
my %dirsize;
my %fnlen;  
my $nfnchars;

my $bytes;
my $ebytes;

#
# output generated with
#
#  find . -path ./.snapshot -prune -o -exec ls -dilsn --time-style=+%s \{\} \;
#
# find output looks like this:
#4495744 4 drwxrwxrwx  24 0 0 4096 1187290970 .
#2996320 8 drwxr-xr-x  189 0 1000 8192 1186594257 ./jangle
#28378499 4 drwxr-x--x  4 1068885 52673 4096 1162938122 ./jangle/cymcruise
#28378500 4 drwx--S---  5 1068885 52673 4096 1162938122 ./jangle/cymcruise/Maildir
#28378501 4 drwx------  2 1068885 52673 4096 1162938122 ./jangle/cymcruise/Maildir/tmp
#28378502 4 drwx------  2 1068885 52673 4096 1162938122 ./jangle/cymcruise/Maildir/new
#28378503 4 drwx------  2 1068885 52673 4096 1162938122 ./jangle/cymcruise/Maildir/cur
#28378504 4 -rw-r--r--  1 1068885 52673 260 943743700 ./jangle/cymcruise/.alias
#999425 4 drwxr-xr-x  92 1125 100 4096 1186523060 .
#999426 0 lrwxrwxrwx  1 0 0 5 1177701093 ./root -> /root
#1015809 4 drwxr-xr-x  4 1289 1000 4096 1174584949 ./andrea
#541007 4 drwxr-xr-x  3 0 0 4096 1173111449 ./andrea/lux
#5014055 4 drwx--S---  11 70228 51207 4096 1172250346 ./andrea/lux/Maildir

# dirs we're currently counting in
my %numindir;

sub finish_dir {
    my $curdir = shift @_;
    #print "finish_dir $curdir\n";
    $dirsize{$numindir{$curdir}}++;
    delete $numindir{$curdir};
}

my $curdir;
$ndirs = 1;
while (<>) {
    #print;
    chomp;
    my ($ino, $blah, $mode, $blah, $nlink, $uid, $gid, $size, $mtime, @path) = split(/ /,$_);
    my $file = join(' ',@path);
    ($file) = split(/ \-\> /, $file);
    my @bits = split(/\//, $file);
    my $f = pop @bits;
    my $dir = join('/', @bits);
    #print "file = '$file', dir = '$dir', curdir = '$curdir'\n";

    if ($dir ne $curdir) {
	for my $d (keys %numindir) {
	    #print "? $d vs $dir\n";
	    &finish_dir($d) if ($d ne substr($dir, 0, length($d)));
	}
	$curdir = $dir;
    }

    my $esize = 0;
    $esize = int (($size-1)/4096)*4096 + 4096 if $size > 0;
    $esize += 160;  # for the inode?
    $bytes += $size;
    $ebytes += $esize;

    $nfiles++; 
    $numindir{$dir}++;

    my $fnlen = length($f);
    $fnlen{$fnlen}++;
    $nfnchars += $fnlen;
    
    if ($mode =~ /^d/) {
	$ndirs++;
	# find does depth-first search, so assume we descend, so that on empty dir we "back out" above and &finish_dir.
	$numindir{$file} = 0;
	$curdir = $file;
    } else {
	$nreg++ if $mode =~ /^f/;
	if ($nlink > 1) {
	    #system "ls -aldi $file";
	    $nhardlinks++;
	    $nlinks{$nlink}++;
	    push(@{$names{$ino}->{$dir}}, $file);
	}
    }
}
for my $d (keys %numindir) {
    &finish_dir($d);
}



my $nsamedir = 0;
open(LOG, ">$logfn") if $logfn;
for my $ino (keys %names) {
    print LOG "# $ino\n";
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

sub do_cdf {
    my $hash = shift @_;
    my $num = shift @_;
    my $sum = shift @_;
    my $fn = shift @_;

    open(CDF, ">$fn") if $fn;
    print CDF "# $name\n# val\tnum\n";

    my $median;
    my $p = 0;
    for my $v (sort {$a <=> $b} keys %$hash) {
	print CDF "$v\t$hash->{$v}\n";
	$p += $hash->{$v} * $v;
	if (!(defined $median) &&
	    $p >= ($sum/2)) {
	    $median = $v;
	}
    }
    my $avg = sprintf("%.2f", $sum/$num);
    print CDF "# avg $avg, median $median, sum $sum, num $num\n";
    return ($avg, $median);
}
close DSLOG;


my ($avgdirsize, $mediandirsize) = &do_cdf(\%dirsize, $ndirs, $nfiles, $dsfn);
my ($avgfnlen, $medianfnlen) = &do_cdf(\%fnlen, $nfiles, $nfnchars, $fnlenfn);


# stat fs
#my $df = `df $base`;
#my $line = (split(/\n/,$df))[1]; # second line
#my ($kb) = $df =~ /\s+\d+\s+(\d+)/;
my $gb = sprintf("%.1f",($ebytes / 1024 / 1024 / 1024));

# final line
my $pad = '# ' . (' ' x (length($name)-2));
print "$pad\tgb\tfiles\tdirs\tdsavg\tdsmed\tfnavg\tfnmed\treg\tnl>1\tsmdr\tnlink=2\t=3\t=4\t...\n";
print "$name\t$gb\t$nfiles\t$ndirs\t$avgdirsize\t$mediandirsize\t$avgfnlen\t$medianfnlen\t$nreg\t$nhardlinks\t$nsamedir";
my $i = 2;
for (sort {$a <=> $b} keys %nlinks) {
    while ($_ < $i) {
	print "\t0";
    }
    print "\t$nlinks{$_}";
    $i = $_ + 1;
}
print "\n";
