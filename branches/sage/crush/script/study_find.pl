#!/usr/bin/perl

use strict;

my $name = shift @ARGV || die;

my $nfiles = 0;
my $ndirs = 0;
my $nreg = 0;
my $nhardlinks = 0;
my %nlinks;
my %ino_nlinks;
my %names;
my %dirsize;

my %fnlen;  

my %hdepth;

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
    #print "finish_dir $numindir{$curdir} in $curdir\n";
    $dirsize{$numindir{$curdir}}++;
    $ndirs++;
    delete $numindir{$curdir};
}

my $curdir;
while (<>) {
    #print;
    chomp;
    my ($ino, $blah, $mode, $nlink, $uid, $gid, $size, $mtime, @path) = split(/[ ]+/,$_);
    my $file = join(' ',@path);
    ($file) = split(/ \-\> /, $file); # ignore symlink dest
    my @bits = split(/\//, $file);
    my $depth = scalar(@bits);
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

    $hdepth{$depth}++;

    my $fnlen = length($f);
    $fnlen{$fnlen}++;
    
    if ($mode =~ /^d/) {
	# find does depth-first search, so assume we descend, so that on empty dir we "back out" above and &finish_dir.
	$numindir{$file} = 0;
	$curdir = $file;
    } else {
	$nreg++ if $mode =~ /^f/;
	if ($nlink > 1) {
	    #system "ls -aldi $file";
	    $nhardlinks++;
	    $nlinks{$nlink}++;
	    $ino_nlinks{$ino} = $nlink;
	    push(@{$names{$ino}->{$dir}}, $file);
	}
    }
}
for my $d (keys %numindir) {
    &finish_dir($d);
}



my $nsamedir = 0;
open(LOG, ">$name.log");
my %dirmap;  # from dir -> to dir
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

    # stick in dirmap
    for (my $i=0; $i<$#dirs; $i++) {
	for (my $j=1; $j <= $#dirs; $j++) {
	    print LOG "# $dirs[$i] <-> $dirs[$j]\n";
	    push(@{$dirmap{$dirs[$i]}->{$dirs[$j]}}, $ino);
	    push(@{$dirmap{$dirs[$j]}->{$dirs[$i]}}, $ino);
	}
    }
}


my $notherinsamedir = 0;
my $notherinsamedirs = 0;
for my $ino (keys %names) {
    my @dirs = keys %{$names{$ino}};
    next unless (scalar(@dirs) > 1);
    my $n = 0;
    my $np = 0;	
    for (my $i=0; $i<$#dirs; $i++) {
	for (my $j=$i+1; $j <= $#dirs; $j++) {
	    $np++;
	    if (scalar(@{$dirmap{$dirs[$i]}->{$dirs[$j]}}) > 1 ||
		scalar(@{$dirmap{$dirs[$j]}->{$dirs[$i]}}) > 1) {
		$n++;
		#print LOG "# $ino is not alone between $dirs[$i] and $dirs[$j] : @{$dirmap{$dirs[$j]}->{$dirs[$i]}}\n";
	    }
	}
    }
    if ($n) {
	print LOG "# $ino\tfor $n / $np dir pairs, there is another hl between the same pair of dirs\n";
	$notherinsamedir += $ino_nlinks{$ino};
	$notherinsamedirs += ($n / $np) * $ino_nlinks{$ino};
    } else {
	print LOG "# $ino is ALL ALONE\n";
    }
}
close LOG;
$notherinsamedirs = sprintf("%.1f",$notherinsamedirs);


sub do_cdf {
    my $hash = shift @_;
    my $num = shift @_;
    my $fn = shift @_;

    open(CDF, ">$fn") if $fn;
    print CDF "# $name\n";

    my $median;
    my $sum = 0;
    my $p = 0;
    my $lastv = 0;
    for my $v (sort {$a <=> $b} keys %$hash) {
	print CDF "$v\t$hash->{$v}\n";
	$p += $hash->{$v};
	$sum += $hash->{$v} * $v;
	if (!(defined $median) &&
	    $p >= ($num/2)) {
	    $median = $v;
	}
    }
    if ($p != $num) {
	warn "uh oh, BUG, $p != $num in cdf/median calculation\n";
    }
    my $avg = sprintf("%.2f", $sum/$num);
    print CDF "# avg $avg, median $median, sum $sum, num $num\n";
    return ($avg, $median);
}
close DSLOG;


# do cdfs
my ($avgdirsize, $mediandirsize) = &do_cdf(\%dirsize, $ndirs, "$name.ds");
my ($avgfnlen, $medianfnlen) = &do_cdf(\%fnlen, $nfiles, "$name.fnlen");
my ($avgdepth, $mediandepth) = &do_cdf(\%hdepth, $nfiles, "$name.hdepth");


# stat fs
#my $df = `df $base`;
#my $line = (split(/\n/,$df))[1]; # second line
#my ($kb) = $df =~ /\s+\d+\s+(\d+)/;
my $gb = sprintf("%.1f",($ebytes / 1024 / 1024 / 1024));

open(O, ">$name.sum");

# final line
my $pad = '# ' . (' ' x (length($name)-2));
print O "$pad\tgb\tfiles\tdirs\tdsavg\tdsmed\tfnavg\tfnmed\treg\tnl>1\tsmdr\tothers\totherss\tnlink=2\t=3\t=4\t...\n";
print O "$name\t$gb\t$nfiles\t$ndirs\t$avgdirsize\t$mediandirsize\t$avgfnlen\t$medianfnlen\t$nreg\t$nhardlinks\t$nsamedir\t$notherinsamedir\t$notherinsamedirs";
my $i = 2;
for (sort {$a <=> $b} keys %nlinks) {
    while ($_ < $i) {
	print O "\t0";
    }
    print O "\t$nlinks{$_}";
    $i = $_ + 1;
}
print O "\n";

close O;
