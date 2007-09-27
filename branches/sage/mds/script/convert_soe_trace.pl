#!/usr/bin/perl

# this reads in one of kristal's anonymized static traces from
# soe and makes it look like output from
#
#   find . -exec ls -dilsn --time-style=+%s \{\} \;
#
# (which is what SyntheticClient likes to "import", and
#  study_static.pl likes to analyze for hardlinks, dirsizes, etc.)

while (<>) {
    chomp;
    my ($file, $ino, $size, $actime, $ctime, $mtime, $uid, $gid, $omode, $nlink) = split(/ /,substr($_,1));
    $file = '.' . $file;
    my $nmode = oct($omode);
    my $mode = '-...';
    $mode = 'd...' if (($nmode & 0170000) == 0040000);
    $mode = 'f...' if (($nmode & 0170000) == 0100000);
    $size = hex($size);
    $mtime = hex($mtime);
    $uid = hex($uid);
    $gid = hex($gid);
    print "$ino ? $mode ? $nlink $uid $gid $size $mtime $file\n";
}

__END__

soe format is
0. a space
1. full path of file name (MD5-ed and in base 64)
2. inode number
3. size of file in bytes (hex)
4. atime (hex)
5. ctime (hex)
6. mtime (hex)
7. uid (hex)
8. gid (hex)
9. mode (octal)
10. number of links
