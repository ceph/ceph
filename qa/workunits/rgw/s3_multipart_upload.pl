#! /usr/bin/perl

=head1 NAME

s3_multipart_upload.pl - Script to test rgw multipart upload using s3 interface.

=head1 SYNOPSIS

Use:
        perl s3_multipart_upload.pl [--help]

Examples:
        perl s3_multipart_upload.pl
        or
        perl s3_multipart_upload.pl  --help

=head1 DESCRIPTION

This script intends to test the rgw multipart upload followed by a download
and verify checksum using s3 interface and reports test results

=head1 ARGUMENTS

s3_multipart_upload.pl takes the following arguments:
   --help
   (optional) Displays the usage message.

=cut

use Amazon::S3;
use Data::Dumper;
use IO::File;
use Getopt::Long;
use Digest::MD5;
use Pod::Usage();
use FindBin;
use lib $FindBin::Bin;
use s3_utilities;
use Net::Domain qw(hostfqdn);

my $help;

Getopt::Long::GetOptions(
    'help' => \$help
);
Pod::Usage::pod2usage(-verbose => 1) && exit if ($help);

#== local variables ===
my $s3;
my $hostdom  = $ENV{RGW_FQDN}||hostfqdn();
my $port     = $ENV{RGW_PORT}||7280;
our $hostname = "$hostdom:$port";
our $testfileloc;
our $mytestfilename;

# upload a file to the bucket
sub upload_file {
    my ($fsize, $i) = @_;
    create_file($fsize, $i);
    print "adding file to bucket $bucketname: $mytestfilename\n";
    ($bucket->add_key_filename( $mytestfilename, $testfileloc,
        { content_type => 'text/plain', },
    ) and (print "upload file successful\n" ) and return 0 ) or (print "upload failed\n" and return 1);
}

# delete the bucket
sub delete_bucket {
   ($bucket->delete_bucket) and (print "bucket delete succeeded \n") or die $s3->err . "delete bucket failed\n" . $s3->errstr;
}

# Function to perform multipart upload of given file size to the user bucket via s3 interface
sub multipart_upload
{
    my ($size, $parts) = @_;
    # generate random user every time
    my $user = rand();
    # Divide the file size in to equal parts and upload to bucket in multiple parts
    my $fsize = ($size/$parts);
    my $fsize1;
    run_s3($user);
    if ($parts == 10){
        $fsize1 = '100Mb';
    } elsif ($parts == 100){
        $fsize1 = '10Mb';
    }
    foreach my $i(1..$parts){
       print "uploading file - part $i \n";
       upload_file($fsize1, $i);
    }
    fetch_file_from_bucket($fsize1, $parts);
    compare_cksum($fsize1, $parts);
    purge_data($user);
}

# Function to download the files from bucket to verify there is no data corruption
sub fetch_file_from_bucket
{
    # fetch file from the bucket
    my ($fsize, $parts) = @_;
    foreach my $i(1..$parts){
    my $src_file = "$fsize.$i";
    my $dest_file = "/tmp/downloadfile.$i";
    print
      "Downloading $src_file from bucket to $dest_file \n";
    $response =
      $bucket->get_key_filename( $src_file, GET,
        $dest_file )
      or die $s3->err . ": " . $s3->errstr;
    }
}

# Compare the source file with destination file and verify checksum to ensure
# the files are not corrupted
sub compare_cksum
{
    my ($fsize, $parts)=@_;
    my $md5    = Digest::MD5->new;
    my $flag = 0;
    foreach my $i (1..$parts){
        my $src_file = "/tmp/"."$fsize".".$i";
        my $dest_file = "/tmp/downloadfile".".$i";
        open( FILE, $src_file )
         or die "Error: Could not open $src_file for MD5 checksum...";
        open( DLFILE, $dest_file )
         or die "Error: Could not open $dest_file for MD5 checksum.";
        binmode(FILE);
        binmode(DLFILE);
        my $md5sum   = $md5->addfile(*FILE)->hexdigest;
        my $md5sumdl = $md5->addfile(*DLFILE)->hexdigest;
        close FILE;
        close DLFILE;
        # compare the checksums
        if ( $md5sum eq $md5sumdl ) {
            $flag++;
        }
    }
    if ($flag == $parts){
       pass("checksum verification for multipart upload passed" );
    }else{
       fail("checksum verification for multipart upload failed" );
    }
}

#== Main starts here===
ceph_os_info();
check();
# The following test runs multi part upload of file size 1Gb in 10 parts
multipart_upload('1048576000', 10);
# The following test runs multipart upload of 1 Gb file in 100 parts
multipart_upload('1048576000', 100);
print "OK";
