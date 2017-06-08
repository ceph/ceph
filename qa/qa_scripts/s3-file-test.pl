#! /usr/bin/perl

=head1 NAME

s3-file-test.pl - Script to test the S3 functionality

=head1 SYNOPSIS

Use:
        perl s3-file-test.pl <test_id> [--help]

Examples:
        perl s3-file-test.pl <1|2|3|4|5>
        or
        perl s3-file-test.pl  --help

=head1 DESCRIPTION

This script intends to test the S3 functionality
and reports the test results

=head1 ARGUMENTS

s3-file-test.pl takes the following arguments:
   <test_id>
   (mandatory) Specify the test case ID. It should be one of these values: 1|2|3|4|5
   --help
   (optional) Displays the usage message.

=cut

use Amazon::S3;
use Data::Dumper;
use strict;
use IO::File;
use Getopt::Long;
use Digest::MD5;
use S3Lib qw(display_ceph_os_info get_timestamp get_hostname get_user_info $rgw_user delete_user _write_log_entry _exit_result get_status);
use Pod::Usage();

my ( $help, $tc );

Getopt::Long::GetOptions(
    'help' => \$help
);
my $tc = $ARGV[0];
Pod::Usage::pod2usage(-verbose => 1) && exit if (( @ARGV == 0 ) || ($help) );

#== local variables ===
my $exit_status = 0;
my $mytestfilename;
my $logmsg;
my $kruft;
my $s3;
my $domain   = "front.sepia.ceph.com";
my $host     = get_hostname();
my $hostname = "$host.$domain";
my $testfileloc;

# Execute the test case based on the input to the script
sub create_file {
    if ( $tc == 1 ) {
        system("dd if=/dev/zero of=/tmp/10MBfile bs=10485760 count=1");
        $mytestfilename = '10MBfile';
    }
    elsif ( $tc == 2 ) {
        system("dd if=/dev/zero of=/tmp/100MBfile bs=10485760 count=10");
        $mytestfilename = '100MBfile';
    }
    elsif ( $tc == 3 ) {
        system("dd if=/dev/zero of=/tmp/500MBfile bs=10485760 count=50");
        $mytestfilename = '500MBfile';
    }
    elsif ( $tc == 4 ) {
        system("dd if=/dev/zero of=/tmp/1GBfile bs=10485760 count=100");
        $mytestfilename = '1GBfile';
    }
    elsif ( $tc == 5 ) {
        system("dd if=/dev/zero of=/tmp/2GBfile bs=10485760 count=200");
        $mytestfilename = '2GBfile';
    } else {
        $exit_status = 1;
        _exit_result($exit_status);
    }
    return 0;
}

# Run tests for the S3 functionality
sub run_tests {
    # Modify access keys to suit the target account
    my ( $access_key, $secret_key ) = get_user_info();
    if ( ($access_key) && ($secret_key) ) {
        $s3 = Amazon::S3->new(
            {
                aws_access_key_id     => $access_key,
                aws_secret_access_key => $secret_key,
                host                  => $hostname,
                secure                => 0,
                retry                 => 1,
            }
        );
    }

    # List the existing buckets
    my $response = $s3->buckets;
    foreach my $bucket ( @{ $response->{buckets} } ) {
        print "You have a bucket: " . $bucket->bucket . "\n";
    }

    # create a new bucket (the test bucket)
    my $bucketname = 'kftestbucket' . get_timestamp();
    print "Attempting to create bucket = $bucketname\n";
    my $bucket = $s3->add_bucket( { bucket => $bucketname } )
      or die $s3->err . $s3->errstr;
    print "Bucket Created: $bucketname\n";
    $logmsg = "Bucket Created: $bucketname";
    _write_log_entry($logmsg);

    # upload a file to the bucket
    print "adding file to bucket: $mytestfilename\n";
    $bucket->add_key_filename( $mytestfilename, $testfileloc,
        { content_type => 'text/plain', },
    ) or die $s3->err . ": " . $s3->errstr;
    $logmsg = "$mytestfilename uploaded";
    _write_log_entry($logmsg);

    # store a value in the bucket
    $bucket->add_key( 'reminder.txt', 'this is text via add_key' )
      or die $s3->err . ": " . $s3->errstr;
    $logmsg = "Text value stored in file";
    _write_log_entry($logmsg);

    # list files in the bucket
    $response = $bucket->list_all
      or die $s3->err . ": " . $s3->errstr;
    foreach my $key ( @{ $response->{keys} } ) {
        my $key_name = $key->{key};
        my $key_size = $key->{size};
        print "Bucket contains key '$key_name' of size $key_size\n";
    }

    # fetch file from the bucket
    print
      "Downloading $mytestfilename to temp file: /tmp/downloadfilepurgeme...";
    $response =
      $bucket->get_key_filename( $mytestfilename, 'GET',
        '/tmp/downloadfilepurgeme' )
      or die $s3->err . ": " . $s3->errstr;
    $logmsg = "file downloaded";
    _write_log_entry($logmsg);

    # fetch value from the bucket
    $response = $bucket->get_key('reminder.txt')
      or die $s3->err . ": " . $s3->errstr;
    print "reminder.txt:\n";
    print "  content length: " . $response->{content_length} . "\n";
    print "    content type: " . $response->{content_type} . "\n";
    print "            etag: " . $response->{content_type} . "\n";
    print "         content: " . $response->{value} . "\n";

    # check the original file against the downloaded file to see if the file has been
    # corrupted.
    my $md5    = Digest::MD5->new;
    my $check  = 1;
    my $File   = $testfileloc;
    my $dlfile = "/tmp/downloadfilepurgeme";
    open( FILE, $File )
      or die "Error: Could not open $File for MD5 checksum...";
    open( DLFILE, $dlfile )
      or die "Error: Could not open $dlfile for MD5 checksum.";
    binmode(FILE);
    binmode(DLFILE);
    my $md5sum   = $md5->addfile(*FILE)->hexdigest;
    my $md5sumdl = $md5->addfile(*DLFILE)->hexdigest;
    close FILE;
    close DLFILE;

    print "\n";
    print "Finished MD5 Checksum for $File:\n";
    print "$md5sum\n";
    print "Finished MD5 Checksum for $dlfile:\n";
    print "$md5sumdl\n";
    print "\n";

    # compare the checksums
    if ( $md5sum eq $md5sumdl ) {
        print "Checksums are equal\n";
        $logmsg = "Checksums are equal";
        _write_log_entry($logmsg);
    }
    else {
        print "Checksums are not equal\n";
        $exit_status = 2;
        $logmsg      = "[Failure] Checksums are not equal";
        _write_log_entry($logmsg);

    }

    # Negative test: try deleting the bucket which still contains objects
    # the method should return false
    if ( !$bucket->delete_bucket ) {
        $logmsg = "Negative test - delete full bucket - Pass";
    }
    else {
        $logmsg      = " Negative test - delete full bucket - Fail";
        $exit_status = 3;
    }
    _write_log_entry($logmsg);

    # delete keys
    $bucket->delete_key('reminder.txt')  or die $s3->err . ": " . $s3->errstr;
    $bucket->delete_key($mytestfilename) or die $s3->err . ": " . $s3->errstr;
    $bucket->delete_key('bogusfile')     or die $s3->err . ": " . $s3->errstr;

    # and finally delete the bucket
    $bucket->delete_bucket or die $s3->err . ": " . $s3->errstr;
}

#== Main starts here===
display_ceph_os_info();

# check if service is already running
sub check
{
    my $state = get_status();
    if ($state) {
        _write_log_entry("radosgw is NOT running. quitting");
        $exit_status = 1;
        _exit_result($exit_status);
    }
}
check();
my $flag = create_file();
if (!$flag) {
    $testfileloc = "/tmp/" . $mytestfilename;
    print "Test file = $testfileloc\n";
    run_tests();
    delete_user();
}
_exit_result($exit_status);

