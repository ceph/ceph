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

my $help;

Getopt::Long::GetOptions(
    'help' => \$help
);
Pod::Usage::pod2usage(-verbose => 1) && exit if ($help);

#== local variables ===
my $s3;
my $domain   = "front.sepia.ceph.com";
my $host     = get_hostname();
our $hostname = "$host.$domain:7280";
my $testfileloc;
my $sec;
my $min;
my $hour;
my $mon;
my $year;
my $mday;
my $wday;
my $yday;
my $isdst;
my $PASS_CNT = 0;
my $FAIL_CNT = 0;
my $mytestfilename;
my $testfileloc;

# function to get the current time stamp from the test set up
sub get_timestamp {
   ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
   if ($mon < 10) { $mon = "0$mon"; }
   if ($hour < 10) { $hour = "0$hour"; }
   if ($min < 10) { $min = "0$min"; }
   if ($sec < 1) { $sec = "0$sec"; }
   $year=$year+1900;
   return $year . '_' . $mon . '_' . $mday . '_' . $hour . '_' . $min . '_' . $sec;
}

# Function to check if radosgw is already running
sub get_status {
    my $service = "radosgw";
    my $cmd = "pgrep $service";
    my $status = get_cmd_op($cmd);
    if ($status =~ /\d+/ ){
        return 0;
    }
    return 1;
}

# function to execute the command and return output
sub get_cmd_op
{
    my $cmd = shift;
    my $excmd = `$cmd`;
    return $excmd;
}

#Function that executes the CLI commands and returns the output of the command
sub get_command_output {
    my $cmd_output = shift;
    open( FH, ">>$test_log" );
    print FH "\"$cmd_output\"\n";
    my $exec_cmd = `$cmd_output 2>&1`;
    print FH "$exec_cmd\n";
    close(FH);
    return $exec_cmd;
}

# Function to get the hostname
sub get_hostname
{
    my $cmd = "hostname";
    my $get_host = get_command_output($cmd);
    chomp($get_host);
    return($get_host);
}

sub pass {
    my ($comment) = @_;
    print "Comment required." unless length $comment;
    chomp $comment;
    print_border2();
    print "Test case: $TC_CNT PASSED - $comment \n";
    print_border2();
    $PASS_CNT++;
}

sub fail {
    my ($comment) = @_;
    print "Comment required." unless length $comment;
    chomp $comment;
    print_border2();
    print "Test case: $TC_CNT FAILED - $comment \n";
    print_border2();
    $FAIL_CNT++;
}

sub print_border2 {
    print "~" x 90 . "\n";
}

# Function to create the user "qa_user" and extract the user access_key and secret_key of the user
sub get_user_info
{
    my ($rgw_user) = @_;
    my $cmd = "sudo radosgw-admin user create --uid=$rgw_user --display-name=$rgw_user";
    my $cmd_op = get_command_output($cmd);
    if ($cmd_op !~ /keys/){
        return (0,0);
    }
    my @get_user = (split/,/,$cmd_op);
    foreach (@get_user) {
        if ($_ =~ /access_key/ ){
            $get_acc_key = $_;
        } elsif ($_ =~ /secret_key/ ){
            $get_sec_key = $_;
        }
    }
    my $access_key = $get_acc_key;
    my $acc_key = (split /:/, $access_key)[1];
    $acc_key =~ s/\\//g;
    $acc_key =~ s/ //g;
    $acc_key =~ s/"//g;
    my $secret_key = $get_sec_key;
    my $sec_key = (split /:/, $secret_key)[1];
    chop($sec_key);
    chop($sec_key);
    $sec_key =~ s/\\//g;
    $sec_key =~ s/ //g;
    $sec_key =~ s/"//g;
    return ($acc_key, $sec_key);
}

# Function that deletes the given user and all associated user data
sub purge_data
{
    my ($rgw_user) = @_;
    my $cmd = "sudo radosgw-admin user rm --uid=$rgw_user --purge-data";
    my $cmd_op = get_command_output($cmd);
    if ($cmd_op !~ /./){
        print "user $rgw_user deleted\n";
    } else {
        print "user $rgw_user NOT deleted\n";
        return 1;
    }
    return 0;
}

# Function to get the Ceph and distro info
sub ceph_os_info
{
        my $ceph_v = get_command_output ( "ceph -v" );
        my @ceph_arr = split(" ",$ceph_v);
        $ceph_v = "Ceph Version:   $ceph_arr[2]";
        my $os_distro = get_command_output ( "lsb_release -d" );
        my @os_arr = split(":",$os_distro);
        $os_distro = "Linux Flavor:$os_arr[1]";
        return ($ceph_v, $os_distro);
}

# Execute the test case based on the input to the script
sub create_file {
    my ($file_size, $part) = @_;
    my $cnt;
    $mytestfilename = "$file_size.$part";
    $testfileloc = "/tmp/".$mytestfilename;
    if ($file_size == '10Mb'){
        $cnt = 1;
    } elsif ($file_size == '100Mb'){
        $cnt = 10;
    } elsif ($file_size == '500Mb'){
        $cnt = 50;
    } elsif ($file_size == '1Gb'){
        $cnt = 100;
    } elsif ($file_size == '2Gb'){
        $cnt = 200;
    }
    my $ret = system("dd if=/dev/zero of=$testfileloc bs=10485760 count=$cnt");
    if ($ret) { exit 1 };
    return 0;
}

sub run_s3
{
# Run tests for the S3 functionality
    # Modify access key and secret key to suit the user account
    my ($user) = @_;
    our ( $access_key, $secret_key ) = get_user_info($user);
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

our $bucketname = 'buck_'.get_timestamp();
# create a new bucket (the test bucket)
our $bucket = $s3->add_bucket( { bucket => $bucketname } )
      or die $s3->err. "bucket $bucketname create failed\n". $s3->errstr;
    print "Bucket Created: $bucketname \n";
    return 0;
}

# upload a file to the bucket
sub upload_file {
    my ($fsize, $i) = @_;
    create_file($fsize, $i);
    print "adding file to bucket $bucketname: $mytestfilename\n";
    ($bucket->add_key_filename( $mytestfilename, $testfileloc,
        { content_type => 'text/plain', },
    ) and (print "upload file successful\n" ) and return 0 ) or (print "upload failed\n" and return 1);
}

# delete keys
sub delete_keys {
   (($bucket->delete_key($mytestfilename)) and return 0) or return 1;
}

# delete the bucket
sub delete_bucket {
   ($bucket->delete_bucket) and (print "bucket delete succeeded \n") or die $s3->err . "delete bucket failed\n" . $s3->errstr;
}

# check if rgw service is already running
sub check
{
    my $state = get_status();
    if ($state) {
        exit 1;
    }
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
