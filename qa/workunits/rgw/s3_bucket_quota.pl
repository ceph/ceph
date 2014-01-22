#! /usr/bin/perl

=head1 NAME

s3_bucket_quota.pl - Script to test the rgw bucket quota functionality using s3 interface. 

=head1 SYNOPSIS

Use:
        perl s3_bucket_quota.pl [--help]

Examples:
        perl s3_bucket_quota.pl 
        or
        perl s3_bucket_quota.pl  --help

=head1 DESCRIPTION

This script intends to test the rgw bucket quota funcionality using s3 interface 
and reports the test results

=head1 ARGUMENTS

s3_bucket_quota.pl takes the following arguments:
   --help
   (optional) Displays the usage message.

=cut

use Amazon::S3;
use Data::Dumper;
#use strict;
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
my $mytestfilename;
my $mytestfilename1;
my $logmsg;
my $kruft;
my $s3;
my $domain   = "front.sepia.ceph.com";
my $host     = get_hostname();
my $hostname = "$host.$domain:7280"; # as rgw is running on port 7280
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
my $rgw_user = "qa_user";

# function to get the current time stamp from the test set up
sub get_timestamp {
   ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
   if ($mon < 10) { $mon = "0$mon"; }
   if ($hour < 10) { $hour = "0$hour"; }
   if ($min < 10) { $min = "0$min"; }
   if ($sec < 10) { $sec = "0$sec"; }
   $year=$year+1900;
   return $year . '_' . $mon . '_' . $mday . '_' . $hour . '_' . $min . '_' . $sec;
}

# Function to check if radosgw is already running
sub get_status {
    my $service = "radosgw";
    my $cmd = "ps -ef | grep $service | grep -v grep";
    my $status = get_cmd_op($cmd);
    if ($status =~ /client.radosgw/ ){
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

# Function that deletes the user $rgw_user and write to logfile. 
sub delete_user
{
    my $cmd = "sudo radosgw-admin user rm --uid=$rgw_user";
    my $cmd_op = get_command_output($cmd);
    if ($cmd_op !~ /aborting/){
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
    my ($file_size) = @_;
    my $cnt;
    $mytestfilename = $file_size; 
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
    # Modify access keys to suit the target account
    our ( $access_key, $secret_key ) = get_user_info();
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
      or die $s3->err. "bucket create failed\n". $s3->errstr;
    print "Bucket Created: $bucketname\n";
    return 0;
}

sub quota_set_max_size {
    my $set_quota = `sudo radosgw-admin quota set --bucket=$bucketname --max-size=1048576000`; 
    if ($set_quota !~ /./){
      print "quota set for the bucket: $bucketname \n";
    } else {
      print "quota set failed for the bucket: $bucketname \n";
      exit 1;
    }
    return 0;
}

sub quota_set_max_size_zero {
    run_s3();
    my $set_quota = `sudo radosgw-admin quota set --bucket=$bucketname --max-size=0`; 
    if ($set_quota !~ /./){
      pass ("quota set for the bucket: $bucketname with max size as zero\n");
    } else {
      fail ("quota set with max size 0 failed for the bucket: $bucketname \n");
    }
    delete_bucket();
}

sub quota_set_max_objs_zero {
    run_s3();
    my $set_quota = `sudo radosgw-admin quota set --bucket=$bucketname --max-objects=0`; 
    if ($set_quota !~ /./){
      pass ("quota set for the bucket: $bucketname with max objects as zero\n");
    } else {
      fail ("quota set with max objects 0 failed for the bucket: $bucketname \n");
    }
    delete_bucket();
}

sub quota_set_neg_size {
    run_s3();
    my $set_quota = `sudo radosgw-admin quota set --bucket=$bucketname --max-size=-1`; 
    if ($set_quota !~ /./){
      pass ("quota set for the bucket: $bucketname with max size -1\n");
    } else {
      fail ("quota set failed for the bucket: $bucketname with max size -1 \n");
    }
    delete_bucket();
}

sub quota_set_neg_objs {
    run_s3();
    my $set_quota = `sudo radosgw-admin quota set --bucket=$bucketname --max-objects=-1`; 
    if ($set_quota !~ /./){
      pass ("quota set for the bucket: $bucketname max objects -1 \n");
    } else {
      fail ("quota set failed for the bucket: $bucketname \n with max objects -1");
    }
    delete_bucket();
}

sub quota_set_user_objs {
    my $set_quota = `sudo radosgw-admin quota set --uid=$rgw_user --quota-scope=bucket`; 
    my $set_quota1 = `sudo radosgw-admin quota set --bucket=$bucketname --max-objects=1`; 
    if ($set_quota1 !~ /./){
      print "bucket quota max_objs set for the given user: $bucketname \n";
    } else {
      print "bucket quota max_objs set failed for the given user: $bucketname \n";
      exit 1;
    }
    return 0;
}

sub quota_set_user_size {
    my $set_quota = `sudo radosgw-admin quota set --uid=$rgw_user --quota-scope=bucket`; 
    my $set_quota1 = `sudo radosgw-admin quota set --bucket=$bucketname --max-size=1048576000`; 
    if ($set_quota1 !~ /./){
      print "bucket quota max size set for the given user: $bucketname \n";
    } else {
      print "bucket quota max size set failed for the user: $bucketname \n";
      exit 1;
    }
    return 0;
}

sub quota_set_max_obj {
    # set max objects 
    my $set_quota = `sudo radosgw-admin quota set --bucket=$bucketname --max-objects=1`; 
    if ($set_quota !~ /./){ 
      print "quota set for the bucket: $bucketname \n"; 
    } else {
      print "quota set failed for the bucket: $bucketname \n"; 
     exit 1;
    }
    return 0;
}

sub quota_enable {
    my $en_quota = `sudo radosgw-admin quota enable --bucket=$bucketname`; 
    if ($en_quota !~ /./){ 
      print "quota enabled for the bucket: $bucketname \n"; 
    } else {
      print "quota enable failed for the bucket: $bucketname \n"; 
      exit 1;
    }
    return 0;
}

sub quota_disable {
    my $dis_quota = `sudo radosgw-admin quota disable --bucket=$bucketname`; 
    if ($dis_quota !~ /./){ 
      print "quota disabled for the bucket: $bucketname \n"; 
    } else {
      print "quota disable failed for the bucket: $bucketname \n"; 
      exit 1;
    }
    return 0;
}

# upload a file to the bucket
sub upload_file {
    print "adding file to bucket: $mytestfilename\n";
    ($bucket->add_key_filename( $mytestfilename, $testfileloc,
        { content_type => 'text/plain', },
    ) and (print "upload file successful\n" ) and return 0 ) or (return 1);
}

# delete keys
sub delete_keys {
   (($bucket->delete_key($mytestfilename)) and return 0) or return 1;
}

# delete the bucket
sub delete_bucket {
   #($bucket->delete_key($mytestfilename1) and print "delete keys on bucket succeeded second time\n" ) or die $s3->err . "delete keys on bucket failed second time\n" . $s3->errstr;
   ($bucket->delete_bucket) and (print "bucket delete succeeded \n") or die $s3->err . "delete bucket failed\n" . $s3->errstr;
}

# Readd the file back to bucket 
sub readd_file {
    system("dd if=/dev/zero of=/tmp/10MBfile1 bs=10485760 count=1");
    $mytestfilename1 = '10MBfile1';
    print "readding file to bucket: $mytestfilename1\n";
    ((($bucket->add_key_filename( $mytestfilename1, $testfileloc,
        { content_type => 'text/plain', },
    )) and (print "readding file success\n") and return 0) or (return 1));
}

# set bucket quota with max_objects and verify 
sub test_max_objects {
    my $size = '10Mb';
    create_file($size);
    run_s3();
    quota_set_max_obj();
    quota_enable();
    my $ret_value = upload_file();
    if ($ret_value == 0){
        pass ( "Test max objects passed" );
    } else {
        fail ( "Test max objects failed" );
    }
    delete_user();
    delete_keys();
    delete_bucket();
}

# Set bucket quota for specific user and ensure max objects set for the user is validated
sub test_max_objects_per_user{
    my $size = '10Mb';
    create_file($size);
    run_s3();
    quota_set_user_objs();
    quota_enable();
    my $ret_value = upload_file();
    if ($ret_value == 0){
        pass ( "Test max objects for the given user passed" );
    } else {
        fail ( "Test max objects for the given user failed" );
    }
    delete_user();
    delete_keys();
    delete_bucket();
}

# set bucket quota with max_objects and try to exceed the max_objects and verify 
sub test_beyond_max_objs {
    my $size = "10Mb";
    create_file($size);
    run_s3();
    quota_set_max_obj();
    quota_enable();
    upload_file();
    my $ret_value = readd_file();
    if ($ret_value == 1){
        pass ( "set max objects and test beyond max objects passed" );
    } else {
        fail ( "set max objects and test beyond max objects failed" );
    }
    delete_user();
    delete_keys();
    delete_bucket();
}

# set bucket quota for a user with max_objects and try to exceed the max_objects and verify 
sub test_beyond_max_objs_user {
    my $size = "10Mb";
    create_file($size);
    run_s3();
    quota_set_user_objs();
    quota_enable();
    upload_file();
    my $ret_value = readd_file();
    if ($ret_value == 1){
        pass ( "set max objects for a given user and test beyond max objects passed" );
    } else {
        fail ( "set max objects for a given user and test beyond max objects failed" );
    }
    delete_user();
    delete_keys();
    delete_bucket();
}

# set bucket quota for max size and ensure it is validated
sub test_quota_size {
    my $ret_value;
    my $size = "2Gb";
    create_file($size);
    run_s3();
    quota_set_max_size();    
    quota_enable();
    my $ret_value = upload_file();
    if ($ret_value == 1) {
        pass ( "set max size and ensure that objects upload beyond max size is not entertained" );
        my $retdel = delete_keys();
        if ($retdel == 0) {
            print "delete objects successful \n";
            my $size1 = "1Gb";
            create_file($size1);
            my $ret_val1 = upload_file(); 
            if ($ret_val1 == 0) {
                pass ( "set max size and ensure that the max size is in effect" );
            } else {
                fail ( "set max size and ensure the max size takes effect" );
            }
        }
    } else {
        fail ( "set max size and ensure that objects beyond max size is not allowed" );
    }
    delete_user();
    delete_keys();
    delete_bucket();
}

# set bucket quota for max size for a given user and ensure it is validated
sub test_quota_size_user {
    my $ret_value;
    my $size = "2Gb";
    create_file($size);
    run_s3();
    quota_set_user_size();
    quota_enable();
    my $ret_value = upload_file();
    if ($ret_value == 1) {
        pass ( "set max size for a given user and ensure that objects upload beyond max size is not entertained" );
        my $retdel = delete_keys();
        if ($retdel == 0) {
            print "delete objects successful \n";
            my $size1 = "1Gb";
            create_file($size1);
            my $ret_val1 = upload_file();
            if ($ret_val1 == 0) {
                pass ( "set max size for a given user and ensure that the max size is in effect" );
            } else {
                fail ( "set max size for a given user and ensure the max size takes effect" );
            }
        }
    } else {
        fail ( "set max size for a given user and ensure that objects beyond max size is not allowed" );
    }
    delete_user();
    delete_keys();
    delete_bucket();
}

# set bucket quota size but disable quota and verify
sub test_quota_size_disabled {
    my $ret_value;
    my $size = "2Gb";
    create_file($size);
    run_s3();
    quota_set_max_size();
    quota_disable();
    my $ret_value = upload_file();
    if ($ret_value == 0) {
        pass ( "bucket quota size doesnt take effect when quota is disabled" );
    } else {
        fail ( "bucket quota size doesnt take effect when quota is disabled" );
    }
    delete_user();
    delete_keys();
    delete_bucket();
}

# set bucket quota size for a given user but disable quota and verify
sub test_quota_size_disabled_user {
    my $ret_value;
    my $size = "2Gb";
    create_file($size);
    run_s3();
    quota_set_user_size();
    quota_disable();
    my $ret_value = upload_file();
    if ($ret_value == 0) {
        pass ( "bucket quota size for a given user doesnt take effect when quota is disabled" );
    } else {
        fail ( "bucket quota size for a given user doesnt take effect when quota is disabled" );
    }
    delete_user();
    delete_keys();
    delete_bucket();
}

# set bucket quota for specified user and verify

# check if rgw service is already running
sub check
{
    my $state = get_status();
    if ($state) {
        exit 1;
    }
}

#== Main starts here===
ceph_os_info();
test_max_objects();
test_max_objects_per_user();
test_beyond_max_objs();
test_beyond_max_objs_user();
quota_set_max_size_zero();
quota_set_max_objs_zero();
quota_set_neg_objs();
quota_set_neg_size();
test_quota_size(); 
test_quota_size_user();
test_quota_size_disabled();
test_quota_size_disabled_user();

print "OK";
