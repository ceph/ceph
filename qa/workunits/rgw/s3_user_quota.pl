#! /usr/bin/perl

=head1 NAME

s3_user_quota.pl - Script to test the rgw user quota functionality using s3 interface.

=head1 SYNOPSIS

Use:
        perl s3_user_quota.pl [--help]

Examples:
        perl s3_user_quota.pl
        or
        perl s3_user_quota.pl  --help

=head1 DESCRIPTION

This script intends to test the rgw user quota funcionality using s3 interface
and reports the test results

=head1 ARGUMENTS

s3_user_quota.pl takes the following arguments:
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
my $mytestfilename;
my $mytestfilename1;
my $logmsg;
my $kruft;
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
our $cnt;

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

sub quota_set_max_size_per_user {
    my ($maxsize, $size1,$rgw_user) = @_;
    run_s3($rgw_user);
    my $set_quota = `sudo radosgw-admin quota set --uid=$rgw_user --quota-scope=user --max-size=$maxsize`;
    if (($set_quota !~ /./)&&($maxsize == 0)){
      my $ret = test_max_objs($size1, $rgw_user);
      if ($ret == 1){
         pass("quota set for user: $rgw_user with max_size=$maxsize passed" );
      }else {
         fail("quota set for user: $rgw_user with max_size=$maxsize failed" );
      }
    } elsif (($set_quota !~ /./) && ($maxsize != 0)) {
      my $ret = test_max_objs($size1, $rgw_user);
      if ($ret == 0){
         pass("quota set for user: $rgw_user with max_size=$maxsize passed" );
      }else {
         fail("quota set for user: $rgw_user with max_size=$maxsize failed" );
      }
    }
    delete_keys();
    purge_data($rgw_user);
    return 0;
}

sub max_size_per_user {
    my ($maxsize, $size1,$rgw_user) = @_;
    run_s3($rgw_user);
    my $set_quota = `sudo radosgw-admin quota set --uid=$rgw_user --quota-scope=user --max-size=$maxsize`;
    if (($set_quota !~ /./) && ($maxsize != 0)) {
      my $ret = test_max_objs($size1, $rgw_user);
      if ($ret == 0){
         $cnt++;
      }
    }
    return $cnt;
}

sub quota_set_max_obj_per_user {
    # set max objects
    my ($maxobjs, $size1, $rgw_user) = @_;
    run_s3($rgw_user);
    my $set_quota = `sudo radosgw-admin quota set --uid=$rgw_user --quota-scope=user --max-objects=$maxobjs`;
    if (($set_quota !~ /./) && ($maxobjs == 0)){
      my $ret = test_max_objs($size1, $rgw_user);
      if ($ret == 1){
         pass("quota set for user: $rgw_user with max_objects=$maxobjs passed" );
      }else {
         fail("quota set for user: $rgw_user with max_objects=$maxobjs failed" );
      }
    } elsif (($set_quota !~ /./) && ($maxobjs == 1)) {
      my $ret = test_max_objs($size1, $rgw_user);
      if ($ret == 0){
         pass("quota set for user: $rgw_user with max_objects=$maxobjs passed" );
      }else {
         fail("quota set for user: $rgw_user with max_objects=$maxobjs failed" );
      }
    }
    delete_keys();
    purge_data($rgw_user);
}
 
sub quota_enable_user {
    my ($rgw_user) = @_;
    my $en_quota = `sudo radosgw-admin quota enable --uid=$rgw_user --quota-scope=user`;
    if ($en_quota !~ /./){
      print "quota enabled for the user $rgw_user \n";
    } else {
      print "quota enable failed for the user $rgw_user \n";
      exit 1;
    }
    return 0;
}

sub quota_disable_user {
    my $dis_quota = `sudo radosgw-admin quota disable --uid=$rgw_user --quota-scope=user`;
    if ($dis_quota !~ /./){
      print "quota disabled for the user $rgw_user \n";
    } else {
      print "quota disable failed for the user $rgw_user \n";
      exit 1;
    }
    return 0;
}

# upload a file to the bucket
sub upload_file {
    print "adding file to bucket $bucketname: $mytestfilename\n";
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

#Function to upload the given file size to bucket and verify
sub test_max_objs {
    my ($size, $rgw_user) = @_;
    create_file($size);
    quota_enable_user($rgw_user);
    my $ret_value = upload_file();
    return $ret_value;
}

# set user quota and ensure it is validated
sub test_user_quota_max_size{
    my ($max_buckets,$size, $fsize) = @_;
    my $usr = rand();
    foreach my $i (1..$max_buckets){
       my $ret_value = max_size_per_user($size, $fsize, $usr );
    }
    if ($ret_value == $max_buckets){
       fail( "user quota max size for $usr failed on $max_buckets buckets" );
    } else {
       pass( "user quota max size for $usr passed on $max_buckets buckets" );
    }
    delete_keys();
    purge_data($usr);
}

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
check();
quota_set_max_obj_per_user('0', '10Mb', 'usr1');
quota_set_max_obj_per_user('1', '10Mb', 'usr2');
quota_set_max_size_per_user(0, '10Mb', 'usr1');
quota_set_max_size_per_user(1048576000, '1Gb', 'usr2');
test_user_quota_max_size(3,1048576000,'100Mb');
test_user_quota_max_size(2,1048576000, '1Gb');
print "OK";
