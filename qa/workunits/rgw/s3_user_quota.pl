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
our $mytestfilename;
my $mytestfilename1;
my $logmsg;
my $kruft;
my $s3;
my $hostdom  = $ENV{RGW_FQDN}||hostfqdn();
my $port     = $ENV{RGW_PORT}||7280;
our $hostname = "$hostdom:$port";
our $testfileloc;
our $cnt;

sub quota_set_max_size_per_user {
    my ($maxsize, $size1,$rgw_user) = @_;
    run_s3($rgw_user);
    my $set_quota = `$radosgw_admin quota set --uid=$rgw_user --quota-scope=user --max-size=$maxsize`;
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
    delete_keys($mytestfilename);
    purge_data($rgw_user);
    return 0;
}

sub max_size_per_user {
    my ($maxsize, $size1,$rgw_user) = @_;
    run_s3($rgw_user);
    my $set_quota = `$radosgw_admin quota set --uid=$rgw_user --quota-scope=user --max-size=$maxsize`;
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
    my $set_quota = `$radosgw_admin quota set --uid=$rgw_user --quota-scope=user --max-objects=$maxobjs`;
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
    delete_keys($mytestfilename);
    purge_data($rgw_user);
}
 
sub quota_enable_user {
    my ($rgw_user) = @_;
    my $en_quota = `$radosgw_admin quota enable --uid=$rgw_user --quota-scope=user`;
    if ($en_quota !~ /./){
      print "quota enabled for the user $rgw_user \n";
    } else {
      print "quota enable failed for the user $rgw_user \n";
      exit 1;
    }
    return 0;
}

sub quota_disable_user {
    my $dis_quota = `$radosgw_admin quota disable --uid=$rgw_user --quota-scope=user`;
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

# delete the bucket
sub delete_bucket {
   ($bucket->delete_bucket) and (print "bucket delete succeeded \n") or die $s3->err . "delete bucket failed\n" . $s3->errstr;
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
    delete_keys($mytestfilename);
    purge_data($usr);
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
