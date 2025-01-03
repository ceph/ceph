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

This script intends to test the rgw bucket quota functionality using s3 interface 
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
my $port     = $ENV{RGW_PORT}||80;
our $hostname = "$hostdom:$port";
our $testfileloc;
my $rgw_user = "qa_user";

# Function that deletes the user $rgw_user and write to logfile. 
sub delete_user
{
    my $cmd = "$radosgw_admin user rm --uid=$rgw_user";
    my $cmd_op = get_command_output($cmd);
    if ($cmd_op !~ /aborting/){
        print "user $rgw_user deleted\n";
    } else {
        print "user $rgw_user NOT deleted\n";
        return 1;
    }
    return 0;
}

sub quota_set_max_size {
    my $set_quota = `$radosgw_admin quota set --bucket=$bucketname --max-size=1048576000`; 
    if ($set_quota !~ /./){
      print "quota set for the bucket: $bucketname \n";
    } else {
      print "quota set failed for the bucket: $bucketname \n";
      exit 1;
    }
    return 0;
}

sub quota_set_max_size_zero {
    run_s3($rgw_user);
    my $set_quota = `$radosgw_admin quota set --bucket=$bucketname --max-size=0`; 
    if ($set_quota !~ /./){
      pass ("quota set for the bucket: $bucketname with max size as zero\n");
    } else {
      fail ("quota set with max size 0 failed for the bucket: $bucketname \n");
    }
    delete_bucket();
}

sub quota_set_max_objs_zero {
    run_s3($rgw_user);
    my $set_quota = `$radosgw_admin quota set --bucket=$bucketname --max-objects=0`; 
    if ($set_quota !~ /./){
      pass ("quota set for the bucket: $bucketname with max objects as zero\n");
    } else {
      fail ("quota set with max objects 0 failed for the bucket: $bucketname \n");
    }
    delete_bucket();
}

sub quota_set_neg_size {
    run_s3($rgw_user);
    my $set_quota = `$radosgw_admin quota set --bucket=$bucketname --max-size=-1`; 
    if ($set_quota !~ /./){
      pass ("quota set for the bucket: $bucketname with max size -1\n");
    } else {
      fail ("quota set failed for the bucket: $bucketname with max size -1 \n");
    }
    delete_bucket();
}

sub quota_set_neg_objs {
    run_s3($rgw_user);
    my $set_quota = `$radosgw_admin quota set --bucket=$bucketname --max-objects=-1`; 
    if ($set_quota !~ /./){
      pass ("quota set for the bucket: $bucketname max objects -1 \n");
    } else {
      fail ("quota set failed for the bucket: $bucketname \n with max objects -1");
    }
    delete_bucket();
}

sub quota_set_user_objs {
    my $set_quota = `$radosgw_admin quota set --uid=$rgw_user --quota-scope=bucket`; 
    my $set_quota1 = `$radosgw_admin quota set --bucket=$bucketname --max-objects=1`; 
    if ($set_quota1 !~ /./){
      print "bucket quota max_objs set for the given user: $bucketname \n";
    } else {
      print "bucket quota max_objs set failed for the given user: $bucketname \n";
      exit 1;
    }
    return 0;
}

sub quota_set_user_size {
    my $set_quota = `$radosgw_admin quota set --uid=$rgw_user --quota-scope=bucket`; 
    my $set_quota1 = `$radosgw_admin quota set --bucket=$bucketname --max-size=1048576000`; 
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
    my $set_quota = `$radosgw_admin quota set --bucket=$bucketname --max-objects=1`; 
    if ($set_quota !~ /./){ 
      print "quota set for the bucket: $bucketname \n"; 
    } else {
      print "quota set failed for the bucket: $bucketname \n"; 
     exit 1;
    }
    return 0;
}

sub quota_enable {
    my $en_quota = `$radosgw_admin quota enable --bucket=$bucketname`; 
    if ($en_quota !~ /./){ 
      print "quota enabled for the bucket: $bucketname \n"; 
    } else {
      print "quota enable failed for the bucket: $bucketname \n"; 
      exit 1;
    }
    return 0;
}

sub quota_disable {
    my $dis_quota = `$radosgw_admin quota disable --bucket=$bucketname`; 
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

# delete the bucket
sub delete_bucket {
   #($bucket->delete_key($mytestfilename1) and print "delete keys on bucket succeeded second time\n" ) or die $s3->err . "delete keys on bucket failed second time\n" . $s3->errstr;
   ($bucket->delete_bucket) and (print "bucket delete succeeded \n") or die $s3->err . "delete bucket failed\n" . $s3->errstr;
}

# set bucket quota with max_objects and verify 
sub test_max_objects {
    my $size = '10Mb';
    create_file($size);
    run_s3($rgw_user);
    quota_set_max_obj();
    quota_enable();
    my $ret_value = upload_file();
    if ($ret_value == 0){
        pass ( "Test max objects passed" );
    } else {
        fail ( "Test max objects failed" );
    }
    delete_user();
    delete_keys($mytestfilename);
    delete_bucket();
}

# Set bucket quota for specific user and ensure max objects set for the user is validated
sub test_max_objects_per_user{
    my $size = '10Mb';
    create_file($size);
    run_s3($rgw_user);
    quota_set_user_objs();
    quota_enable();
    my $ret_value = upload_file();
    if ($ret_value == 0){
        pass ( "Test max objects for the given user passed" );
    } else {
        fail ( "Test max objects for the given user failed" );
    }
    delete_user();
    delete_keys($mytestfilename);
    delete_bucket();
}

# set bucket quota with max_objects and try to exceed the max_objects and verify 
sub test_beyond_max_objs {
    my $size = "10Mb";
    create_file($size);
    run_s3($rgw_user);
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
    delete_keys($mytestfilename);
    delete_bucket();
}

# set bucket quota for a user with max_objects and try to exceed the max_objects and verify 
sub test_beyond_max_objs_user {
    my $size = "10Mb";
    create_file($size);
    run_s3($rgw_user);
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
    delete_keys($mytestfilename);
    delete_bucket();
}

# set bucket quota for max size and ensure it is validated
sub test_quota_size {
    my $ret_value;
    my $size = "2Gb";
    create_file($size);
    run_s3($rgw_user);
    quota_set_max_size();    
    quota_enable();
    my $ret_value = upload_file();
    if ($ret_value == 1) {
        pass ( "set max size and ensure that objects upload beyond max size is not entertained" );
        my $retdel = delete_keys($mytestfilename);
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
    delete_keys($mytestfilename);
    delete_bucket();
}

# set bucket quota for max size for a given user and ensure it is validated
sub test_quota_size_user {
    my $ret_value;
    my $size = "2Gb";
    create_file($size);
    run_s3($rgw_user);
    quota_set_user_size();
    quota_enable();
    my $ret_value = upload_file();
    if ($ret_value == 1) {
        pass ( "set max size for a given user and ensure that objects upload beyond max size is not entertained" );
        my $retdel = delete_keys($mytestfilename);
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
    delete_keys($mytestfilename);
    delete_bucket();
}

# set bucket quota size but disable quota and verify
sub test_quota_size_disabled {
    my $ret_value;
    my $size = "2Gb";
    create_file($size);
    run_s3($rgw_user);
    quota_set_max_size();
    quota_disable();
    my $ret_value = upload_file();
    if ($ret_value == 0) {
        pass ( "bucket quota size doesnt take effect when quota is disabled" );
    } else {
        fail ( "bucket quota size doesnt take effect when quota is disabled" );
    }
    delete_user();
    delete_keys($mytestfilename);
    delete_bucket();
}

# set bucket quota size for a given user but disable quota and verify
sub test_quota_size_disabled_user {
    my $ret_value;
    my $size = "2Gb";
    create_file($size);
    run_s3($rgw_user);
    quota_set_user_size();
    quota_disable();
    my $ret_value = upload_file();
    if ($ret_value == 0) {
        pass ( "bucket quota size for a given user doesnt take effect when quota is disabled" );
    } else {
        fail ( "bucket quota size for a given user doesnt take effect when quota is disabled" );
    }
    delete_user();
    delete_keys($mytestfilename);
    delete_bucket();
}

# set bucket quota for specified user and verify

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
