#!/usr/bin/perl
use Amazon::S3;
use Data::Dumper;
use strict;
use IO::File;
use Getopt::Long;
use Digest::MD5;

use S3Lib qw(get_user_info $rgw_user delete_user _write_log_entry _exit_result);

my $exit_status=0;
my $tc;
my $mytestfilename;
my $logmsg;
my $kruft;
my $sec;
my $min;
my $hour;
my $mon;
my $year;
my $mday;
my $wday;
my $yday;
my $isdst;
my $s3;
sub get_timestamp {
   ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
   if ($mon < 10) { $mon = "0$mon"; }
   if ($hour < 10) { $hour = "0$hour"; }
   if ($min < 10) { $min = "0$min"; }
   if ($sec < 10) { $sec = "0$sec"; }
   $year=$year+1900;

   return $year . '_' . $mon . '_' . $mday . '__' . $hour . '_' . $min . '_' . $sec;
}

#
# one time write to log with ceph version
#
open (TC,'>>log.txt');
print TC "[Log] ";
system ("ceph -v >> log.txt");

#Retrieve test case index
$tc=$ARGV[0];
print "$tc\n";


    if ($tc == 1)  { system ("dd if=/dev/zero of=/tmp/10MBfile bs=10485760 count=1");
$mytestfilename = '10MBfile'; }
    elsif ($tc == 2)  { system ("dd if=/dev/zero of=/tmp/100MBfile bs=10485760 count=10");
$mytestfilename = '100MBfile'; }
    elsif ($tc == 3)  { system ("dd if=/dev/zero of=/tmp/500MBfile bs=10485760 count=50");
$mytestfilename = '500MBfile'; }
    elsif ($tc == 4)  { system ("dd if=/dev/zero of=/tmp/1GBfile bs=10485760 count=100");
$mytestfilename = '1GBfile'; }
    elsif ($tc == 5)  { system ("dd if=/dev/zero of=/tmp/2GBfile bs=10485760 count=200");
$mytestfilename = '2GBfile'; }

    else { open (TCOMP, '>>test_completed.txt');
        close (TCOMP);
        exit(2)
    }  

my $testfileloc = "/tmp/".$mytestfilename;
print "Test file = $testfileloc\n";
#**************************************************************************
# Modify access keys to suit the target account
#my $aws_access_key_id = 'YTK5QR2XKATOSU5D9462';
#my $aws_secret_access_key = 'i6xbrQs+edkWBdG8dY7e2DGjCZNfUwLjgEXzQw0B';

my ($access_key, $secret_key) = get_user_info();
if ( ($access_key) && ($secret_key) ) {

# Make S3 connection 
# modify the host name if this test is run outside of QA.

  $s3 = Amazon::S3->new(
      {   aws_access_key_id     => $access_key,
          aws_secret_access_key => $secret_key,
          host                  => 'burnupi60.front.sepia.ceph.com',
          secure                => 0,
          retry                 => 1,
      }
  );

}
#**************************************************************************

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
  $logmsg =  "Bucket Created: $bucketname";
  _write_log_entry($logmsg);

  # or use an existing bucket
  #$bucket = $s3->bucket($bucketname);

  # upload a file to the bucket
print "adding file to bucket: $mytestfilename\n";
  $bucket->add_key_filename( $mytestfilename, $testfileloc,
      { content_type => 'text/plain', },
  ) or die $s3->err . ": " . $s3->errstr;
  $logmsg =  "$mytestfilename uploaded";
  _write_log_entry($logmsg);


  # store a value in the bucket
  $bucket->add_key( 'reminder.txt', 'this is text via add_key' )
      or die $s3->err . ": " . $s3->errstr;
  $logmsg =  "Text value stored in file";
  _write_log_entry($logmsg);

  # copy a file inthe bucket


  # list files in the bucket
  $response = $bucket->list_all
      or die $s3->err . ": " . $s3->errstr;
  foreach my $key ( @{ $response->{keys} } ) {
      my $key_name = $key->{key};
      my $key_size = $key->{size};
      print "Bucket contains key '$key_name' of size $key_size\n";
  }

  # fetch file from the bucket
print "Downloading $mytestfilename to temp file: /tmp/downloadfilepurgeme...";
  $response = $bucket->get_key_filename( $mytestfilename, 'GET', '/tmp/downloadfilepurgeme' )
      or die $s3->err . ": " . $s3->errstr;
  $logmsg =  "file dowloaded";
  _write_log_entry($logmsg);


  # fetch value from the bucket
  $response = $bucket->get_key('reminder.txt')
      or die $s3->err . ": " . $s3->errstr;
  print "reminder.txt:\n";
  print "  content length: " . $response->{content_length} . "\n";
  print "    content type: " . $response->{content_type} . "\n";
  print "            etag: " . $response->{content_type} . "\n";
  print "         content: " . $response->{value} . "\n";
#
# check the original file against the downloaded file to see if the file has been
# corrupted.
#

my $md5 = Digest::MD5->new;
my $check = 1;
my $File = $testfileloc;
my $dlfile = "/tmp/downloadfilepurgeme";
open(FILE, $File) or die "Error: Could not open $File for MD5 checksum...";
open(DLFILE, $dlfile) or die "Error: Could not open $dlfile for MD5 checksum.";
 binmode(FILE);
 binmode(DLFILE);
 my $md5sum = $md5->addfile(*FILE)->hexdigest;
 my $md5sumdl = $md5->addfile(*DLFILE)->hexdigest;
close FILE;
close DLFILE;

print "\n";
 print "Finished MD5 Checksum for $File:\n";
 print "$md5sum\n";
 print "Finished MD5 Checksum for $dlfile:\n";
 print "$md5sumdl\n";
 print "\n";

#Compare
if ( $md5sum eq $md5sumdl) {
   print "Checksums are equal\n";
  $logmsg =  "Checksums are equal";
  _write_log_entry($logmsg);
   }
else {
   print "Checksums are not equal\n";
   $exit_status=2;  
   $logmsg =  "[Failure] Checksums are not equal";
  _write_log_entry($logmsg);

   }

  # Negative test: try deleting the bucket which still contains objects
  # the method should return false 
  if (!$bucket->delete_bucket) { 
     $logmsg=  "Negative test - delete full bucket - Pass"}
  else {
       $logmsg = " Negative test - delete full bucket - Fail";
       $exit_status = 3;
       } 
  _write_log_entry($logmsg);

  # delete keys
  $bucket->delete_key('reminder.txt') or die $s3->err . ": " . $s3->errstr;
  $bucket->delete_key($mytestfilename)        or die $s3->err . ": " . $s3->errstr;
  $bucket->delete_key('bogusfile')        or die $s3->err . ": " . $s3->errstr;
  # and finally delete the bucket
  $bucket->delete_bucket or die $s3->err . ": " . $s3->errstr;

delete_user();

if ($exit_status == 0){
        open(TC,'>>log.txt');
        print TC "[Success]\n";
        close(TC);
        _exit_result();
} else {
        open(TC,'>>log.txt');
        print TC "[Failure]\n";
        close(TC);
        _exit_result($exit_status);
}

