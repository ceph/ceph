#!/usr/bin/perl
use Amazon::S3;
use Data::Dumper;
use strict;
use IO::File;
use Getopt::Long;
use Digest::MD5;
use S3Lib qw(get_hostname get_user_info $rgw_user delete_user _write_log_entry _exit_result);

my $exit_status=0;
my $tc;
my $mytestfilename;
my $logmsg;
my $kruft;
my $counter;
my $fillfile;
my $maxcount=1000;
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
my $domain = "front.sepia.ceph.com";
my $host = get_hostname();
chomp($host);
my $hostname = "$host.$domain";


sub _exit_result {
 open (TCOMP, '>>test_completed.txt');
        close (TCOMP);
 exit($exit_status);
}

sub _write_log_entry {
        open(TC,'>>log.txt');
        print TC "[Log] $logmsg\n";
        close(TC);
}


sub get_timestamp {
   ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
   if ($mon < 10) { $mon = "0$mon"; }
   if ($hour < 10) { $hour = "0$hour"; }
   if ($min < 10) { $min = "0$min"; }
   if ($sec < 10) { $sec = "0$sec"; }
   $year=$year+1900;

   return $year . '_' . $mon . '_' . $mday . '__' . $hour . '_' . $min . '_' . $sec
}

#
# one time write to log with ceph version
#
open (TC,'>>log.txt');
print TC "[Log] ";
system ("ceph -v >> log.txt");

#**************************************************************************
# Modify access keys to suit the target account

my ($access_key, $secret_key) = get_user_info();
if ( ($access_key) && ($secret_key) ) {

# Make S3 connection
# modify the host name if this test is run outside of QA.

  $s3 = Amazon::S3->new(
      {   aws_access_key_id     => $access_key,
          aws_secret_access_key => $secret_key,
          host                  => $hostname,
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
    my $bucketname =  'kftestbucket' . get_timestamp();
  print "Attempting to create bucket = $bucketname\n";

  my $bucket = $s3->add_bucket( { bucket => $bucketname } )
      or die $s3->err . $s3->errstr;
  print "Bucket Created: $bucketname\n";
  $logmsg =  "Bucket Created: $bucketname (add_bucket)";
  _write_log_entry();


  # create files  in the bucket

print "Generating files....\n";
for($counter = 1; $counter <= $maxcount; $counter++){
  $fillfile = 'bogus'.$counter;
  $bucket->add_key( $fillfile, 'this is file #$counter' )
      or die $s3->err . ": " . $s3->errstr;
  if ($counter%10 == 0){print "Created $counter files...\n"}
}

  $logmsg =  "$maxcount files created in bucket $bucketname (add_key)";
  _write_log_entry();


  # list files in the bucket and count them - must be = max count
  print "Verifying correct number of files created....\n";
  $counter=0;
  $response = $bucket->list_all
      or die $s3->err . ": " . $s3->errstr;
  foreach my $key ( @{ $response->{keys} } ) {
      my $key_name = $key->{key};
      my $key_size = $key->{size};
#      print "Bucket contains key '$key_name' of size $key_size\n";
      $counter = $counter + 1;
  }
   $logmsg =  "list_all method successful";
  _write_log_entry();
  if ($counter==$maxcount) {
     $logmsg = "File count verified = $maxcount - Pass";
     _write_log_entry();
  }
  else {
     $logmsg = "[Log] File count fail $counter vs $maxcount";
     _write_log_entry();
     $exit_status=3;
  }

 
  # delete files in the bucket
  print "Deleting $maxcount files...\n";
  $response = $bucket->list_all
      or die $s3->err . ": " . $s3->errstr;
  foreach my $key ( @{ $response->{keys} } ) {

      my $key_name = $key->{key};
      $bucket->delete_key($key_name) or die $s3->err . ": " . $s3->errstr;
  }
   $logmsg =  "$maxcount files deleted (delete_key)";
  _write_log_entry();


  # and finally delete the bucket
  $bucket->delete_bucket or die $s3->err . ": " . $s3->errstr;
   $logmsg =  "Test bucket deleted (delete_bucket)";
  _write_log_entry();

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
        _exit_result();
}

