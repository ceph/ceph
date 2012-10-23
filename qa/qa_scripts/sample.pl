#! /usr/bin/perl

use strict;
use warnings;

#===Variable Declarations====

my $home = "~teuthworker";
my $teuthology = "~teuthology/teuthology/virtualenv/bin/teuthology";
my $archivedir = "$home/qa/archive";
my $config_yaml = "fixed-3.yaml";
#my $test_name = qq/rbd_api_tests.pl/;
my $test_name = qq/[% script %]/;
my $get_script_name = &get_value($test_name,0,'\.');
my $targetyaml = "$home/qa/targets.yaml";

=head1
************Function Definition************************************************************************* 
Name of Function: get_value
Description: get_value() function splits the given command output with the delimiter passed to it
	     and returns the desired index of the array   
Arguments: $cmd_output, $index, $delimiter
Argument Description:  
$cmd_output = String that holds the command output from which required value has to be extracted.
$index = Index of the array  
$delimiter = Delimiter used in split() 
********************************************************************************************************
=cut

sub get_value
{
	my ($cmd_output, $index,$delimiter) = @_;
	my $var = qq/$delimiter/;
	my @split_cmd_output = split($var,$cmd_output);
	my $return_val = $split_cmd_output[$index];
	return $return_val;
} 

=head1
************Function Definition************************************************************************* 
Name of Function: locate_file 
Description: locate_file() function locates the input file passed to it and passes the command 
       	     output to get_value() file.    
Arguments: $get_yaml 
Argument Description:  
$get_yaml = Name of yaml file to be located 
********************************************************************************************************
=cut

sub locate_file
{
	my ($get_yaml) = @_;
	my $loc_yaml = `locate $get_yaml`;
	my $taskyaml = &get_value($loc_yaml,0,"\\n");
	return $taskyaml;
}


=head1
************Function Definition************************************************************************* 
Name of Function: generate_logfile 
Description: Generates a path for log file in the name of test with embedded time stamp under
   	     archivedir.    
Arguments: None 
Argument Description: NA 
********************************************************************************************************
=cut

sub generate_log_path
{
	my @time = split(/ /, localtime());
	my $stamp = $time[1] . "-" . $time[2] . "-" . $time[4] . "-" . $time[3];
	my $log_file = "$archivedir/$get_script_name-$stamp";
	return $log_file;
}

# Main starts here

my $task_yaml = "$get_script_name"."."."yaml";
my $configyaml = &locate_file($config_yaml); 
my $taskyaml = &locate_file($task_yaml);
my $logfile_loc = &generate_log_path(); 
my $tcommand = "$teuthology $configyaml $taskyaml $targetyaml --archive $logfile_loc";

print "$tcommand\n";

system ($tcommand);

if ($? != 0) {
        printf ("Failure  $?\n");
        open (TCOMP, '>>test_completed.txt');
        close (TCOMP);
        open (TCOMP, '>>log.txt');

        print TCOMP "[Failure]\n";
        close (TCOMP);
        exit 1;
}  else      {
        printf ("Success $?\n");

        open (TCOMP, '>>test_completed.txt');
        close (TCOMP);
        open (TCOMP, '>>log.txt');
        print TCOMP "[Success]\n";
        close (TCOMP);
	exit 0;
}

