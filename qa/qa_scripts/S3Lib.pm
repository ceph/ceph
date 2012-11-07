#! /usr/bin/perl
=head1 NAME

S3Lib.pm - Perl Module that contains the functions used by S3 test scripts for testing Rados gateway.

=cut

package S3Lib;
use Cwd;
use Exporter;
@ISA = 'Exporter';
@EXPORT_OK = qw(get_hostname get_user_info $rgw_user delete_user _write_log_entry _exit_result get_status);

#==variables ===
my $rgw_user = "qa_user";

# Function to check if radosgw is already running
sub get_status {
        my $service = "radosgw";
        my $cmd = "ps -ef | grep $service | grep -v grep";
        my $status = get_cmd_op($cmd);
        if (!$status) {
                my $cmd1 = "echo $?";
                my $status1 = get_cmd_op($cmd1);
                if (!$status1){
                        return 0;
                }
        }
        return 1;
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

# Function that enters the given msg to log.txt

sub _write_log_entry {
    my $logmsg = shift;
    open(TC,'>>log.txt');
    print TC "[Log] $logmsg\n";
    close(TC);
}

# Function that creates the test_completed.txt as required by xstudio run at the end of the test

sub _exit_result {
    my $exit_status = shift;
    open (TCOMP, '>>test_completed.txt');
    close (TCOMP);
    exit($exit_status);
}

# Function to create the user "qa_user" and extract the user access_key and secret_key of the user
sub get_user_info
{
    my $cmd = "sudo radosgw-admin user create --uid=$rgw_user --display-name=$rgw_user";
    my $cmd_op = get_command_output($cmd);
    if ($cmd_op !~ /keys/){
        _write_log_entry( "user $rgw_user NOT created" );
        return (0,0);
    }
    _write_log_entry( "user $rgw_user created" );
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
        _write_log_entry( "user $rgw_user deleted" );
    } else {
        _write_log_entry( "user $rgw_user NOT deleted" );
    }
}

# Function to get the hostname
sub get_hostname
{
    my $cmd = "hostname";
    my $get_host = get_command_output($cmd);
    chomp($get_host);
    return($get_host);
}
