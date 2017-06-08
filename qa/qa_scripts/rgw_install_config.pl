#! /usr/bin/perl

=head1 NAME

rgw_install_config.pl - Script to install and configure the rados gateway on client machine.

=head1 SYNOPSIS

Use:
        perl rgw_install_config.pl [--help]

Examples:
        perl rgw_install_config.pl
        or
        perl rgw_install_config.pl  --help

=head1 ARGUMENTS

rgw_install_config.pl takes the following arguments:
        --help
        (optional) Displays the usage message.

=cut

use strict;
use warnings;
use Pod::Usage();
use Getopt::Long();

my $help;

Getopt::Long::GetOptions(
    'help'   => \$help
);

Pod::Usage::pod2usage( -verbose => 1 ) if ($help);

# Function that enters the given msg to log.txt

sub _write_log_entry {
    my $logmsg = shift;
    open(TC,'>>log.txt');
    print TC "[Log] $logmsg\n";
    close(TC);
}

# Function to get the hostname
sub get_hostname
{
    my $cmd = "hostname";
    my $get_host = `$cmd`;
    chomp($get_host);
    return($get_host);
}

# Function to execute the command and return the exit status
sub exec_cmd
{
    my $cmd = shift;
    my $excmd = system($cmd);
    if ( $excmd == 0 ) {
        _write_log_entry("$cmd successful");
	return 0;
    } else{ 
        _write_log_entry("$cmd NOT successful");
        return 1;
    }	
} 

# function to execute the command and return output
sub get_cmd_op
{
    my $cmd = shift;
    my $excmd = `$cmd`;
    _write_log_entry("$cmd \n $excmd");
    return $excmd;
}

# Function to enable module for apache and fastcgi
sub enmod
{
	if (!system("sudo a2enmod rewrite")){
		if (!system("sudo a2enmod fastcgi")){
		        _write_log_entry("a2enmod rewrite && a2enmod fastcgi successful"); 
			return 0;
		}	
		write_log_entry("a2enmod fastcgi NOT successful"); 
                return 1;
	}	
	write_log_entry("a2enmod rewrite NOT successful"); 
        return 1;
}

# Function to create httpd.conf file
sub set_httpconf
{
       my $hostname = shift;
       my $http_conf = "/etc/apache2/httpd.conf";
       my $file = "test_file";
       open (FH, ">$file"); 
       print FH "ServerName $hostname";
       close FH;
       my $get_op = "sudo sh -c \"cat $file >> $http_conf\"";        
       my $exit_status = exec_cmd($get_op);	
       exec_cmd("rm -f $file");
       return $exit_status;
}

# To append ceph.conf file with radosgw info
sub append_ceph_conf
{
        my $hostname = shift;
	my $file = "/etc/ceph/ceph.conf";
	my $file1 = "test_file1";
	open (FH, ">$file1");
	print FH "[client.radosgw.gateway]
		  host = $hostname
		  keyring = /etc/ceph/keyring.radosgw.gateway
		  rgw socket path = /tmp/radosgw.sock
		  log file = /var/log/ceph/radosgw.log \n";
	close FH;
	my $get_op = "sudo sh -c \"cat $file1 >> $file\""; 
        my $exit_status = exec_cmd($get_op);	
        exec_cmd("rm -f $file1");
        return $exit_status;
}

# create s3gw.fcgi file and set execute permission for the file  
sub create_fcgi
{
        my $file = "/var/www/s3gw.fcgi";
        my $chmod_file = "sudo chmod +x /var/www/s3gw.fcgi";
        my $exe_cmd = "exec /usr/bin/radosgw -c /etc/ceph/ceph.conf -n client.radosgw.gateway";
        my $file1 = "test_file3";
        open (FH, ">$file1");
        print FH "#!/bin/sh \n $exe_cmd \n"; 
        close FH;
        my $get_op = "sudo sh -c \"cat $file1 >> $file\"" ;
        my $exit_status = exec_cmd($get_op);
        exec_cmd("rm -f $file1");
        my $exit_status1 = exec_cmd($chmod_file) if (!$exit_status); 
        return $exit_status1;
}

# To create rgw.conf
sub create_rgw_conf {
	my $content = "FastCgiExternalServer /var/www/s3gw.fcgi -socket /tmp/radosgw.sock
<VirtualHost *:80>
        ServerName rados.domain.com
        ServerAdmin qa\@inktank.com
        DocumentRoot /var/www

        RewriteEngine On
        RewriteRule ^/([a-zA-Z0-9-_.]*)([/]?.*) /s3gw.fcgi?page=\$1&params=\$2&%{QUERY_STRING} [E=HTTP_AUTHORIZATION:%{HTTP:Authorization},L]

        <IfModule mod_fastcgi.c>
                <Directory /var/www>
                        Options +ExecCGI
                        AllowOverride All
                        SetHandler fastcgi-script
                        Order allow,deny
                        Allow from all
                        AuthBasicAuthoritative Off
                </Directory>
        </IfModule>
        AllowEncodedSlashes On
        ErrorLog /var/log/apache2/error.log
        CustomLog /var/log/apache2/access.log combined
        ServerSignature Off
</VirtualHost>";

	my $file = "/etc/apache2/sites-available/rgw.conf";
        my $file1 = "test_file2";
	open (FH, ">$file1");
	print FH "$content";
	close FH;
	my $get_op = "sudo sh -c \"cat $file1 >> $file\""; 
	my $exit_status = exec_cmd($get_op);	
        exec_cmd("rm -f $file1");
	return $exit_status;
}

# To generate keyring for rados gateway and add it to ceph keyring with required access
sub generate_keyring_and_key
{
 	my $cmd = "sudo ceph-authtool --create-keyring /etc/ceph/keyring.radosgw.gateway";
        my $chmod_cmd = "sudo chmod +r /etc/ceph/keyring.radosgw.gateway";
	my $cmd_key = "sudo ceph-authtool /etc/ceph/keyring.radosgw.gateway -n client.radosgw.gateway --gen-key";
	my $chmod_cmd_key = "sudo ceph-authtool -n client.radosgw.gateway --cap osd \'allow rwx\' --cap mon \'allow r\' /etc/ceph/keyring.radosgw.gateway";
  	my $exit_status = exec_cmd($cmd);
	my $exit_status1 = exec_cmd($chmod_cmd) if(!$exit_status);
        my $exit_status2 = exec_cmd($cmd_key) if(!$exit_status1);
        my $exit_status3 = exec_cmd($chmod_cmd_key) if(!$exit_status2);
	return($exit_status3);
}

# To create a rgw user
sub create_user
{
	my $usr = shift;
	my $cmd = "sudo radosgw-admin user create --uid=$usr --display-name=$usr";	
	my $status = exec_cmd($cmd);
	return($status);
}

#To start radosgw
sub start_rgw
{
 	my $cmd = "sudo /etc/init.d/radosgw start";
	my $check_ps = "ps -ef | grep radosgw | grep -v grep"; 
	my $status = get_cmd_op($cmd);
	if (!$status) {  
		my $ps = get_cmd_op($check_ps); 
		if ($ps =~ /radosgw/) {
			return 0;
		}
	}
	return 1;
}

# To start the given service
sub start_service
{
	my $input = shift;
	my $status = exec_cmd ("sudo service $input restart");
	if (!$status){
		my $output = get_cmd_op("sudo service $input status");
		if ($output =~ /running/ ){
			if($input eq "apache2" ) {
				return 0;
			}elsif($input eq "ceph"){
				my $count = get_cmd_op("sudo service ceph status | wc -l");
				if ($count == 8 ){
					return 0;
				}
			}
		}
	}
	return 1;
}

# To enable/disable site
sub ensite_dissite
{
	my $a2ensite = "sudo a2ensite rgw.conf";
	my $a2dissite = "sudo a2dissite default";
	my $check_en = get_cmd_op($a2ensite); 
	my $check_dis = get_cmd_op($a2dissite); 
	if (($check_en =~ /nabl/) && ($check_dis =~ /isabl/)){
		return 0;
	}
	return 1;
}

#====Main starts here ======

my $domain = "front.sepia.ceph.com";
my $host = get_hostname();
my $hostname = "$host.$domain";
my $run_update = "sudo apt-get update";  
my $install_radosgw = "sudo apt-get install radosgw";
my $mkdir_rgw = "sudo mkdir -p /var/lib/ceph/radosgw/ceph-radosgw.gateway";
my $add_entry_ceph_keyring = "sudo ceph -k /etc/ceph/ceph.keyring auth add client.radosgw.gateway -i /etc/ceph/keyring.radosgw.gateway";
my $start_rgw = "sudo /etc/init.d/radosgw start";
my $user = "qa";
my $install_ap_fcgi = "yes | sudo apt-get install apache2 libapache2-mod-fastcgi" if(!exec_cmd($run_update));
my $check_en = enmod() if(!$install_ap_fcgi);
my $check_http = set_httpconf($hostname) if (!$check_en); 
my $check_apache = start_service("apache2") if (!$check_http) ;
my $check_install = exec_cmd($install_radosgw) if(!$check_apache);		
my $get_exit = append_ceph_conf($host) if(!$check_install);
my $get_exec = exec_cmd($mkdir_rgw) if(!$get_exit);	
my $get_status = create_rgw_conf() if (!$get_exec);
my $get_enstatus = ensite_dissite() if (!$get_status);
my $get_status1 = create_fcgi() if(!$get_enstatus);
my $status = generate_keyring_and_key() if (!$get_status1);
my $status_add = exec_cmd($add_entry_ceph_keyring) if(!$status);
my $status_ceph = start_service("ceph") if (!$status_add);
my $status_apache = start_service("apache2") if (!$status_ceph);
my $status_rgw = start_rgw() if (!$status_apache);
my $status_user = create_user($user) if (!$status_rgw);	
_write_log_entry("RGW installation and configuration successful!") if (!$status_user);	





