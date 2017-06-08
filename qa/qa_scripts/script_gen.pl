#!/usr/bin/perl

=head1 NAME

script_gen.pl - create a perl wrapper for the teuthology scripts  

=head1 SYNOPSIS

Use:
	perl script_gen.pl --script_name <script_name> [--help] 

Examples:
	perl script_gen.pl --script_name abc.pl or 
	perl script_gen.pl --help

=head1 DESCRIPTION

This script creates a perl wrapper in the name of script_name passed to it.
The task yaml file name and log file name
within the wrapper are modified accordingly.   

=head1 ARGUMENTS

script_gen.pl takes the following arguments:

	--help
	(optional.) Displays the usage message.

	--script_name script_name 
	(Required.) script name same as the name of the teuthology task for
	which perl wrapper is needed.

=cut

use strict;
use warnings;
use Template;

use Pod::Usage();
use Getopt::Long();

my ($help, $script_name);

Getopt::Long::GetOptions(
	'help' => \$help, 
	'script_name=s' => \$script_name);

	Pod::Usage::pod2usage( -verbose=>1 ) if ($help);

unless (defined($script_name)){
	Pod::Usage::pod2usage( -exitstatus =>2 );
}
my $sample_script = "sample.pl";
my $template =  Template->new;
my $variables = {
        script => $script_name,
};
$template->process($sample_script,$variables,$script_name);

