#!/usr/bin/perl -w

use strict;
use IO::Dir;
use IO::File;

my @files;
my $file_handle;
my @file_data;
my %seen_paths = ();

opendir(DIR, ".");
@files = readdir(DIR);
closedir(DIR);

open(OUT, ">openclose.cephtrace") or die "cannot_open_file: $!";
flock(OUT, 2);

for $file_handle(@files) {
    if($file_handle =~ /IOR/ and $file_handle !~ /cephtrace/) {
	open(FILE, $file_handle) or die "cannot_open_file: $!";
	flock(FILE, 2);
	
	@file_data = <FILE>;

	my $line;
	my @args;
	foreach $line (@file_data) {
	    
	    my $timestamp;
	    my $op;
	    my $ret_val;
	    @args = split(/\s/, $line);

	    $timestamp = $args[0];
	    $op = $args[1];

	    my $path;
	    my $rest;
	    if ($op =~ /open\(/) {
		# check success
		my $before_eq;
		my $after_eq;
		my @replies;
		($before_eq, $after_eq) = split(/=/, $line);
		@replies = split(/\s/, $after_eq);
		$ret_val = $replies[1];
		
		if ($ret_val != -1) {
		    ($op, $path, $rest) = split(/\"/, $op);
		    print OUT "open\n$path\n102\n$ret_val\n";
		}
	    }
	    elsif ($op =~ /close\(/) {
		my $nothing;
		my $rparen;
		my $rest;
		($nothing, $rparen) = split(/\(/, $op);
		($ret_val, $rest) = split(/\)/, $rparen);
		print OUT "close\n$ret_val\n";
	    }
	}
	close(FILE);
    }
}
close(OUT);
