#!/usr/bin/perl -w

use strict;
use IO::Dir;
use IO::File;

my @files;
my $file_handle;
my @file_data;

#opendir(DIR, ".");
my $pathname = "/projects/ssrc/traces-organized/LLNL-2003/tools/IOR2/traces_fileperproc";
opendir(DIR, $pathname);
@files = readdir(DIR);
closedir(DIR);

for $file_handle(@files) {
    if($file_handle =~ /IOR/ and $file_handle !~ /cephtrace/) {
	#print "$pathname/$file_handle";
	open(FILE, "$pathname/$file_handle") or die "cannot_open_file: $!";
	flock(FILE, 2);
	open(OUT, ">IOR2/$file_handle.cephtrace") or die "cannot_open_file: $!";
	flock(OUT, 2);
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
	    elsif ($op =~ /read\(/) {
		my $before;
		my $after;
		my $rest;
		my $read_size;
		my @replies;
		($before, $after) = split(/\(/, $op);
		($ret_val, $rest) = split(/,/, $after);
		($before, $after) = split(/\) *=/, $line);
		@replies = split(/\s/, $after);
		$read_size = $replies[1];

		# FIXME hack so reads aren't empty
		print OUT "write\n$ret_val\n$read_size\n-1\n";
		print OUT "lseek\n$ret_val\n$read_size\n1\n";
		print OUT "read\n$ret_val\n$read_size\n-1\n";
	    }
	    elsif ($op =~ /write/) {
		my $before;
		my $after;
		my $rest;
		my $write_size;
		my @replies;
		($before, $after) = split(/\(/, $op);
		($ret_val, $rest) = split(/,/, $after);
		($before, $after) = split(/\) *=/, $line);
		@replies = split(/\s/, $after);
		$write_size = $replies[1];

		print OUT "write\n$ret_val\n$write_size\n-1\n";
	    }
	}
	close(FILE);
	close(OUT);
    }
}

