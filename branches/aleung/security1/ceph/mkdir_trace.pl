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

open(OUT, ">tracepaths.cephtrace") or die "cannot_open_file: $!";
flock(OUT, 2);

for $file_handle(@files) {
    if($file_handle =~ /IOR/ and $file_handle !~ /cephtrace/) {
	open(FILE, $file_handle) or die "cannot_open_file: $!";
	flock(FILE, 2);
	
	@file_data = <FILE>;

	my $line;
	my @args;
	foreach $line (@file_data) {
	    my $op;
	   
	    @args = split(/\s/, $line);
	    $op = $args[1];

	    if($op =~ /open\(/) {
		
		my $before;
		my $path;
		my $after;
		($before, $path, $after) = split(/\"/, $op);

		my $handle;
		my @rest;
		($handle, @rest) = reverse split(/\//, $path);
		
		my $subdir;
		my $dirpath;
		foreach $subdir (reverse @rest) {
		    if ($dirpath) {
			$dirpath = "$dirpath/$subdir";
		    }
		    else {
			$dirpath = "$subdir";
		    }
		    #print OUT "dirpath=$dirpath\n";
		    if ($dirpath) {
			if (! $seen_paths{$dirpath}) {
			    $seen_paths{$dirpath} = 1;
			    print OUT "mkdir\n\/$dirpath\n493\n";
			}
		    }
		}

	    }
	}
	
    }
}
close(OUT);
