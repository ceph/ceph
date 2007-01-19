#!/usr/bin/perl

use strict;
my $fn = shift @ARGV;
my $f = `cat $fn`;

my $header = '// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

';

unless ($f =~ /Ceph - scalable distributed file system/) {
	open(O, ">$fn.new");
	print O $header;
	print O $f;
	close O;
	rename "$fn.new", $fn;
}

