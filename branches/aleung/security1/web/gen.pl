#!/usr/bin/perl

use strict;
my $bodyf = shift @ARGV;
my $templatef = 'template.html';

open(O, $bodyf);
my $body = join('',<O>);
close O;
open(O, $templatef);
my $template = join('',<O>);
close O;
$template =~ s/--body--/$body/;
print $template;
