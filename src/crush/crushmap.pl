#!/usr/bin/perl

use CrushWrapper;
use Config::General;
use Tie::DxHash;
tie my %conf, "Tie::DxHash";

my $wrap = new CrushWrapper::CrushWrapper;

$wrap->create();

%conf = Config::General::ParseConfig( -ConfigFile => "sample.txt", -Tie => "Tie::DxHash" );

my $arr = \%conf;

use Data::Dumper;

print Dumper $arr;


=item
@nums = (
    1, -4, 11, 17, 1, -92, -15, 48
    );

print "length: " . scalar(@nums) ."\n";
($result, $min, $max, $avg) = $wrap->add_bucket('1','1','1',scalar(@nums),\@nums,\@nums);

print join(" : ", ($result, $min, $max, $avg) );
print "\n";
=cut


