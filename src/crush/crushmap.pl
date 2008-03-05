#!/usr/bin/perl

use CrushWrapper;
use Config::General;
use Tie::DxHash;
use Data::Dumper;

tie my %conf, "Tie::DxHash";

my $wrap = new CrushWrapper::CrushWrapper;

$wrap->create();

%conf = Config::General::ParseConfig( -ConfigFile => "sample.txt", -Tie => "Tie::DxHash" );

my $arr = \%conf;
my @ritems;

#@nums = (1, -4, 11, 17, 1, -92, -15, 48);
#print "length: " . scalar(@nums) ."\n";

foreach my $type (keys %{$arr->{'type'}}) {
	$wrap->set_type_name($arr->{'type'}->{$type}->{'id'},$type);
	print $wrap->get_type_name($arr->{'type'}->{$type}->{'id'}) ."\n";
}

#first argument must be zero for auto bucket_id
my $i=0;
foreach my $osd (keys %{$arr->{'osd'}}) {
	# bucket_id, alg, type, size, items, weights
	$ritems[$i] = $wrap->add_bucket(0,4,0,0,[],[]);
	$wrap->set_item_name($ritems[$i], $osd);
	print "bucket_id: $ritems[$i]\n";
	print $wrap->get_item_name($ritems[$i]) ."\n";
	$i++;
}

foreach my $type (keys %{$arr->{'type'}}) {
	next if ($type eq 'osd');
	foreach my $bucket (keys %{$arr->{$type}}) {
		print "bucket: $bucket\n";
		#check for algorithm type
		my @item_ids;
		foreach my $item (keys %{$arr->{$type}->{'item'}}) {
			push @item_ids, $wrap->get_item_id($item);
		}
		$ritems[$i] = $wrap->add_bucket(0,4,0,scalar(@item_ids),\@item_ids,[]);
		$wrap->set_item_name($ritems[$i],$bucket);
		print $wrap->get_item_name($ritems[$i]) ."\n";
		$i++;
	}   
}



print Dumper @ritems;
print Dumper $arr;



=item

/*** BUCKETS ***/
enum {
    CRUSH_BUCKET_UNIFORM = 1,
    CRUSH_BUCKET_LIST = 2,
    CRUSH_BUCKET_TREE = 3,
    CRUSH_BUCKET_STRAW = 4
};

/*** RULES ***/
enum {
    CRUSH_RULE_NOOP = 0,
    CRUSH_RULE_TAKE = 1,          /* arg1 = value to start with */
    CRUSH_RULE_CHOOSE_FIRSTN = 2, /* arg1 = num items to pick */
                                  /* arg2 = type */
    CRUSH_RULE_CHOOSE_INDEP = 3,  /* same */
    CRUSH_RULE_EMIT = 4           /* no args */
};

=cut
