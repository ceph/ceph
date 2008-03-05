#!/usr/bin/perl

use CrushWrapper;
use Config::General;
use Tie::DxHash;
use Data::Dumper;

tie my %conf, "Tie::DxHash";

my $wrap = new CrushWrapper::CrushWrapper;

my $alg_types = {
	uniform => 1,
	list => 2, 
	tree => 3, 
	straw => 4
};

$wrap->create();

%conf = Config::General::ParseConfig( -ConfigFile => "sample.txt", -Tie => "Tie::DxHash" );

my $arr = \%conf;
my @ritems;

#@nums = (1, -4, 11, 17, 1, -92, -15, 48);
#print "length: " . scalar(@nums) ."\n";

# find lowest id number used
sub get_lowest {
	my $item = shift;
	return unless ref $item;

	my $lowest = 0;

	if (ref $item eq 'HASH') { 
		$lowest = $item->{'id'} if $lowest > $item->{'id'};
		foreach my $key (keys %{$item}) { 
			#next if grep { $key eq $_ } qw(type rule);

			my $sublowest = get_lowest($item->{$key});
			$lowest = $sublowest if $lowest > $sublowest;
		}
	} elsif (ref $item eq 'ARRAY') { 
		foreach my $element (@{$item}) { 
			my $sublowest = get_lowest($element);
			$lowest = $sublowest if $lowest > $sublowest;
		}
	} 

	return $lowest;
}

my $lowest = get_lowest($arr);
#print "lowest is $lowest\n";

foreach my $type (keys %{$arr->{'type'}}) {
	$wrap->set_type_name($arr->{'type'}->{$type}->{'id'},$type);
	#print $wrap->get_type_name($arr->{'type'}->{$type}->{'id'}) ."\n";
}

# first argument must be zero for auto bucket_id
my $i=0;
foreach my $osd (keys %{$arr->{'osd'}}) {
	# bucket_id, alg, type, size, items, weights
	my $bucket_id = $osd->{'id'};
	$bucket_id = --$lowest if !$bucket_id;

	$ritems[$i] = $wrap->add_bucket($bucket_id, $alg_types->{'straw'}, 0, 0, [], []);
	$wrap->set_item_name($ritems[$i], $osd);
	#print "bucket_id: $ritems[$i]\n";
	#print $wrap->get_item_name($ritems[$i]) ."\n";
	$i++;
}

foreach my $type (keys %{$arr->{'type'}}) {
	next if ($type eq 'osd');
	print "doing type $type\n";
	foreach my $bucket (keys %{$arr->{$type}}) {
		#print "bucket: $bucket\n";
		# check for algorithm type
		my @item_ids;
		foreach my $item (keys %{$arr->{$type}->{'item'}}) {
			push @item_ids, $wrap->get_item_id($item);
		}
		my $bucket_id = $arr->{$type}->{$bucket}->{'id'};
		$bucket_id = --$lowest if !$bucket_id;

		


		my $alg = $alg_types->{$arr->{$type}->{$bucket}->{'alg'}};
		$alg = $alg_types->{'straw'} if !$alg;

		print "alg is: $alg\n";

		$ritems[$i] = $wrap->add_bucket($bucket_id, $alg, 0, scalar(@item_ids), \@item_ids, []);
		$wrap->set_item_name($ritems[$i],$bucket);
		#print $wrap->get_item_name($ritems[$i]) ."\n";
		$i++;
	}   
}



#print Dumper @ritems;
#print Dumper $arr;



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
