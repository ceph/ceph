#!/usr/bin/perl

use Config::General;
use Tie::IxHash;
use Data::Dumper;

use strict;

use CrushWrapper;

my $usage = "crushtool infile\n";
my $fn = shift @ARGV || die $usage;

my $alg_types = {
	uniform => 1,
	list => 2, 
	tree => 3, 
	straw => 4
};
my $alg_names = {
    1 => 'uniform',
    2 => 'list',
    3 => 'tree',
    4 => 'straw' };

my $wrap = new CrushWrapper::CrushWrapper;

&compile_crush($fn, "$fn.out");
&decompile_crush("$fn.out", "$fn.out.out");

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


sub decompile_crush {
    my $infn = shift @_;
    my $outfn = shift @_;

    $wrap->create();

    print "reading...\n";
    my $r = $wrap->read_from_file($infn);

    die "can't read file $infn ($r)\n" if ($r != 0);

    my $arr;

    # types
    my $types = $wrap->get_num_type_names();
    my %type_map;
    for (my $id=0; $types > 0; $id++) {
	my $name = $wrap->get_type_name($id);
	next if $name eq '';
	$types--;
	$type_map{$id} = $name;
	print "type $id is '$name'\n";
	$arr->{'types'}->{'type'}->{$name}->{'type_id'} = $id;
    }
    
    # devices
    my $max_devices = $wrap->get_max_devices();
    my %device_weight;
    my %name_map;
    for (my $id=0; $id < $max_devices; $id++) {
	my $name = $wrap->get_item_name($id);
	next if $name eq '';
	$name_map{$id} = $name;
	print "device $id '$name'\n";
	$arr->{'devices'}->{$type_map{0}}->{$name}->{'id'} = $id;
	my $off = $wrap->get_device_offload($id);
	if ($off) {
	    my $off = (0x10000 - $off) / 0x10000; 
	    $arr->{'devices'}->{$type_map{0}}->{$name}->{'offload'} = $off;
	}
    }

    # get bucket names
    my $max_buckets = $wrap->get_max_buckets();
    for (my $id=-1; $id > -1-$max_buckets; $id--) {
	my $name = $wrap->get_item_name($id);
	next if $name eq '';
	$name_map{$id} = $name;
    }

    # do buckets
    my $max_buckets = $wrap->get_max_buckets();
    for (my $id=-1; $id > -1-$max_buckets; $id--) {
	my $name = $wrap->get_item_name($id);
	next if $name eq '';
	print "bucket $id '$name'\n";
	my $alg = $wrap->get_bucket_alg($id);
	my $type = $wrap->get_bucket_type($id);
	$arr->{'buckets'}->{$type_map{$type}}->{$name}->{'id'} = $id;
	$arr->{'buckets'}->{$type_map{$type}}->{$name}->{'alg'} = $alg_names->{$alg};
	my $n = $wrap->get_bucket_size($id);
	for (my $i=0; $i<$n; $i++) {
	    my $item = $wrap->get_bucket_item($id, $i);
	    my $weight = $wrap->get_bucket_item_weight($id, $i);
	    next unless $weight;
	    $weight /= 0x10000;
	    $arr->{'buckets'}->{$type_map{$type}}->{$name}->{'item'}->{$name_map{$item}}->{'weight'} = $weight;
	}
    }

    

    print Dumper $arr;

}

sub compile_crush {
    my $infn = shift @_;
    my $outfn = shift @_;

    $wrap->create();

    tie my %conf, "Tie::IxHash";
    %conf = Config::General::ParseConfig( -ConfigFile => $fn,
					  -Tie => "Tie::IxHash",
					  -MergeDuplicateBlocks => 1 );
    
    my $arr = \%conf;
    print Dumper $arr;

    my $lowest = get_lowest($arr);
    #print "lowest is $lowest\n";
    
    my %weights;  # item id -> weight
    
    # types
    my %type_ids;
    foreach my $item_type (keys %{$arr->{'types'}->{'type'}}) {
	my $type_id = $arr->{'types'}->{'type'}->{$item_type}->{'type_id'};
	print "type $type_id '$item_type'\n";
	$type_ids{$item_type} = $type_id;
	$wrap->set_type_name($type_id, $item_type);
    }
    
    # build device table
    my %device_ids;  # name -> id
    foreach my $item_type (keys %{$arr->{'devices'}}) {
	foreach my $name (keys %{$arr->{'devices'}->{$item_type}}) {
	    my $id = $arr->{'devices'}->{$item_type}->{$name}->{'id'};
	    if (!defined $id || $id < 0) { 
		die "invalid device id for $item_type $name: id is required and must be non-negative";
	    }
	    $wrap->set_item_name($id, $name);
	    
	    my $w = $arr->{'devices'}->{$item_type}->{$name}->{'weight'};
	    $weights{$id} = $w;
	    $device_ids{$name} = $id;
	    print "device $id '$name' weight $w\n";
	}
    }
    
    # build bucket table
    my %bucket_ids;
    foreach my $bucket_type (keys %{$arr->{'buckets'}}) {
	foreach my $name (keys %{$arr->{'buckets'}->{$bucket_type}}) {
	    # verify type
	    unless (defined $type_ids{$bucket_type}) {
		die "invalid bucket type $bucket_type\n";
	    }
	    
	    # id
	    my $id = $arr->{'buckets'}->{$bucket_type}->{$name}->{'id'};
	    if (defined $id && $id > -1) { 
		die "invalid bucket id for $bucket_type $name: id must be negative";
	    } elsif (!defined $id) { 
		# get the next lower ID number and inject it into the config hash
		$id = --$lowest;
		$arr->{'buckets'}->{$bucket_type}->{$name}->{'id'} = $id;
	    }
	    
	    $wrap->set_item_name($id, $name);
	    $bucket_ids{$name} = $id;
	    
	    my @item_ids;
	    my @weights;
	    my $myweight;
	    foreach my $item_name (keys %{$arr->{'buckets'}->{$bucket_type}->{$name}->{'item'}}) {
		my $id = $wrap->get_item_id($item_name);
		push @item_ids, $id;
		my $weight = $arr->{'buckets'}->{$bucket_type}->{$name}->{'item'}->{$item_name}->{'weight'};
		$weight ||= $weights{$id};
		push(@weights, $weight * 65536);  # 16.16 fixed point
		$myweight += $weight;
	    }
	    
	    my $alg = $arr->{'buckets'}->{$bucket_type}->{$name}->{'alg'};
	    $alg = 'straw' if !$alg;
	    die "invalid bucket alg $alg\n"
		unless $alg_types->{$alg};
	    
	    my $typeid = $type_ids{$bucket_type};
	    my $algid = $alg_types->{$alg};
	    print "\tid $id\n";
	    print "\talg $alg ($algid)\n";
	    print "\ttype $bucket_type ($typeid)\n";
	    print "\titems @item_ids\n";
	    print "\tweights @weights\n";
	    
	    # id, alg, type, size, items, weights
	    #TODO: pass the correct value for type to add_bucket
	    my $result = $wrap->add_bucket($id, $algid, $typeid,
				       scalar(@item_ids), \@item_ids, \@weights);
	    #print "\t.. $result\n\n";
	    print "\tweight $myweight\n";
	    $weights{$id} = $myweight;
	}
    }

    # rules
    for my $rule_name (keys %{$arr->{'rules'}->{'rule'}}) {
	my $r = $arr->{'rules'}->{'rule'}->{$rule_name};
	my $pool = $r->{'pool'};
	my $typeid = $rule_types{$r->{'type'}};
	my $min_size = $r->{'min_size'};
	my $max_size = $r->{'max_size'};
		
    }

    $wrap->finalize;
    $wrap->write_to_file($outfn);
    1;
}



print "Line: " . __LINE__ ."\n";


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
