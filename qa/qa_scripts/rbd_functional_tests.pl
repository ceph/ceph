#! /usr/bin/perl

=head1 NAME

rbd_functional_tests.pl - Script to test the RBD functionality.  

=head1 SYNOPSIS

Use:
        perl rbd_functional_tests.pl [--help]

Examples:
        perl rbd_functional_tests.pl  
	or
	perl rbd_functional_tests.pl  --help

=head1 ARGUMENTS

rbd_functional_tests.pl takes the following arguments:
        --help
        (optional) Displays the usage message.

If cephx is enabled, set 'export CEPH_ARGS="--keyring /etc/ceph/ceph.keyring --id <user>"'
and execute the script as root.

For Example,for "nova" user, 'export CEPH_ARGS="--keyring /etc/ceph/ceph.keyring --id nova"'

=cut

use strict;
use warnings;
use Cwd;
use RbdLib qw(perform_action log_results display_result _pre_clean_up _post_clean_up _create_rados_pool display_ceph_os_info $RADOS_MKPOOL $RADOS_RMPOOL $RBD_CREATE $RBD_RESIZE $RBD_INFO $RBD_REMOVE $RBD_RENAME $RBD_MV $RBD_LS $RBD_LIST $RBD_CLONE $RBD_EXPORT $RBD_IMPORT $RBD_CP $RBD_COPY $SNAP_CREATE $SNAP_LS $SNAP_LIST $SNAP_ROLLBACK $SNAP_PURGE $SNAP_REMOVE $POOL_RM_SUCCESS $POOL_MK_SUCCESS $RBD_EXISTS_ERR $RBD_WATCH $RBD_MAP $RBD_UNMAP $RBD_SHOWMAPPED $RADOS_LS get_command_output verify_action debug_msg tpass tfail display_func_result $CLI_FLAG);

use Pod::Usage();
use Getopt::Long();

my $help;

Getopt::Long::GetOptions(
    'help'   => \$help
);

Pod::Usage::pod2usage( -verbose => 1 ) if ($help);

my $pool_name = "rbd";

#===========Variables used in the script========

my $img_name         = "test_img";
my $snap_name        = "snap1";
my $snap_name2       = "snap2";
my $snap_name3       = "snap3";
my $snap_name4       = "snap4";
my $new_rbd_img      = "new_rbd_img";
my $non_existing_img = "rbdimage";
my $cp_new           = "new";
my $exp_file         = "rbd_test_file1";
my $exp_file1        = "rbd_test_file2";
my $exp_file2        = "rbd_test_file3";
my $rbd_imp_file     = "test_file";
my $rbd_imp_image    = "new_imp_img";
my $content          = "This is a test file";
my $rbd_snap_new     = "new";
my $neg_img_name     = "neg_img";
my $max_img_name     = "max_img";
my $img_name1        = "test_img1";
my $rbd_imp_test     = "new_test_file";
my $non_pool_name    = "no_pool";
my $no_snap          = "no_snap";
my $img_name_mv      = "new_img_mv";
my $test_log         = "logfile.txt";
my $success          = "test_completed.txt";
my $fail             = "log.txt";
my $obj_initial      = "000000000000";
my $obj_second       = "000000000001";
my $exec_cmd;
our $MSG;
our $CLI_FLAG = "FALSE";
my $TC3_LOG = "verify import of file to rbd image and export the same image to file";
my $TC5_LOG = "Export rbd image to already existing non-empty file";
my $TC6_LOG = "Import file to an existing rbd image";
my $TC7_LOG = "verify import of an empty local file to rbd image";  
my $TC8_LOG = "verify import of nonexisting file to rbd image";
my $TC9_LOG = "verify import of a directory to rbd image";
my $TC11_LOG = "verify export from an non-existing rbd image"; 

sub _create_pool {
    $exec_cmd = get_command_output("$RADOS_RMPOOL $pool_name");
    if (   ( $exec_cmd =~ /$POOL_RM_SUCCESS/ )
        || ( $exec_cmd =~ /does not exist/ ) )
    {
        debug_msg ("Pool $pool_name deleted");
    }
    $exec_cmd = get_command_output("$RADOS_MKPOOL $pool_name");
    if (   ( $exec_cmd =~ /$POOL_MK_SUCCESS/ )
        || ( $exec_cmd =~ /RBD_EXISTS_ERR/ ) )
    {
        debug_msg ("Pool $pool_name created");
    }
}

#To map rbd image to device
sub rbd_mapp {
    my $img_name = shift;

    # Execute "modprobe rbd"
    my $cmd = get_command_output("sudo modprobe rbd");
    if ( !$cmd ) {
        perform_action( $RBD_MAP, "$pool_name\/$img_name", 0 );
        my $ret = rbd_showmapped1($img_name);
        print "ret is $ret \n";
        return $ret;

        #perform_action( $RBD_MAP, "$pool_name\/$non_existing_img", 2 );
    }
}

# To list rbd map
sub rbd_showmapped1 {
    my $img     = shift;
    my $ret_map = get_command_output($RBD_SHOWMAPPED);
    my @lines   = split( /\n/, $ret_map );
    shift(@lines);
    foreach (@lines) {
        if ( $_ =~ /(\d+)\s+(\S+)\s+($img)\s+\-\s+(\S+)/ ) {
            print "match $2 and $3  and $4 \n";
            return $4;
        }
    }
    return 0;
}

# To unmap rbd device
sub rbd_unmap {
    perform_action( $RBD_UNMAP, "/dev/rbd0", 0 );
    sleep(10);
    perform_action( $RBD_UNMAP, "/dev/rbd10", 2 );
}

# To unmap rbd device
sub rbd_unmap_dev {
    my $dev = shift;
    my $rcode = perform_action( $RBD_UNMAP, $dev, 0 );
    return $rcode;
}

# Delete the RBD image used by the client
sub del_mapped_img {
    debug_msg("start of test");
    my $img = rand();
    my $dev;
    my $rc_create =
      perform_action( $RBD_CREATE, "$img,pool $pool_name,size 100", 0 );
    if ( !$rc_create ) {
        $dev = rbd_mapp($img);
        if ($dev) {
            my $rc_del = perform_action( $RBD_REMOVE, "$pool_name\/$img", 0 );
            if ($rc_del) {
                tpass("TC22 - Delete mapped rbd img");
                perform_action( $RBD_UNMAP, $dev, 0 );
                perform_action( $RBD_REMOVE, "$pool_name\/$img", 0 );
            }
            else {
                tfail("TC22 - Delete mapped rbd img");
            }
        }
    }
    debug_msg("end of test");
    #log_results();
}

# To create a test file and write to it
sub create_test_file {
    my ( $test_file, $dd_ifile ) = @_;
    print "dd is $dd_ifile \n";
    my $command = "dd if=$dd_ifile of=$test_file bs=1024 count=5000";
    my $cmd_op  = get_command_output($command);
    if ( $cmd_op =~ /records/ ) {
        my $csum = get_cksum($test_file);
        return $csum;
    }
    else {
        return -1;
    }
}

# get cksum of given file
sub get_cksum {
    my ($t_file) = @_;
    my $csum = get_command_output("cksum  $t_file");
    my $cksum = ( split / /, $csum )[0];
    return $cksum;
}

#import file to rbd image and verify if objects created
sub import_verify_objs {
    my ( $img, $file ) = @_;
    my $rc = perform_action( $RBD_IMPORT, "$file $img", 0 );
    if ( !$rc ) {
        my $rc1 = check_rados_objs($img);
        debug_msg("import $file $img passed") && return 0 if ( !$rc1 );
    }
    debug_msg("import $file $img failed");
    return 1;
}

sub import_resize_checkobjs {
    my $img         = "image1";
    my $file        = "imp_file";
    my $dd_if       = "/dev/zero";
    my $exp_to_file = "/tmp/new";
    my $rcc         = create_test_file( $file, $dd_if );
    if ( $rcc != -1 ) {
        my $rc1 = import_verify_objs( $img, $file );
        if ( !$rc1 ) {
            my $rc2 =
              perform_action( $RBD_RESIZE, "$pool_name\/$img --size 0", 0 );
            if ( !$rc2 ) {
                my $rc3 = check_rados_objs($img);
                if ($rc3) {
                    tpass("TC2 passed: Import file to an image, resize image and verify");

=head - this one hangs 	
					my $rc2 = perform_action( $RBD_EXPORT,"$pool_name\/$img $exp_to_file",0);
					if ($rc2) {
						tpass( "TC12 Passed - Export image of size 0" );	
					} else {
						tfail( "TC12 Failed - Export image of size 0" );	
					}
=cut

                }
                else {
                    tfail("TC2 failed: Import file to an image, resize and verify");
                }
            }
        }
    }
    perform_action( $RBD_REMOVE, "$pool_name\/$img", 0 );
    remove_test_files($file);
    remove_test_files($exp_to_file);
}

sub remove_test_files {
    my $file = shift;
    my $cmd  = get_command_output("rm -rf $file");
    if ( $cmd !~ /./ ) {
        debug_msg("$file deleted");
    }
    else {
        debug_msg("$file NOT deleted");
    }
}

sub import_export {
    my $new_img       = "testin";
    my $imp_from_file = "/tmp/file1";
    my $exp_to_file   = "/tmp/file2";
    my ( $dd_ip_file, $tc, $tc_log ) = @_;
    my $csum = create_test_file( $imp_from_file, $dd_ip_file );
    if ( $csum != -1 ) {
        my $rc1 = import_verify_objs( $new_img, $imp_from_file );
        if ( !$rc1 ) {
            my $rc2 = perform_action( $RBD_EXPORT, "$new_img $exp_to_file", 0 );
            my $csum2 = get_cksum($exp_to_file);
            if ( !$rc2 ) {
                if   ( $csum == $csum2 ) { tpass("TC$tc passed: $tc_log"); }
                else                     { tfail("TC$tc failed: $tc_log") }
            }
        }
    }
    #log_results();
    my $a = perform_action( $RBD_REMOVE, "$pool_name\/$new_img", 0 );
    remove_test_files($exp_to_file);
    remove_test_files($imp_from_file);
}

sub import_rename_export {
    my $new_img       = "testing_img1";
    my $new1_img      = "testing_img2";
    my $imp_from_file = "/tmp/file1";
    my $dd_ifile      = "/dev/zero";
    my $exp_to_file   = "/tmp/file2";
    my $csum          = create_test_file( $imp_from_file, $dd_ifile );
    if ( $csum != -1 ) {
        my $rc1 = import_verify_objs( $new_img, $imp_from_file );
        my $rc_mv =
          perform_action( $RBD_RENAME,
            "$pool_name/$new_img $pool_name/$new1_img", 0 )
          if ( !$rc1 );
        if ( !$rc_mv ) {
            my $rc2 = perform_action( $RBD_EXPORT, "$new_img $exp_to_file", 0 );
            if ($rc2) {
                tpass("TC4 passed: verify import file to an image , rename image and export image");
            }
            else {
                tfail("TC4 failed: verify import file to an image , rename image and export image");
            }
        }
    }
    #log_results();
    perform_action( $RBD_REMOVE, "$pool_name\/$new1_img", 0 );
    remove_test_files($imp_from_file);
}

sub imp_exp_existing {
    my ( $cmd, $imgg, $file, $tc, $tc_log ) = @_;
    my $dd_if = "/dev/zero";
    my $rc2   = " ";
    my $rc_create = perform_action( $RBD_CREATE, "$imgg, pool $pool_name, size 1024", 0 );
    print ":rc is $rc_create \n";
    if ( !$rc_create ) {
        my $cksum = create_test_file( $file, $dd_if );
        if ( $cksum != -1 ) {
            $rc2 = perform_action( "$cmd", "$file $pool_name\/$imgg", 0 )
              if ( $cmd eq 'rbd import' );
            $rc2 = perform_action( "$cmd", "$pool_name\/$imgg $file", 0 )
              if ( $cmd eq 'rbd export' );
            if ($rc2) {
                tpass("TC$tc passed: $tc_log");
            }
            else {
                tfail("TC$tc failed: $tc_log");
            }
        }
    }
    perform_action( $RBD_REMOVE, "$pool_name\/$imgg", 0 );
    remove_test_files( $file );
}

sub import_tcs {
    my $new_img = rand();
    my $rc1;
    my ( $option, $tc, $tc_log ) = @_;
    get_command_output("touch $option") if ( $tc == 7 );
    get_command_output("mkdir $option") if ( $tc == 9 );
    if ( ( $tc == 7 ) || ( $tc == 9 ) || ( $tc == 8 ) ) {
        $rc1 = perform_action( $RBD_IMPORT, "$option $new_img", 0 );
    }
    elsif ( $tc == 11 ) {
        $rc1 = perform_action( $RBD_EXPORT, "$new_img $option", 0 );
    }
    if ($rc1) {
        tpass("TC$tc passed: $tc_log");
    }
    else {
        tfail("TC$tc failed: $tc_log");
    }
    remove_test_files($option);
    #log_results();
}

sub get_prefix_obj {
    my $img          = shift;
    my $rbd_info_op  = verify_action( $RBD_INFO, "$img", 0 );
    my @get_prefix_l = split( "\n", $rbd_info_op );
    my $get_prefix   = ( split /:/, $get_prefix_l[3] )[-1];
    $get_prefix =~ s/^\s+//;
    return $get_prefix;
}

sub get_img_size {
    my $img         = shift;
    my $rbd_info_op = verify_action( $RBD_INFO, "$img", 0 );
    my @get_size_l  = split( "\n", $rbd_info_op );
    my $get_size    = ( split / /, $get_size_l[1] )[1];
    return $get_size;
}

sub check_rados_basic_ls {
    my ($image) = @_;
    my $rbd_image = "$image\.rbd";
    my $cmd_output = verify_action( $RADOS_LS, "-p $pool_name", 0 );
    if (   ( $cmd_output =~ /rbd_directory/ )
        && ( $cmd_output =~ /$rbd_image/ ) )
    {
        return 0;
    }
    else {
        return 1;
    }
}

sub check_rados_objs {
    my ($img)          = @_;
    my $get_obj_prefix = get_prefix_obj($img);
    my $obj1           = "$get_obj_prefix\.$obj_initial";
    my $obj2           = "$get_obj_prefix\.$obj_second";
    my @get_rados_objs = verify_action( $RADOS_LS, "-p $pool_name", 0 );
    if (   ( grep /$obj1/, @get_rados_objs )
        && ( grep /$obj2/, @get_rados_objs ) )
    {
        return 0;
    }
    return 1;
}

sub create_check_objs {
    my $cmd_rc =
      perform_action( $RBD_CREATE, "$img_name,pool $pool_name,size 1024", 0 );
    my $rc = check_rados_basic_ls($img_name);
    if ( !$rc ) {
        tpass("TC1 passed: rados ls passed");
    }
    else {
        tfail("TC1 failed: rados ls failed");
    }
    #log_results();
}

sub create_snap_resize_rollback {
    my $img = rand();
    my $snp = rand();
    debug_msg("Start of Test");
    my $rc_c = create_snapshot( $img, $snp );
    if ( !$rc_c ) {
        my $rc1 =
          perform_action( $RBD_RESIZE, "$pool_name\/$img --size 50", 0 );
        if ( !$rc1 ) {
            my $rc_sr = rollback_snapshots( $img, $snp );
            if ( !$rc_sr ) {
                tpass("TC13 passed: create,resize and rollback");
                perform_action( $SNAP_PURGE, "$pool_name\/$img", 0 );
                perform_action( $RBD_REMOVE, "$pool_name\/$img", 0 );
            }
            else {
                tfail("TC13 failed: create,resize and rollback");
            }
        }
    }
    debug_msg("End of the Test");
    #log_results();
}

sub create_snap_rollback {
    my $img = rand();
    my $snp = rand();
    debug_msg("Start of Test");
    my $rc_c = create_snapshot( $img, $snp );
    if ( !$rc_c ) {
        my $rc_sr = rollback_snapshots( $img, $snp );
        if ( !$rc_sr ) {
            tpass("TC15 passed: create snapshot and rollback snapshot immediately");
            perform_action( $SNAP_PURGE, "$pool_name\/$img", 0 );
            perform_action( $RBD_REMOVE, "$pool_name\/$img", 0 );
        }
        else {
            tfail("TC15 failed: create snapshot and rollback snapshot immediately");
        }
    }
    debug_msg("End of the Test");
    #log_results();
}

sub create_delete_rollback {
    my $img = rand();
    my $snp = rand();
    debug_msg("Start of Test");
    my $rc_c = create_snapshot( $img, $snp );
    if ( !$rc_c ) {
        my $rc1 = perform_action( $SNAP_REMOVE, "$pool_name\/$img\@$snp", 0 );
        if ( !$rc1 ) {
            my $rc_sr = rollback_snapshots( $img, $snp );
            if ($rc_sr) {
                tpass("TC16 passed: create,delete and rollback");
                my $rc_srm =
                  perform_action( $SNAP_PURGE, "$pool_name\/$img", 2 );
                if ($rc_srm) {
                    tpass("TC17 passed: snap purge on image with no snapshots");
                }
                else {
                    tfail("TC17 passed: snap purge on image with no snapshot failed");
                }
            }
            else {
                tfail("TC16 failed: create,delete and rollback");
            }
        }
    }
    my $rc_rm = perform_action( $RBD_REMOVE, "$pool_name\/$img", 0 );
    debug_msg("End of the Test");
    #log_results();
}

sub create_snapshot {
    my ( $image, $snap ) = @_;
    my $rc_create =
      perform_action( $RBD_CREATE, "$image,pool $pool_name,size 100", 0 );
    if ( !$rc_create ) {
        my $rc_snap =
          perform_action( $SNAP_CREATE, "--snap $snap $pool_name\/$image", 0 );
        return $rc_snap;
    }
}

sub rollback_snapshots {
    my ( $img, $snap ) = @_;
    my $rc_sr =
      perform_action( $SNAP_ROLLBACK, "--snap $snap $pool_name\/$img", 0 );
    if ( !$rc_sr ) {
        my $img_size1 = get_img_size($img);
        if ( $img_size1 == 102400 ) {
            return 0;
        }
    }
    return 1;
}

#===Main===

my $test_img  = rand();
my $test_file = rand();
my $test_dir  = rand();
_pre_clean_up();
display_ceph_os_info();
_create_pool();
create_check_objs();
import_resize_checkobjs();
import_export( "/dev/zero", 3, $TC3_LOG );
import_rename_export();
imp_exp_existing( $RBD_EXPORT, $test_img, $test_file,  5 , $TC5_LOG);
imp_exp_existing( $RBD_IMPORT, $test_img, $test_file, 6 , $TC6_LOG);
import_tcs( $test_file, 7, $TC7_LOG );
import_tcs( $test_file, 8 , $TC8_LOG);
import_tcs( $test_dir,  9 , $TC9_LOG);
import_tcs( $test_file, 11, $TC11_LOG );
#del_mapped_img ()  dont execute this when cluster and client is on the same m/c
create_snap_resize_rollback();
create_snap_rollback();
create_delete_rollback();
display_func_result();
