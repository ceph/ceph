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
use Pod::Usage();
use Getopt::Long();

my $help;

Getopt::Long::GetOptions(
    'help'   => \$help
);

Pod::Usage::pod2usage( -verbose => 1 ) if ($help);

my $pool_name = "rbd";

# variables
my $PASS_CNT       = 0;
my $FAIL_CNT       = 0;
my $TPASS_CNT      = 0;
my $TFAIL_CNT      = 0;
my $RADOS_MKPOOL   = "rados mkpool";
my $RADOS_RMPOOL   = "rados rmpool";
my $RBD_CREATE     = "rbd create";
my $RBD_RESIZE     = "rbd resize";
my $RBD_INFO       = "rbd info";
my $RBD_REMOVE     = "rbd rm";
my $RBD_RENAME     = "rbd rename";
my $RBD_MV         = "rbd mv";
my $RBD_LS         = "rbd ls";
my $RBD_LIST       = "rbd list";
my $RBD_CLONE      = "rbd clone";
my $RBD_EXPORT     = "rbd export";
my $RBD_IMPORT     = "rbd import";
my $RBD_COPY       = "rbd copy";
my $RBD_CP         = "rbd cp";
my $SNAP_CREATE    = "rbd snap create";
my $SNAP_LIST      = "rbd snap list";
my $SNAP_LS        = "rbd snap ls";
my $SNAP_ROLLBACK  = "rbd snap rollback";
my $SNAP_REMOVE    = "rbd snap rm";
my $SNAP_PURGE     = "rbd snap purge";
my $RBD_WATCH      = "rbd watch";
my $RBD_MAP        = "sudo rbd map";
my $RBD_UNMAP      = "sudo rbd unmap";
my $RBD_SHOWMAPPED = "rbd showmapped";
my $RADOS_LS       = "rados ls";

#====Error messages========================

my $RBD_RM_ERROR      = "image name was not specified";
my $SNAP_LS_ERROR     = "snap name was not specified";
my $SNAP_RM_ERROR     = "remove failed";
my $SNAP_ROLLBACK_ERR = "rollback failed";
my $RBD_CP_ERROR      = "error: destination image name";
my $RBD_EXISTS_ERR    = "exists";
my $RBD_RENAME_ERROR  = "rename error";
my $RBD_DELETE_ERROR  = "delete error";
my $RBD_NO_IMAGE      = "error opening image";
my $RBD_POOL_ERROR    = "error opening pool";
my $RBD_INT_ERR       = "expected integer";
my $RBD_ARG_ERR       = "requires an argument";
my $RBD_EXP_ERR       = "export error";
my $RBD_IMP_ERR       = "import failed";
my $RBD_MAP_ERR       = "add failed";
my $RBD_UNMAP_ERR     = "remove failed";
my $RBD_INFO_SNAP_ERR = "error setting snapshot context";

#=======Success messages=======================

my $POOL_MK_SUCCESS       = "successfully created pool";
my $POOL_RM_SUCCESS       = "successfully deleted pool";
my $RBD_RM_SUCCESS        = "Removing image: 100%";
my $RBD_CP_SUCCESS        = "Image copy: 100%";
my $RBD_RESIZE_SUCCESS    = "Resizing image: 100%";
my $RBD_EXP_SUCCESS       = "Exporting image: 100%";
my $RBD_IMP_SUCCESS       = "Importing image: 100%";
my $SNAP_ROLLBACK_SUCCESS = "Rolling back to snapshot: 100%";
my $SNAP_PURGE_SUCCESS    = "Removing all snapshots: 100%";

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
my $PASS_FLAG;
my $TPASS_FLAG;
my $rc = " ";
our $MSG;
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
        pass("Pool $pool_name deleted");
    }
    $exec_cmd = get_command_output("$RADOS_MKPOOL $pool_name");
    if (   ( $exec_cmd =~ /$POOL_MK_SUCCESS/ )
        || ( $exec_cmd =~ /RBD_EXISTS_ERR/ ) )
    {
        pass("Pool $pool_name created");
    }
}

sub _clean_up {
    my $exec_cmd = get_command_output(
        "rm  $img_name $rbd_imp_file $rbd_imp_test $exp_file $exp_file1" );
}

sub _pre_clean_up {
    my $exec_cmd = get_command_output(
"rm -rf logfile.txt log.txt test_completed.txt"
    );
}

sub perform_action {
    my ( $action, $cmd_args, $option ) = @_;
    my $command = frame_command( $action, $cmd_args, $option );
    my $cmd_op  = get_command_output($command);
    my $rc      = validate_cmd_output( $cmd_op, $action, $cmd_args, $option );
    return $rc;
}

sub verify_action {
    my ( $action, $cmd_args, $option ) = @_;
    my $command = frame_command( $action, $cmd_args, $option );
    my $cmd_op = get_command_output($command);
    return $cmd_op;
}

sub pass {
    my ($comment) = @_;
    print "Comment required." unless length $comment;
    chomp $comment;
    print_border2();
    print "PASS: $comment \n";
    print_border2();
    $PASS_CNT++;
    $PASS_FLAG = "TRUE";
}

sub tpass {
    my ($comment) = @_;
    print "Comment required." unless length $comment;
    chomp $comment;
    print_border4();
    print "PASS: $comment \n";
    print_border4();
    $TPASS_CNT++;
    $TPASS_FLAG = "TRUE";
    $MSG = "$comment";
}

sub debug_msg {
    my ($comment) = @_;
    print "Comment required." unless length $comment;
    chomp $comment;
    print_border2();
    print "DEBUG: $comment \n";
    print_border2();
}

sub fail {
    my ($comment) = @_;
    print "Comment required." unless length $comment;
    chomp $comment;
    print_border2();
    print "FAIL: $comment \n";
    print_border2();
    $FAIL_CNT++;
    $PASS_FLAG = "FALSE";
}

sub tfail {
    my ($comment) = @_;
    print "Comment required." unless length $comment;
    chomp $comment;
    print_border4();
    print "FAIL: $comment \n";
    print_border4();
    $TFAIL_CNT++;
    $TPASS_FLAG = "FALSE";
    $MSG = "$comment";
}

sub print_border {
    print "=" x 90 . "\n";
}

sub print_border2 {
    print "~" x 90 . "\n";
}

sub print_border4 {
    print "*" x 90 . "\n";
}

sub banner {
    my ($string) = @_;
    chomp $string;
    print_border();
    print $string, "\n";
    print_border();
}

sub display_result {
    banner("TEST RESULTS");
    banner(
"No. of test cases passed:$PASS_CNT\nNo. of test cases failed:$FAIL_CNT\n"
    );
}

sub display_func_result {
    banner("TEST RESULTS");
    banner(
"No. of test cases passed:$TPASS_CNT\nNo. of test cases failed:$TFAIL_CNT\n"
    );
}

sub get_command_output {
    my $cmd_output = shift;
    open( FH, ">>$test_log" );
    print FH "*" x 90 . "\n";
    print FH "\"$cmd_output\"\n";
    my $exec_cmd = `$cmd_output 2>&1`;
    print FH "$exec_cmd\n";
    print FH "*" x 90 . "\n";
    close(FH);
    return $exec_cmd;
}

sub frame_command {
    my ( $action, $cmd_args, $option ) = @_;
    my @command_set = split( /,/, $cmd_args );
    my $command = join( ' --', @command_set );
    $command = "$action $command";
    return $command;
}

sub check_if_listed {
    my ( $cmd, $check_arg ) = @_;
    my $check_op = get_command_output($cmd);
    if ( $check_op =~ /$check_arg/ ) {
        pass("$cmd passed");
        return 0;
    }
    else {
        fail("$cmd failed");
        return 1;
    }
}

sub check_if_not_listed {
    my ( $cmd, $check_arg ) = @_;
    my $check_op = get_command_output($cmd);
    if ( $check_op =~ /$check_arg/ ) {
        fail("$cmd failed");
        return 1;
    }
    else {
        pass("$cmd successful");
        return 0;
    }
}

sub validate_cmd_output {
    my ( $snap, $arg1 );
    my $arg = " ";
    my $MSG = " ";
    my $snaps;
    my ( $cmd_op, $act, $args, $test_flag ) = @_;
    if ( !$test_flag ) {
        if ( ( $act =~ /$RBD_CREATE/ ) && ( !$cmd_op ) ) {
            $arg = ( split /,/,  $args )[0];
            $arg = ( split /\//, $arg )[-1];
            $rc = check_if_listed( "$RBD_LS $pool_name", $arg );
            return $rc;
        }
        elsif (( ( $act =~ /$RBD_RENAME/ ) || ( $act =~ /$RBD_MV/ ) )
            && ( !$cmd_op ) )
        {
            $arg = ( split /\//, $args )[-1];
            $rc = check_if_listed( "$RBD_LS $pool_name", $arg );
            return $rc;
        }
        elsif ( ( $act =~ /$SNAP_CREATE/ ) && ( !$cmd_op ) ) {
            $snaps = ( split / /,  $args )[1];
            $arg   = ( split /\//, $args )[-1];
            $rc = check_if_listed( "$SNAP_LS $arg", $snaps );
            return $rc;
        }
        elsif ( ( $act =~ /$SNAP_REMOVE/ ) && ( !$cmd_op ) ) {
            $snaps = ( split /\@/, $args )[-1];
            $arg1  = ( split /\@/, $args )[-2];
            $arg   = ( split /\//, $arg1 )[-1];
            $rc = check_if_not_listed( "$SNAP_LS $arg", $snaps );
            return $rc;
        }
        elsif (( $act =~ /$SNAP_PURGE/ )
            && ( $cmd_op =~ /$SNAP_PURGE_SUCCESS/ ) )
        {
            pass("$act $args passed");
        }
        elsif ( $act =~ /$RBD_INFO/ ) {
            $arg = ( split /\//, $args )[-1];
            my $rbd_img_quoted = "\'$arg\'";
            pass("$act $args passed")
              if ( $cmd_op =~ /rbd image $rbd_img_quoted/ );
        }
        elsif ( ( $act =~ /$RBD_REMOVE/ ) && ( $cmd_op =~ /$RBD_RM_SUCCESS/ ) )
        {
            $rc = check_if_not_listed( "$RBD_LS $pool_name", $args );
            return $rc;
        }
        elsif (( $act =~ /$RBD_RESIZE/ )
            && ( $cmd_op =~ /$RBD_RESIZE_SUCCESS/ ) )
        {
            pass("$act $args passed");
        }
        elsif (( ( $act =~ /$RBD_COPY/ ) || ( $act =~ /$RBD_CP/ ) )
            && ( $cmd_op =~ /$RBD_CP_SUCCESS/ ) )
        {
            pass("$act $args passed");
        }
        elsif (
            ( $act =~ /$RBD_EXPORT/ )
            && (   ( $cmd_op =~ /$RBD_EXP_SUCCESS/ )
                && ( $cmd_op !~ /$RBD_EXISTS_ERR/ ) )
          )
        {
            pass("$act $args passed");
        }
        elsif (
            ( $act =~ /$RBD_IMPORT/ )
            && (   ( $cmd_op =~ /$RBD_IMP_SUCCESS/ )
                && ( $cmd_op !~ /$RBD_EXISTS_ERR/ )
                && ( $cmd_op !~ /$RBD_IMP_ERR/ ) )
          )
        {
            pass("$act $args passed");
        }
        elsif (( $act =~ /$SNAP_ROLLBACK/ )
            && ( $cmd_op =~ /$SNAP_ROLLBACK_SUCCESS/ ) )
        {
            pass("$act $args passed");
        }
        elsif ( ( $act =~ /$RBD_SHOWMAPPED/ ) && ( $cmd_op =~ /$img_name/ ) ) {
            pass("$act $args passed");
        }
        elsif ( ( $act =~ /$RBD_MAP/ ) && ( $cmd_op !~ /$RBD_MAP_ERR/ ) ) {
            pass("$act $args passed");
        }
        elsif ( ( $act =~ /$RBD_UNMAP/ ) && ( $cmd_op !~ /$RBD_UNMAP_ERR/ ) ) {
            pass("$act $args passed");
        }
        else {
            fail("$act $args failed");
            return 1;
        }
        return 0;
    }
    elsif ( ( $test_flag == 1 ) && ( $cmd_op =~ /$RBD_EXISTS_ERR/ ) ) {
        pass("Already exists: $act $args passed");
        return 0;
    }
    elsif (
        ( $test_flag == 2 )
        && (   ( $cmd_op =~ /$RBD_RENAME_ERROR/ )
            || ( $cmd_op =~ /$RBD_DELETE_ERROR/ )
            || ( $cmd_op =~ /$RBD_NO_IMAGE/ )
            || ( $cmd_op =~ /$RBD_POOL_ERROR/ )
            || ( $cmd_op =~ /$RBD_INT_ERR/ )
            || ( $cmd_op =~ /$RBD_ARG_ERR/ )
            || ( $cmd_op =~ /$RBD_RM_ERROR/ )
            || ( $cmd_op =~ /$SNAP_LS_ERROR/ )
            || ( $cmd_op =~ /$SNAP_RM_ERROR/ )
            || ( $cmd_op =~ /$RBD_CP_ERROR/ )
            || ( $cmd_op =~ /$RBD_EXP_ERR/ )
            || ( $cmd_op =~ /$RBD_IMP_ERR/ )
            || ( $cmd_op =~ /$SNAP_ROLLBACK_ERR/ )
            || ( $cmd_op =~ /$RBD_MAP_ERR/ )
            || ( $cmd_op =~ /$RBD_UNMAP_ERR/ )
            || ( $cmd_op =~ /$RBD_INFO_SNAP_ERR/ ) )
      )
    {
        pass("negative case: $act $args passed");
        return 0;
    }
    elsif ( ( $test_flag == 3 ) && ( $cmd_op =~ /usage/ ) ) {
        pass("negative case: $act $args passed");
        return 0;
    }
    else {
        fail("negative case:$act $args failed");
        return 1;
    }
}

# main starts here

banner("start tests");

# Tests for create image
sub create_image {
    perform_action( $RBD_CREATE, "$img_name,pool $pool_name,size 1024",    0 );
    perform_action( $RBD_CREATE, "$img_name_mv,pool $pool_name,size 1024", 0 );
    perform_action( $RBD_CREATE, "$img_name1,pool $pool_name,size 0,order 22",
        3 );
    perform_action( $RBD_CREATE, "$img_name1,pool $pool_name,size 0",     3 );
    perform_action( $RBD_CREATE, "$neg_img_name,pool $pool_name,size -1", 3 );
    perform_action( $RBD_CREATE, "$img_name1 pool $pool_name",            3 );
    perform_action( $RBD_CREATE, "--size 1024",                           3 );
    perform_action( $RBD_CREATE,
        "$max_img_name,pool $pool_name,size 1024000000000", 0 );
    perform_action( $RBD_CREATE, "$img_name1,pool $pool_name,size 2048,order",
        2 );
    perform_action( $RBD_CREATE, "$img_name1,pool $pool_name,size,order 22",
        2 );
}

#Tests to create snapshot
sub create_snapshots {
    perform_action( $SNAP_CREATE, "--snap $snap_name $pool_name\/$img_name",
        0 );
    perform_action( $SNAP_CREATE, "--snap $snap_name $pool_name\/$img_name",
        1 );
    perform_action( $SNAP_CREATE, "$snap_name", 2 );

#perform_action($SNAP_CREATE,"--snap $snap_name2 $pool_name\/$non_existing_img",2);
    perform_action( $SNAP_CREATE, "--snap $snap_name3 $pool_name\/$img_name",
        0 );
    perform_action( $SNAP_CREATE, "--snap $snap_name4 $pool_name\/$img_name",
        0 );
}

#Tests to rollback snapshot
sub rollback_snapshot {
    perform_action( $SNAP_ROLLBACK, "--snap $snap_name2 $pool_name\/$img_name",
        0 );
    perform_action( $SNAP_ROLLBACK,
        "--snap $rbd_snap_new $pool_name\/$img_name", 2 );
    perform_action( $SNAP_ROLLBACK,
        "--snap $snap_name $pool_name\/$new_rbd_img", 2 );
}

#Tests to purge snapshots
sub purge_snapshots {
    perform_action( $SNAP_PURGE, "$pool_name\/$img_name",    0 );
    perform_action( $SNAP_PURGE, "$pool_name\/$new_rbd_img", 2 );
}

#Tests to list snapshots for an image
sub list_snapshots {
    perform_action( $SNAP_LIST, "$pool_name\/$non_existing_img", 2 );
}

# Tests for remove snapshots
sub remove_snapshot {
    perform_action( $SNAP_REMOVE, "$pool_name\/$img_name\@$snap_name",      0 );
    perform_action( $SNAP_REMOVE, "$non_pool_name\/$img_name\@$snap_name3", 2 );
    perform_action( $SNAP_REMOVE, "$pool_name\/$img_name\@$snap_name2",     0 );
    perform_action( $SNAP_REMOVE, "$pool_name\/$non_existing_img",          2 );
    perform_action( $SNAP_REMOVE, " ",                                      2 );
}

# Tests for resize image
sub resize_image {
    perform_action( $RBD_RESIZE, "$img_name,size 1024,pool $pool_name", 0 );
    perform_action( $RBD_RESIZE, "$non_existing_img,size 1024,pool $pool_name",
        2 );
}

# Tests for list rbd image
sub list_image {
    perform_action( $RBD_LIST, "$non_pool_name", 2 );
}

# Tests to copy rbd image
sub copy_image {
    perform_action( $RBD_CP, "$pool_name\/$img_name $pool_name\/$cp_new", 0 );
    perform_action( $RBD_CP, "$pool_name\/$non_existing_img",             2 );
}

#Tests for rbd info
sub info_image {
    perform_action( $RBD_INFO, "$pool_name\/$img_name", 0 );
    perform_action( $RBD_INFO, "--snap $snap_name $pool_name\/$img_name_mv",
        2 );
    perform_action( $RBD_INFO, "--snap $no_snap $pool_name\/$img_name", 2 );
    perform_action( $RBD_INFO, "$pool_name\/$non_existing_img",         2 );
}

#Tests for rename image
sub rename_image {
    perform_action( $RBD_RENAME,
        "$pool_name\/$img_name_mv $pool_name\/$new_rbd_img", 0 );
    perform_action( $RBD_MV,
        "$pool_name\/$new_rbd_img $pool_name\/$img_name_mv", 0 );
}

# Tests for remove image
sub remove_image {

    #perform_action($RBD_REMOVE,"$pool_name\/$img_name",0);
    perform_action( $RBD_REMOVE, "$pool_name\/$new_rbd_img",         0 );
    perform_action( $RBD_REMOVE, "--pool $pool_name $rbd_imp_image", 0 );
    perform_action( $RBD_REMOVE, "-p $pool_name $cp_new",            0 );
    perform_action( $RBD_REMOVE, " ",                                2 );
}

# Tests for export rbd image
sub export_image {
    perform_action( $RBD_EXPORT, "$pool_name\/$img_name $exp_file", 0 );
    perform_action( $RBD_EXPORT, "$pool_name\/$img_name .",         2 );
    perform_action( $RBD_EXPORT, "$pool_name\/$img_name",           2 );
    perform_action( $RBD_EXPORT,
        "--snap $snap_name $pool_name\/$img_name $exp_file1", 0 );
    perform_action( $RBD_EXPORT,
        "--snap $no_snap $pool_name\/$img_name $exp_file1", 2 );
    perform_action( $RBD_EXPORT,
        "--snap $snap_name $pool_name\/$non_existing_img $exp_file2", 2 );
}

#Tests for import file to rbd image
sub import_image {
    my $i = create_test_file( $rbd_imp_file, $content );
    if ( $i == 0 ) {
        perform_action( $RBD_IMPORT, "$rbd_imp_file $rbd_imp_image", 0 );
    }
    create_test_file( "$rbd_imp_test", 0 );
    perform_action( $RBD_IMPORT, "$rbd_imp_test $pool_name\/$rbd_imp_image",
        2 );
    perform_action( $RBD_IMPORT, "$exp_file $pool_name\/$rbd_imp_image", 2 );
}

#To map rbd image to device
sub rbd_map {
    my $img_name = shift;

    # Execute "modprobe rbd"
    my $cmd = get_command_output("sudo modprobe rbd");
    if ( !$cmd ) {
        perform_action( $RBD_MAP, "$pool_name\/$img_name", 0 );
        my $ret = rbd_showmapped($img_name);
        print "ret is $ret \n";
        return $ret;

        #perform_action( $RBD_MAP, "$pool_name\/$non_existing_img", 2 );
    }
}

# To list rbd map
sub rbd_showmapped {
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
        $dev = rbd_map($img);
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
    log_results();
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

# Test Script execution result
sub log_results {
    if ( $TPASS_FLAG eq "TRUE" ) {
        open( TC, '>>test_completed.txt' );
        close(TC);
        open( TC, '>>log.txt' );
        print TC "[Success] $MSG\n";
        close(TC);
    }
    else {
        open( TC, '>>test_completed.txt' );
        close(TC);
        open( TC, '>>log.txt' );
        print TC "[Failure] $MSG\n";
        close(TC);
    }
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
                    tfail("TC2 failed : Import file to an image, resize and verify");
                }
            }
        }
    }
    perform_action( $RBD_REMOVE, $img, 0 );
    remove_test_files($file);
    remove_test_files($exp_to_file);
    log_results();
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
    my $a = perform_action( $RBD_REMOVE, "$pool_name/$new_img", 0 );
    remove_test_files($exp_to_file);
    remove_test_files($imp_from_file);
    log_results();
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
    perform_action( $RBD_REMOVE, "$new1_img", 0 );
    remove_test_files($imp_from_file);
    log_results();
}

sub imp_exp_existing {
    my ( $cmd, $args, $tc, $tc_log ) = @_;
    my $dd_if = "/dev/zero";
    my @arg   = split( / /, $args );
    my $rc2   = " ";
    my $rc_create =
      perform_action( $RBD_CREATE, "$pool_name/$arg[0],size 1024", 0 );
    if ( !$rc_create ) {
        my $cksum = create_test_file( $arg[1], $dd_if );
        print "cksum is $cksum\n";
        if ( $cksum != -1 ) {
            $rc2 = perform_action( "$cmd", "$arg[1] $pool_name/$arg[0]", 0 )
              if ( $cmd eq 'rbd import' );
            $rc2 = perform_action( "$cmd", "$pool_name/$arg[0] $arg[1]", 0 )
              if ( $cmd eq 'rbd export' );
            if ($rc2) {
                tpass("TC$tc passed: $tc_log");
            }
            else {
                tfail("TC$tc failed: $tc_log");
            }
        }
    }
    perform_action( $RBD_REMOVE, "$pool_name/$arg[0]", 0 );
    remove_test_files( $arg[1] );
    log_results();
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
    log_results();
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
    $rc = check_rados_basic_ls($img_name);
    if ( !$rc ) {
        tpass("TC1 passed: rados ls passed");
    }
    else {
        tfail("TC1 failed: rados ls failed");
    }
    log_results();
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
    log_results();
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
    log_results();
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
    log_results();
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

sub ceph_os_info
{
        my $ceph_v = get_command_output ( "ceph -v" );
        my @ceph_arr = split(" ",$ceph_v);
        $ceph_v = "Ceph Version:   $ceph_arr[2]";
        my $os_distro = get_command_output ( "lsb_release -d" );
        my @os_arr = split(":",$os_distro);
        $os_distro = "Linux Flavor:$os_arr[1]";
        return ($ceph_v, $os_distro);
}

sub display_ceph_os_info
{
        my ($vceph, $vos) = ceph_os_info();
        my $msg = "The Tests are running on";
        debug_msg ( "$msg\n$vos$vceph",1 );
        open( TC, '>>log.txt' );
        print TC "[Log] $vceph\n";
        close (TC);
}

#===Main===

my $test_img  = rand();
my $test_img1 = rand();
my $test_file = rand();
my $test_dir  = rand();
_pre_clean_up();
display_ceph_os_info();
_create_pool();
create_check_objs();
import_resize_checkobjs();
import_export( "/dev/zero", 3, $TC3_LOG );
import_rename_export();
imp_exp_existing( $RBD_EXPORT, "$test_img $test_file",  5 , $TC5_LOG);
imp_exp_existing( $RBD_IMPORT, "$test_img1 $test_file", 6 , $TC6_LOG);
import_tcs( $test_file, 7, $TC7_LOG );
import_tcs( $test_file, 8 , $TC8_LOG);
import_tcs( $test_dir,  9 , $TC9_LOG);
import_tcs( $test_file, 11, $TC11_LOG );
create_snap_resize_rollback();
create_snap_rollback();
create_delete_rollback();
display_func_result();
