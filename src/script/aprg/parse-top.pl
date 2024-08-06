#!/usr/bin/perl
#==============================================================================#
# COPYRIGHT_NOTICE_START
# IBM Confidential -- parse-ecmon.pl
# INTERNAL USE ONLY
# Manchester Lab Platform Performance
# (C) Copyright IBM Corp. 2017
# COPYRIGHT_NOTICE_END
#==============================================================================#

=pod

=head1 NAME

parse-top.pl -- parse the data from top to produce a gnuplot chart of CPU util

=head1 USAGE

  ./parse-top.pl --config=<[top_output].log> --cpu=<[range] --avg=<output.json>

  Parses top log files to produce gnuplot charts.

   --config=<top.out> : top log input file to examine.
   --pids=<pids.out> : file containing the list of pids, in the form <NAME>:<pid_list , sep>
   --cpu=<[min-max]> : range of cpus to filter.
   --avg=<output.json>: oputput file of CPU avg to produce

   Ensure your file got '.out' suffix.

=head1 METHODS

=cut

use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;
use Data::Dumper;
use List::Util qw(first max min sum);
use JSON::XS;

my %o = (); # command line options
$o{verbose} = 0;
$o{sort} = "util";
$o{timefmt} =  '"%Y-%m-%d %H:%M:%S"';
$o{cpu_filter} = 0; # Do not filter by CPU
#$o{avg_summary} = 0; # Do not produce summary by CPU

GetOptions ( \%o , 'config|c=s', 'cpu|u=s', 'avg|a=s', 'pids|p=s', 'help|?', 'verbose|v', 'nochart|n' )
  or pod2usage(
  -message  => "Invalid argument found\n",
  -verbose  => 99,
  -sections => "USAGE"
);
pod2usage(
  -verbose  => 99,
  -sections => "USAGE") if $o{help};
# Each column of the plot .dat file is a thread name
my $plot_template = <<'END';
set terminal pngcairo size 650,280 enhanced font 'Verdana,10'
set output 'OUT'
set key outside horiz bottom center box noreverse noenhanced autotitle
set datafile missing '-'
set datafile separator ","
#set timefmt "%Y-%m-%d %H:%M:%S"
set timefmt TIMEFORMAT
#set xdata time
#set format x "%Y-%m-%d %H:%M:%S"
#set format x TIMEFORMAT
#set format y "%2.3f"
set format y '%.0s%c'
set style data lines
#set xrange [ XRANGE ]
set xtics border in scale 1,0.5 nomirror rotate by -45  autojustify
#set xtics XTICS
#set xtics 3600
set title "TITLE"
set ylabel 'METRIC%'
set grid
set key autotitle columnheader

#plot 'DAT' using 1:1 title columnheader(1) w lp, for [i=3:NUMELEM] '' using i:i title columnheader(i) w lp
plot 'DAT' using 1 w lp, for [i=2:NUMELEM] '' using i w lp

#set logscale y
#set output 'GOL'
#plot 'DAT' using 1:2 title columnheader(2) w lp, for [i=3:NUMELEM] '' using 1:i title columnheader(i) w lp
END

#=======================================================#
# Naive regex to match each line of top CPU%
#
#    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
#  32399 root      20   0  353616  51760  29512 R  46.7   0.0   0:05.34 msgr-worker-0
#my $CPU = qr/\s*\d+\s+\w+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\w\s+([.\d]+)\s+[.\d]+\s+[.:\d]+\s+([-\w\d]+)/;
#my $CPU = qr/^\s+\d+\s+\w+\s+\d+\s+\d+\s+\d+.*$/;
# Columns form top that this scripts extracts: these are the keys for a sample hashref
my @key_top_columns = ( "PPID", "P", "%CPU","%MEM", "COMMAND" );
my @top_columns;

# REgex for the latest toprc
#my $CPU = qr/^\s+\d+(?#PPID)\s+\d+(?#PID)\s+\d+(?#Last CPU)\s+\d+(?# PR)\s+\d+(?# Ni)\s+(?# VIRT).*$/;
my $CPU = qr/^\s+\d+(?#PPID)\s+\d+(?#PID)\s+.*$/;
# Update: changed the toprc to show columns PPID, and last processor the thread run
#   PPID     PID  P  PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
#      1   76922 28  20   0   19164  11264   9472 S   0.0   0.0   0:00.01 sshd
#  76922   76924  3  20   0   19164   6544   4864 S   0.0   0.0   0:00.11 sshd
#      0       1  7  20   0   15824   8960   7680 S   0.0   0.0   0:00.02 sshd
my $START = qr/^\s+PPID\s+PID.*$/;
#Threads: 1027 total,
my $THREADS = qr/^Threads:\s+\d+\s+total.*$/;
# %Cpu0  : 81.1 us, 14.6 sy,  0.0 ni,  0.0 id,  0.0 wa,  0.7 hi,  3.6 si,  0.0 st     %Cpu1  : 79.9 us, 14.9 sy,  0.0 ni,  0.0 id,  0.0 wa,  0.7 hi,  4.6 si,  0.0 st
#my $CPU_pc = qr/%(Cpu\d+)\s+:(\d+\.\d+)\sus,\s+(\d+\.\d+)\ssy,/;
#my $CPU_line = qr/${CPU_pc}[^%]*${CPU_pc}$/;
# Since changed the toprc file, its now showing all cpuis in a single column
#my $CPU_line = qr/^[%](Cpu\d+)\s+:\s+(\d+\.\d+)\sus,\s+(\d+\.\d+)\ssy,[^%]*%(Cpu\d+)\s+:\s+(\d+\.\d+)\sus,\s+(\d+\.\d+)\ssy,.*/;
my $CPU_line = qr/^[%](Cpu\d+)\s+:\s+(\d+\.\d+)\sus,\s+(\d+\.\d+)\ssy,.*/;

# Format of the pid input file
my $PID_LINE = qr/^(\w+):\s+([\d,]+)$/;

# Range for the cpu cores to filter the results
# Todo:
# a number
# an interval: number dash number
# a comma separated list of numbers
my $CPU_range = {
    regex => qr/(\d+)-(\d+)/,
    min => 0,
    max => 0,
};
#=======================================================#

my $ghref = {}; # very bad name -- global hashref
my $core_href = {};
my $core_href_avg = {}; # Avg of CPU util (us, sys) per core
my $num_samples = 0;
my $num_core_samples = 0;

my $fh;
my @comms = ();

#=======================================================#
sub create_cpu_range {
    #my ($str) = @_;
    my $str = $o{cpu};
    if ( $str =~ /$CPU_range->{regex}/ )
    {
        ($CPU_range->{min}, $CPU_range->{max}) = ($1, $2);
        if ( $CPU_range->{max}< $CPU_range->{min})
        {
            my $dummy = $CPU_range->{max};
            $CPU_range->{max} = $CPU_range->{min};
            $CPU_range->{min} = $dummy;
        }
        #print Dumper($CPU_range) . "\n";
        $o{cpu_filter} = 1;
    }
}

#=======================================================#
# Given a Cpu\d+ name, return TRUE if its in the $CPU_range, false otherwise
sub is_cpu_in_range {
    # Probably verify that we get valid hash ref args, keys: comm cpu
    my ($cpu_name) = @_;

    return 1 unless $o{cpu_filter};

    if ( $cpu_name =~ qr/Cpu(\d+)/ )
    {
        my ($id) = ($1);
        return ( ($CPU_range->{min} <= $id ) &&
            ($id <= $CPU_range->{max} ));
    }
    return 0;
}

#=======================================================#
## Inserts the sample in a global table.
    # The table is organised as
    # (from the pids list, we have name:list)
    # name:
    #   (all samples for all rlevant pid+ppid)
    #
    # eg.
    # OSD
    #   reactor-1: [cpu util list], [mem util], etc
    # Given a sample:
    # if its pid is in the $pids_hr, then add the sample ppid in the $pids_hr
    # for that name group, so next sample should get parsed correctly
    # ToDO: the cpu argument could be the filename of the output of
    # ps -p <pid> -L -o pid,tid,comm,psr,pcpu
    # so we contruct a hash:
    # {command}->{thread_id}->{cpu_core}->[cpu_util]
    # I guess top provided the overall util regardless in which core the thread
    # is running when the sample was taken

sub insert_sample_hr {
    my ($href, $hr, $pids_hr) = @_;

    # For each key in the $pids_hr (that is the process names, eg OSD, FIO)
    # if the sample PID or PPID is in the list of pids for that name, then insert the sample,
    # if the sample PID is in the list, add its PPID as well for the next sample thread to be added
    # lastcpu could be used to show how many times the thread was rescheduled to different core
    foreach my $k (keys %$pids_hr)
    {
        #if (my ($matched) = grep $_ eq $hr->{pid} or $_ eq $hr->{ppid}, @{$pids_hr->{$k}})
        my %_hash = map {$_ => 1} @{$pids_hr->{$k}};
        #print Dumper(\%_hash) . "\n";
        #print Dumper($hr) . "\n";

        if (defined $_hash{$hr->{pid}} or defined $_hash{$hr->{ppid}})
        {
            if (not defined $_hash{$hr->{ppid}})
            {
                push  @{$pids_hr->{$k}}, $hr->{ppid};
            }
            foreach my $metric ('cpu', 'mem')
            {
                if (defined $href->{$k}->{$hr->{comm}}->{$metric})
                {
                    $href->{$k}->{$hr->{comm}}->{$metric}->[$num_samples-1] += $hr->{$metric};
                }
                else {
                    $href->{$k}->{$hr->{comm}}->{$metric} = [$hr->{$metric}];
                }
            }
        }
    }
    #print Dumper($href) . "\n";
}

#=======================================================#
sub insert_core_sample_hr {
    # Probably verify that we get valid hash ref args, keys: sys, us
    # Core cpu samples are global, not specific to a process
    my ($hr) = @_;

    foreach my $cpu_name (keys %{$hr})
    {
        foreach my $v (keys %{$hr->{$cpu_name}})
        {
            if (defined $core_href->{$cpu_name}->{$v})
            {
                push @{$core_href->{$cpu_name}->{$v}}, $hr->{$cpu_name}->{$v};
            }
            else {
                $core_href->{$cpu_name}->{$v} = [$hr->{$cpu_name}->{$v}];
            }
        }
    }
}
#=======================================================#
# load the pids file, produce a hash:{name:pid_list}
# Retuns a hashref with the list of pids per name (eg OSD, FIO)
sub parse_pids_files {
    die "Must provide pids file" unless ( defined $o{pids} and -f $o{pids});
    die "Could not read file $o{pids}" unless(open($fh, "<", $o{pids} ));
    my @lines = <$fh>;
    close $fh;
    die "Empty pids file" if ($#lines < 0); # no rows, empty file

    my $pids_hr = {};
    foreach my $line (@lines)
    {
        if ($line =~ m/$PID_LINE/)
        {
            my ($name, $lst) = ($1, $2);
            my @pids = split /,/, $lst;
            if (defined $pids_hr->{$name} )
            {
                push @{$pids_hr->{$name}}, @pids;
            }
            else {
                $pids_hr->{$name} = [ @pids ];
            }
        }
    }
    #print Dumper($pids_hr) . "\n";
    return $pids_hr;
}

#=======================================================#
# load the file, get all lines in an array, (slurp?) then traverse each line to get the
# COMMAND and %CPU entry in a hashref, then produce a gnuplot file
sub parse_files {
    my ($href,$pids_hr) = @_;

    die "Must provide config file" unless ( defined $o{config} and -f $o{config});
    die "Could not read file $o{config}" unless(open($fh, "<", $o{config} ));
    my @lines = <$fh>;
    close $fh;
    die "Empty file" if ($#lines < 0); # no rows, empty file

    my $core_matches=0;
    my $cpu_matches=0;
    foreach my $line (@lines)
    {
        if ($line =~ /$START/)
        {
            @top_columns = split /\s+/, $line unless scalar @top_columns;
            $num_samples++;
        }
        elsif ($line =~ /$CPU/)
        {
            my @tokens = split /\s+/, $line;
            # keys are the comms, X-axis are the %cpu util
            #insert_sample_hr({ comm => $tokens[12], tid=> $tokens[1], cpu=> $tokens[9]} );
            #insert_sample_hr({ comm => $tokens[12], cpu=> $tokens[9], mem=> $tokens[10]} );
            # New toprc config columns
            insert_sample_hr($href, { pid=> $tokens[2], ppid=> $tokens[1], comm => $tokens[13], cpu=> $tokens[10], mem=> $tokens[11], lastcpu=> $tokens[3]}, $pids_hr );
            $cpu_matches++;
        }
        elsif ($line =~ /$THREADS/)
        {
            $num_core_samples++;
        }
        elsif ($line =~ m/$CPU_line/g)
        {
            my ($cpu_l,$us_l,$sys_l) = ($1,$2,$3);
            # Single column
            insert_core_sample_hr( {
                    $cpu_l => {
                        us => $us_l,
                        sys => $sys_l,
                    }
                });
=pod
            my ($cpu_l,$us_l,$sys_l,$cpu_r,$us_r,$sys_r) = ($1,$2,$3,$4,$5,$6);
            insert_core_sample_hr( {
                    $cpu_l => {
                        us => $us_l,
                        sys => $sys_l,
                    }
                });

            insert_core_sample_hr( {
                    $cpu_r => {
                        us => $us_r,
                        sys => $sys_r,
                    }
                });

=cut
            $core_matches++;
        }
    }
    print "samples: $num_samples,  core samples: $num_core_samples,
        core matches: $core_matches, cpu_matches: $cpu_matches, lines: ". scalar @lines ."\n";
    print "top columns: " . join (',', @top_columns) . "\n";
    print Dumper($href) . "\n";
}

#=======================================================#
# Generate the set of files: .dat. plot for the $key (cpu )
# First arg is the metric (cpu, mem) and second is the group name (from pids input argument)
# ToDO: need affinity output to filter only those threads running on those cpus
sub generate_output {
    my ($g_href,$key,$pname) = @_;

    my $yaxis = { 'cpu' => "CPU", 'mem' => "MEM" };

    my $avg = {};
    my @sorted = ();
    my @top = ();
    my $href = $g_href->{$pname};

    @comms = sort (keys %{$href}); # these are the commands -- thread names
    print "Total " . scalar(@comms) . " threads\n";
    # Calculate the avg cpu util for each comm, then take the top ten
    foreach my $comm (@comms)
    {
        $avg->{$comm} = sum(@{$href->{$comm}->{$key}})/(scalar @{$href->{$comm}->{$key}}) if (defined $href->{$comm}->{$key});
    }
    @sorted = ( reverse sort {
            $avg->{$a} <=> $avg->{$b}
        } keys %{$avg} );

    print "${pname} : ${key} sorted: " . Dumper(\@sorted) . "\n";

    if (defined $o{nochart}) # skip single charts, only produce CPU avg
    {
        # Produce a single entry in the output .json
        return;
    }
# Define the ouput .dat file:
    my $outname = "${pname}_$o{config}";
    $outname =~ s/\.out/_$key/g;

    my $dat = "$outname.dat";
    my $out = "$outname.png";
    my $plot_script = "$outname.plot";
    my $outlog = "$outname-log.png";
    print "Out: $out\nDat: $dat\nPlot: $plot_script\n";

    die "Could not open file $dat for writting" unless(open($fh, ">", $dat ));

#print header of the dat file: the top items of @sorted
# need to use CSV format
    my $num_columns = 10; # top 10 comms
    my @atop  = @sorted;
    @top = splice (@atop, 0, $num_columns);
    print $fh join(',',@top)."\n";

# The command at the top of the list (more %CPU, %mem)
    foreach my $sample ( 0..$num_samples )
    {
        my @row = ();
        foreach my $comm (@top)
        {
            if (defined $href->{$comm}->{$key}->[$sample])
            {
                push @row, $href->{$comm}->{$key}->[$sample];
            }
            else{
                push @row, '-';
            }
        }

        # Save into .dat file
        print $fh join (',', @row). "\n";
    }

    close($fh);
# Produce a .plot file:
    die "Could not open file $plot_script for writting" unless(open($fh, ">", $plot_script ));

    my $title = $outname;
    $title =~ tr/_/-/;
    my $plot = $plot_template;
# Apply substition for those special strings
    $plot =~ s/DAT/$dat/g;
    $plot =~ s/OUT/$out/g;
    #$plot =~ s/GOL/$outlog/g;
    #$plot =~ s/XTICS/$xtics/g;
    #$plot =~ s/RSS/$key/g;
    #$plot =~ s/UNIT/$o{unit}/g;
    #$plot =~ s/XRANGE/$xrange/g;
    $plot =~ s/TITLE/$title/g;
    $plot =~ s/METRIC/$yaxis->{$key}/g;
    $num_columns++;
    $plot =~ s/NUMELEM/$num_columns/g;
    $plot =~ s/TIMEFORMAT/$o{timefmt}/g;

    print $fh "$plot\n";
    close($fh);
}

#=======================================================#
# Generate the set of files: .dat. plot for the cpu cores
sub generate_cores_output {
    my ($key) = @_; # us or sys
    my $avg= {};
    my @sorted = ();
    my @top = ();
    # Calculate the avg cpu util for each core
    my @aux_cpus = sort (keys %$core_href);
    # Filter the list of cpus to only those we give as a 'cpu' range argument:
    my @cpus = ();

    foreach my $core (@aux_cpus)
    {
        if (is_cpu_in_range($core))
        {
            push @cpus, $core;
        }
    }
    # Calculate avg CPU for this top file: coalesce to a single sample
    foreach my $core (@cpus)
    {
        $avg->{$core} = sum(@{$core_href->{$core}->{$key}})/(scalar @{$core_href->{$core}->{$key}});
    }
    @sorted = ( reverse sort {
            $avg->{$a} <=> $avg->{$b}
        } keys %{$avg} );
    # This file CPU summary:
    $core_href_avg->{$key} = $avg;
    print "cores sorted by $key:" . Dumper(\@sorted) . "\n";

    if (defined $o{nochart})
    {
        # skip generating this sample charts
        return;
    }
# Define the ouput .dat file:
    my $outname = $o{config};
    $outname =~ s/\.out/_$key/g;

    my $dat = "core_$outname.dat";
    my $out = "core_$outname.png";
    my $plot_script = "core_$outname.plot";
    my $outlog = "core_$outname-log.png";
    print "Out: $out, Dat: $dat, $plot_script \n";

    die "Could not open file $dat for writting" unless(open($fh, ">", $dat ));

#print header of the dat file: the top items of @sorted
    my $num_columns = 10; # top 10 cpus
    my @atop  = @sorted;
    @top = splice (@atop, 0, $num_columns);
    print $fh join(',',@top)."\n";

# The core at the top of the list (more %CPU for $key)
    foreach my $sample ( 0..$num_core_samples )
    {
        my @row = ();

        foreach my $core (@top)
        {
            if (defined $core_href->{$core}->{$key}->[$sample])
            {
                push @row, $core_href->{$core}->{$key}->[$sample];
            }
            else{
                push @row, '-';
            }
        }

        # Save into .dat file
        print $fh join (',', @row). "\n";
    }

    close($fh);
# Produce a .plot file:
    die "Could not open file $plot_script for writting" unless(open($fh, ">", $plot_script ));

    my $title = $outname;
    $title =~ tr/_/-/;
    my $plot = $plot_template;
# Apply substition for those special strings
    $plot =~ s/DAT/$dat/g;
    $plot =~ s/OUT/$out/g;
    #$plot =~ s/GOL/$outlog/g;
    #$plot =~ s/XTICS/$xtics/g;
    #$plot =~ s/RSS/$key/g;
    #$plot =~ s/UNIT/$o{unit}/g;
    #$plot =~ s/XRANGE/$xrange/g;
    $plot =~ s/TITLE/$title/g;
    $plot =~ s/METRIC/CPU/g;
    $num_columns++;
    $plot =~ s/NUMELEM/$num_columns/g;
    $plot =~ s/TIMEFORMAT/$o{timefmt}/g;

    print $fh "$plot\n";
    close($fh);
}

sub generate_cpu_avg {
    # Produce a single entry in the output .json
    my $fh;
    my $data = [];
    my $json_text = '';
    my $json = JSON::XS->new->utf8->pretty(1); #JSON->new;
    if ( defined $o{avg} and -f $o{avg} )
    {
        die "Could not open file $o{avg} for read" unless(open($fh, "+<:encoding(UTF-8)", $o{avg}));
        local $/;
        $json_text = <$fh>;
        $data = $json->decode($json_text);
        close $fh;
    }
    push @{$data}, $core_href_avg;
    my $output = $json->encode($data);
    {
        die "Could not open file $o{avg} for write" unless(open($fh, ">:encoding(UTF-8)", $o{avg}));
        print $fh $output;
        close $fh;
    }
    print "JSON:" . Dumper($output) . "\n";
}

#=======================================================#
create_cpu_range();
my $pids_hr = parse_pids_files();
parse_files($ghref, $pids_hr );
# Generate cpu and mem utilisation per thread:
    foreach my $k (keys %{$pids_hr})
    {
        print "Process name: $k\n";
        generate_output($ghref,'cpu', $k);
        generate_output($ghref, 'mem', $k);
    }

# Generate core cpu utilisation:
generate_cores_output('us');
generate_cores_output('sys');

generate_cpu_avg();
