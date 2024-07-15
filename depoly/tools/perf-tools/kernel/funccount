#!/bin/bash
#
# funccount - count kernel function calls matching specified wildcards.
#             Uses Linux ftrace.
#
# This is a proof of concept using Linux ftrace capabilities on older kernels,
# and works by using function profiling: in-kernel counters.
#
# USAGE: funccount [-hT] [-i secs] [-d secs] [-t top] funcstring
#    eg,
#        funccount 'ext3*'	# count all ext3* kernel function calls
#
# Run "funccount -h" for full usage.
#
# WARNING: This uses dynamic tracing of kernel functions, and could cause
# kernel panics or freezes. Test, and know what you are doing, before use.
#
# REQUIREMENTS: CONFIG_FUNCTION_PROFILER, awk.
#
# From perf-tools: https://github.com/brendangregg/perf-tools
#
# COPYRIGHT: Copyright (c) 2014 Brendan Gregg.
#
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License
#  as published by the Free Software Foundation; either version 2
#  of the License, or (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software Foundation,
#  Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
#
#  (http://www.gnu.org/copyleft/gpl.html)
#
# 12-Jul-2014	Brendan Gregg	Created this.

### default variables
tracing=/sys/kernel/debug/tracing
opt_duration=0; duration=; opt_interval=0; interval=999999; opt_timestamp=0
opt_tail=0; tcmd=cat; ttext=
trap 'quit=1' INT QUIT TERM PIPE HUP	# sends execution to end tracing section

function usage {
	cat <<-END >&2
	USAGE: funccount [-hT] [-i secs] [-d secs] [-t top] funcstring
	                 -d seconds      # total duration of trace
	                 -h              # this usage message
	                 -i seconds      # interval summary
	                 -t top          # show top num entries only
	                 -T              # include timestamp (for -i)
	  eg,
	       funccount 'vfs*'          # trace all funcs that match "vfs*"
	       funccount -d 5 'tcp*'     # trace "tcp*" funcs for 5 seconds
	       funccount -t 10 'ext3*'   # show top 10 "ext3*" funcs
	       funccount -i 1 'ext3*'    # summary every 1 second
	       funccount -i 1 -d 5 'ext3*' # 5 x 1 second summaries

	See the man page and example file for more info.
END
	exit
}

function warn {
	if ! eval "$@"; then
		echo >&2 "WARNING: command failed \"$@\""
	fi
}

function die {
	echo >&2 "$@"
	exit 1
}

### process options
while getopts d:hi:t:T opt
do
	case $opt in
	d)	opt_duration=1; duration=$OPTARG ;;
	i)	opt_interval=1; interval=$OPTARG ;;
	t)	opt_tail=1; tnum=$OPTARG ;;
	T)	opt_timestamp=1 ;;
	h|?)	usage ;;
	esac
done
shift $(( $OPTIND - 1 ))

### option logic
(( $# == 0 )) && usage
funcs="$1"
if (( opt_tail )); then
	tcmd="tail -$tnum"
	ttext=" Top $tnum only."
fi
if (( opt_duration )); then
	echo "Tracing \"$funcs\" for $duration seconds.$ttext.."
else
	echo "Tracing \"$funcs\".$ttext.. Ctrl-C to end."
fi
(( opt_duration && !opt_interval )) && interval=$duration

### check permissions
cd $tracing || die "ERROR: accessing tracing. Root user? Kernel has FTRACE?
    debugfs mounted? (mount -t debugfs debugfs /sys/kernel/debug)"

### enable tracing
sysctl -q kernel.ftrace_enabled=1	# doesn't set exit status
echo "$funcs" > set_ftrace_filter || die "ERROR: enabling \"$funcs\". Exiting."
warn "echo nop > current_tracer"
if ! echo 1 > function_profile_enabled; then
	echo > set_ftrace_filter
	die "ERROR: enabling function profiling."\
	    "Have CONFIG_FUNCTION_PROFILER? Exiting."
fi

### summarize
quit=0; secs=0
while (( !quit && (!opt_duration || secs < duration) )); do
	(( secs += interval ))
	echo 0 > function_profile_enabled
	echo 1 > function_profile_enabled
	sleep $interval

	echo
	(( opt_timestamp )) && date
	printf "%-30s %8s\n" "FUNC" "COUNT"

	cat trace_stat/function* | awk '
	# skip headers by matching on the numeric hit column
	$2 ~ /[0-9]/ { a[$1] += $2 }
	END {
		for (k in a) {
			printf "%-30s %8d\n", k,  a[k]
		}
	}' | sort -n -k2 | $tcmd
done

### end tracing
echo 2>/dev/null
echo "Ending tracing..." 2>/dev/null
warn "echo 0 > function_profile_enabled"
warn "echo > set_ftrace_filter"
