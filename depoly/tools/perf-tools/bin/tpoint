#!/bin/bash
#
# tpoint - trace a given tracepoint. Static tracing.
#          Written using Linux ftrace.
#
# This will enable a given tracepoint, print events, then disable the tracepoint
# when the program ends. This is like a simple version of the "perf" command for
# printing live tracepoint events only. Wildcards are currently not supported.
# If this is insufficient for any reason, use the perf command instead.
#
# USAGE: ./tpoint [-hHsv] [-d secs] [-p pid] [-L tid] tracepoint [filter]
#        ./tpoint -l
#
# Run "tpoint -h" for full usage.
#
# I wrote this because I often needed a quick way to dump stack traces for a
# given tracepoint.
#
# OVERHEADS: Relative to the frequency of traced events. You might want to
# check their frequency beforehand, using perf_events. Eg:
#
#	perf stat -e block:block_rq_issue -a sleep 5
#
# To count occurrences of that tracepoint for 5 seconds.
#
# REQUIREMENTS: FTRACE and tracepoints, which you may already have on recent
# kernel versions.
#
# From perf-tools: https://github.com/brendangregg/perf-tools
#
# See the tpoint(8) man page (in perf-tools) for more info.
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
# 22-Jul-2014	Brendan Gregg	Created this.

### default variables
tracing=/sys/kernel/debug/tracing
flock=/var/tmp/.ftrace-lock; wroteflock=0
opt_duration=0; duration=; opt_pid=0; pid=; opt_tid=0; tid=
opt_filter=0; filter=; opt_view=0; opt_headers=0; opt_stack=0; dmesg=2
trap ':' INT QUIT TERM PIPE HUP	# sends execution to end tracing section

function usage {
	cat <<-END >&2
	USAGE: tpoint [-hHsv] [-d secs] [-p PID] [-L TID] tracepoint [filter]
	       tpoint -l
	                 -d seconds      # trace duration, and use buffers
	                 -p PID          # PID to match on event
	                 -L TID          # thread id to match on event
	                 -v              # view format file (don't trace)
	                 -H              # include column headers
	                 -l              # list all tracepoints
	                 -s              # show kernel stack traces
	                 -h              # this usage message
	   eg,
	       tpoint -l | grep open
	                                 # find tracepoints containing "open"
	       tpoint syscalls:sys_enter_open
	                                 # trace open() syscall entry
	       tpoint block:block_rq_issue
	                                 # trace block I/O issue
	       tpoint -s block:block_rq_issue
	                                 # show kernel stacks

	See the man page and example file for more info.
END
	exit
}

function warn {
	if ! eval "$@"; then
		echo >&2 "WARNING: command failed \"$@\""
	fi
}

function end {
	# disable tracing
	echo 2>/dev/null
	echo "Ending tracing..." 2>/dev/null
	cd $tracing
	warn "echo 0 > $tdir/enable"
	if (( opt_filter )); then
		warn "echo 0 > $tdir/filter"
	fi
	(( opt_stack )) && warn "echo 0 > options/stacktrace"
	warn "echo > trace"
	(( wroteflock )) && warn "rm $flock"
}

function die {
	echo >&2 "$@"
	exit 1
}

function edie {
	# die with a quiet end()
	echo >&2 "$@"
	exec >/dev/null 2>&1
	end
	exit 1
}

### process options
while getopts d:hHlp:L:sv opt
do
	case $opt in
	d)	opt_duration=1; duration=$OPTARG ;;
	p)	opt_pid=1; pid=$OPTARG ;;
	L)	opt_tid=1; tid=$OPTARG ;;
	H)	opt_headers=1 ;;
	l)	opt_list=1 ;;
	s)	opt_stack=1 ;;
	v)	opt_view=1 ;;
	h|?)	usage ;;
	esac
done
if (( !opt_list )); then
	shift $(( $OPTIND - 1 ))
	(( $# )) || usage
	tpoint=$1
	shift
	if (( $# )); then
		opt_filter=1
		filter=$1
	fi
fi

### option logic
(( opt_pid + opt_filter + opt_tid > 1 )) && \
	die "ERROR: use at most one of -p, -L, or filter."
(( opt_duration && opt_view )) && die "ERROR: use either -d or -v."
if (( opt_pid )); then
	# convert to filter
	opt_filter=1
	# ftrace common_pid is thread id from user's perspective
	for tid in /proc/$pid/task/*; do
		filter="$filter || common_pid == ${tid##*/}"
	done
	filter=${filter:3}  # trim leading ' || ' (four characters)
fi
if (( opt_tid )); then
	opt_filter=1
	filter="common_pid == $tid"
fi
if (( !opt_view && !opt_list )); then
	if (( opt_duration )); then
		echo "Tracing $tpoint for $duration seconds (buffered)..."
	else
		echo "Tracing $tpoint. Ctrl-C to end."
	fi
fi

### check permissions
cd $tracing || die "ERROR: accessing tracing. Root user? Kernel has FTRACE?
    debugfs mounted? (mount -t debugfs debugfs /sys/kernel/debug)"

### do list tracepoints
if (( opt_list )); then
	cd events
	for tp in */*; do
		# skip filter/enable files
		[[ -f $tp ]] && continue
		echo ${tp/\//:}
	done
	exit
fi

### check tracepoints
tdir=events/${tpoint/:/\/}
[[ -e $tdir ]] || die "ERROR: tracepoint $tpoint not found. Exiting"

### view
if (( opt_view )); then
	cat $tdir/format
	exit
fi

### ftrace lock
[[ -e $flock ]] && die "ERROR: ftrace may be in use by PID $(cat $flock) $flock"
echo $$ > $flock || die "ERROR: unable to write $flock."
wroteflock=1

### setup and begin tracing
echo nop > current_tracer
if (( opt_filter )); then
	if ! echo "$filter" > $tdir/filter; then
		edie "ERROR: setting filter or -p. Exiting."
	fi
fi
if (( opt_stack )); then
	if ! echo 1 > options/stacktrace; then
		edie "ERROR: enabling stack traces (-s). Exiting"
	fi
fi
if ! echo 1 >> $tdir/enable; then
	edie "ERROR: enabling tracepoint $tprobe. Exiting."
fi

### print trace buffer
warn "echo > trace"
if (( opt_duration )); then
	sleep $duration
	if (( opt_headers )); then
		cat trace
	else
		grep -v '^#' trace
	fi
else
	# trace_pipe lack headers, so fetch them from trace
	(( opt_headers )) && cat trace
	cat trace_pipe
fi

### end tracing
end
