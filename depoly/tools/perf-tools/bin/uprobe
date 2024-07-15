#!/bin/bash
#
# uprobe - trace a given uprobe definition. User-level dynamic tracing.
#          Written using Linux ftrace. Experimental.
#
# This will create, trace, then destroy a given uprobe definition. See
# Documentation/trace/uprobetrace.txt in the Linux kernel source for the
# syntax of a uprobe definition, and "uprobe -h" for examples. With this tool,
# the probe alias is optional (it will default to something meaningful).
#
# USAGE: ./uprobe [-FhHsv] [-d secs] [-p pid] [-L tid] {-l target |
#                 uprobe_definition [filter]}
#
# Run "uprobe -h" for full usage.
#
# WARNING: This uses dynamic tracing of user-level functions, using some
# relatively new kernel code. I have seen this cause target processes to fail,
# either entering endless spin loops or crashing on illegal instructions. I
# believe newer kernels (post 4.0) are relatively safer, but use caution. Test
# in a lab environment, and know what you are doing, before use.
#
# Use extreme caution with the raw address mode: eg, "p:libc:0xbf130". uprobe
# does not check for instruction alignment, so tracing the wrong address (eg,
# mid-way through a multi-byte instruction) will corrupt the target's memory.
# Other tracers (eg, perf_events with debuginfo) check alignment.
#
# Also beware of widespread tracing that interferes with the operation of the
# system, eg, tracing libc:malloc, which by-default will trace _all_ processes.
# Test in a lab environment before use.
#
# I wrote this because I kept testing different custom uprobes at the command
# line, and wanted a way to automate the steps. For generic user-level
# tracing, use perf_events directly.
#
# REQUIREMENTS: FTRACE and UPROBE CONFIG, which you may already have on recent
# kernel versions, file(1), ldconfig(8), objdump(1), and some version of awk.
# Also, currently only executes on Linux 4.0+ (see WARNING) unless -F is used.
#
# From perf-tools: https://github.com/brendangregg/perf-tools
#
# See the uprobe(8) man page (in perf-tools) for more info.
#
# COPYRIGHT: Copyright (c) 2015 Brendan Gregg.
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
# 27-Jul-2015	Brendan Gregg	Created this.

### default variables
tracing=/sys/kernel/debug/tracing
flock=/var/tmp/.ftrace-lock; wroteflock=0
opt_duration=0; duration=; opt_pid=0; pid=; opt_tid=0; tid=
opt_filter=0; filter=; opt_view=0; opt_headers=0; opt_stack=0; dmesg=2
debug=0; opt_force=0; opt_list=0; target=
PATH=$PATH:/usr/bin:/sbin	# ensure we find objdump, ldconfig
trap ':' INT QUIT TERM PIPE HUP	# sends execution to end tracing section

function usage {
	cat <<-END >&2
	USAGE: uprobe [-FhHsv] [-d secs] [-p PID] [-L TID] {-l target |
	              uprobe_definition [filter]}
	                 -F              # force. trace despite warnings.
	                 -d seconds      # trace duration, and use buffers
	                 -l target       # list functions from this executable
	                 -p PID          # PID to match on events
	                 -L TID          # thread id to match on events
	                 -v              # view format file (don't trace)
	                 -H              # include column headers
	                 -s              # show user stack traces
	                 -h              # this usage message
	
	Note that these examples may need modification to match your kernel
	version's function names and platform's register usage.
	   eg,
	       # trace readline() calls in all running "bash" executables:
	           uprobe p:bash:readline
	       # trace readline() with explicit executable path:
	           uprobe p:/bin/bash:readline
	       # trace the return of readline() with return value as a string:
	           uprobe 'r:bash:readline +0(\$retval):string'
	       # trace sleep() calls in all running libc shared libraries:
	           uprobe p:libc:sleep
	       # trace sleep() with register %di (x86):
	           uprobe 'p:libc:sleep %di'
	       # trace this address (use caution: must be instruction aligned):
	           uprobe p:libc:0xbf130
	       # trace gettimeofday() for PID 1182 only:
	           uprobe -p 1182 p:libc:gettimeofday
	       # trace the return of fopen() only when it returns NULL:
	           uprobe 'r:libc:fopen file=\$retval' 'file == 0'

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
	warn "echo 0 > events/uprobes/$uname/enable"
	if (( opt_filter )); then
		warn "echo 0 > events/uprobes/$uname/filter"
	fi
	warn "echo -:$uname >> uprobe_events"
	(( opt_stack )) && warn "echo 0 > options/userstacktrace"
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

function set_path {
	name=$1

	path=$(which $name)
	if [[ "$path" == "" ]]; then
		path=$(ldconfig -v 2>/dev/null | awk -v lib=$name '
		    $1 ~ /:/ { sub(/:/, "", $1); path = $1 }
		    { sub(/\..*/, "", $1); }
		    $1 == lib { print path "/" $3 }')
		if [[ "$path" == "" ]]; then
			die "ERROR: segment \"$name\" ambiguous." \
			    "Program or library? Try a full path."
		fi
	fi

	if [[ ! -x $path ]]; then
		die "ERROR: resolved \"$name\" to \"$path\", but file missing"
	fi
}

function set_addr {
	path=$1
	name=$2
	sym=$3

	[[ "$path" == "" ]] && die "ERROR: missing symbol path."
	[[ "$sym" == "" ]] && die "ERROR: missing symbol for $path"

	addr=$(objdump -tT $path | awk -v sym=$sym '
	    $NF == sym && $4 == ".text"  { print $1; exit }')
	[[ "$addr" == "" ]] && die "ERROR: missing symbol \"$sym\" in $path"
	(( 0x$addr == 0 )) && die "ERROR: failed resolving \"$sym\" in $path." \
	    "Maybe it exists in a different target (eg, library)?"
	addr=0x$( printf "%x" 0x$addr )		# strip leading zeros

	type=$(file $path)
	if [[ "$type" != *shared?object* ]]; then
		# subtract the base mapping address. see Documentation/trace/
		# uprobetracer.txt for background.
		base=$(objdump -x $path | awk '
		    $1 == "LOAD" && $3 ~ /^[0x]*$/ { print $5 }')
		[[ "$base" != 0x* ]] && die "ERROR: finding base load addr"\
		    "for $path."
		addr=$(( addr - base ))
		(( addr < 0 )) && die "ERROR: transposed address for $sym"\
		    "became negative: $addr"
		addr=0x$( printf "%x" $addr)
	fi
}

### process options
while getopts Fd:hHl:p:L:sv opt
do
	case $opt in
	F)	opt_force=1 ;;
	d)	opt_duration=1; duration=$OPTARG ;;
	p)	opt_pid=1; pid=$OPTARG ;;
	L)	opt_tid=1; tid=$OPTARG ;;
	l)	opt_list=1; target=$OPTARG ;;
	H)	opt_headers=1 ;;
	s)	opt_stack=1 ;;
	v)	opt_view=1 ;;
	h|?)	usage ;;
	esac
done
shift $(( $OPTIND - 1 ))
uprobe=$1
shift
if (( $# )); then
	opt_filter=1
	filter=$1
fi

### handle listing
[[ "$opt_list" == 1 && "$uprobe" != "" ]] && die "ERROR: -l takes a target only"
if (( opt_list )); then
	if [[ "$target" != */* ]]; then
		set_path $target
		target=$path
	fi
	objdump -tT $target | awk '$4 == ".text" { print $NF }' | sort | uniq
	exit
fi

### check kernel version
ver=$(uname -r)
maj=${ver%%.*}
if (( opt_force == 0 && $maj < 4 )); then
	cat <<-END >&2
	ERROR: Kernel version >= 4.0 preferred (you have $ver). Aborting.
	
	Background: uprobes were first added in 3.5. I've tested them on 3.13,
	and found them unsafe, as they can crash or lock up processes, which can
	effectively lock up the system. On 4.0, uprobes seem much safer. You
	can use -F to force tracing, but you've been warned.
END
	exit
fi

### check command dependencies
for cmd in file objdump ldconfig awk; do
	which $cmd > /dev/null
	(( $? != 0 )) && die "ERROR: missing $cmd in \$PATH. $0 needs" \
	    "to use this command. Exiting."
done

### option logic
[[ "$uprobe" == "" ]] && usage
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
if [[ "$uprobe" != p:* && "$uprobe" != r:* ]]; then
	echo >&2 "ERROR: invalid uprobe definition (should start with p: or r:)"
	usage
fi
#
# Parse the following:
# p:bash:readline
# p:my bash:readline
# p:bash:readline %si
# r:bash:readline $ret
# p:my bash:readline %si
# p:bash:readline si=%si
# p:my bash:readline si=%si
# r:bash:readline cmd=+0($retval):string
# ... and all of the above with /bin/bash instead of bash
# ... and all of the above with libc:sleep instead of ...
# ... and all of the above with /lib/x86_64-linux-gnu/libc.so.6:sleep ...
# ... and all of the above with symbol addresses
# ... and examples from USAGE message
# The following code is not as complicated as it looks.
#
utype=${uprobe%%:*}
urest="${uprobe#*:} "
set -- $urest
if [[ $1 == *:* ]]; then
	uname=; probe=$1; shift; uargs="$@"
else
	[[ $2 != *:* ]] && die "ERROR: invalid probe. See usage (-h)."
	uname=$1; probe=$2; shift 2; uargs="$@"
fi
path=$probe; path=${path%%:*}
addr=$probe; addr=${addr##*:}

# set seg and fix path (eg, seg=bash, path=/bin/bash)
if [[ $path == */* ]]; then
	seg=${path##*/}
	seg=${seg%%.*}
else
	seg=$path
	# determine path, eg, given "zsh" or "libc"
	set_path $path
fi

# fix uname and addr (eg, uname=readline, addr=0x8db60)
if [[ "$addr" == 0x* ]]; then
	# symbol unknown; default to seg+addr
	[[ "$uname" == "" ]] && uname=${seg}_$addr
else
	[[ "$uname" == "" ]] && uname=$addr
	set_addr $path $seg $addr
fi

# construct uprobe
uprobe="$utype:$uname $path:$addr"
[[ "$uargs" != "" ]] && uprobe="$uprobe $uargs"

if (( debug )); then
	echo "uname: \"$uname\", uprobe: \"$uprobe\""
fi

### check permissions
cd $tracing || die "ERROR: accessing tracing. Root user? Kernel has FTRACE?
    debugfs mounted? (mount -t debugfs debugfs /sys/kernel/debug)"

if (( !opt_view )); then
	if (( opt_duration )); then
		echo "Tracing uprobe $uname for $duration seconds (buffered)..."
	else
		echo "Tracing uprobe $uname ($uprobe). Ctrl-C to end."
	fi
fi

### ftrace lock
[[ -e $flock ]] && die "ERROR: ftrace may be in use by PID $(cat $flock) $flock"
echo $$ > $flock || die "ERROR: unable to write $flock."
wroteflock=1

### setup and begin tracing
echo nop > current_tracer
if ! echo "$uprobe" >> uprobe_events; then
	echo >&2 "ERROR: adding uprobe \"$uprobe\"."
	if (( dmesg )); then
		echo >&2 "Last $dmesg dmesg entries (might contain reason):"
		dmesg | tail -$dmesg | sed 's/^/    /'
	fi
	edie "Exiting."
fi
if (( opt_view )); then
	cat events/uprobes/$uname/format
	edie ""
fi
if (( opt_filter )); then
	if ! echo "$filter" > events/uprobes/$uname/filter; then
		edie "ERROR: setting filter or -p. Exiting."
	fi
fi
if (( opt_stack )); then
	if ! echo 1 > options/userstacktrace; then
		edie "ERROR: enabling stack traces (-s). Exiting"
	fi
fi
if ! echo 1 > events/uprobes/$uname/enable; then
	edie "ERROR: enabling uprobe $uname. Exiting."
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
