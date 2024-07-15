#!/bin/bash
#
# reset-ftrace - reset state of ftrace, disabling all tracing.
#                Written for Linux ftrace.
#
# This may only be of use to ftrace hackers who, in the process of developing
# ftrace software, often get the subsystem into a partially active state, and
# would like a quick way to reset state. Check the end of this script for the
# actually files reset, and add more if you need.
#
# USAGE: ./reset-ftrace [-fhq]
#
# REQUIREMENTS: FTRACE CONFIG.
#
# From perf-tools: https://github.com/brendangregg/perf-tools
#
# See the reset-ftrace(8) man page (in perf-tools) for more info.
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
# 20-Jul-2014	Brendan Gregg	Created this.

tracing=/sys/kernel/debug/tracing
flock=/var/tmp/.ftrace-lock
opt_force=0; opt_quiet=0

function usage {
	cat <<-END >&2
	USAGE: reset-ftrace [-fhq]
	                 -f              # force: delete ftrace lock file
	                 -q              # quiet: reset, but say nothing
	                 -h              # this usage message
	  eg,
	       reset-ftrace              # disable active ftrace session
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

function vecho {
	(( opt_quiet )) && return
	echo "$@"
}

# write to file
function writefile {
	file=$1
	string=$2	# optional
	if [[ ! -w $file ]]; then
		echo >&2 "WARNING: file $file not writable/exists. Skipping."
		return
	fi
	vecho "$file, before:"
	(( ! opt_quiet )) && cat -n $file
	warn "echo $string > $file"
	vecho "$file, after:"
	(( ! opt_quiet )) && cat -n $file
	vecho
}

### process options
while getopts fhq opt
do
	case $opt in
	f)	opt_force=1 ;;
	q)	opt_quiet=1 ;;
	h|?)	usage ;;
	esac
done
shift $(( $OPTIND - 1 ))

### ftrace lock
if [[ -e $flock ]]; then
	if (( opt_force )); then
		warn rm $flock
	else
		echo -e >&2 "ERROR: ftrace lock ($flock) exists. It shows" \
		    "ftrace may be in use by PID $(cat $flock).\nDouble check" \
		    "to see if that PID is still active. If not, consider" \
		    "using -f to force a reset. Exiting."
		exit 1
	fi
fi

### reset ftrace state
vecho "Reseting ftrace state..."
vecho
cd $tracing || die "ERROR: accessing tracing. Root user? Kernel has FTRACE?"
writefile current_tracer nop
writefile set_ftrace_filter
writefile set_graph_function
writefile set_ftrace_pid
writefile events/enable 0
writefile tracing_thresh 0
writefile kprobe_events
writefile tracing_on 1
vecho "Done."
