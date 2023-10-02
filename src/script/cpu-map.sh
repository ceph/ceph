#!/bin/bash
# !
# ! Usage: ./cpu-map.sh -p <process_PIDs comma separated> -n <process_Name>
# !                     -g <group-name:affinity-range>
# !
# ! Set the CPU affinity for a list of running processes
# !
# ! Ex.: ./cpu-map.sh -n crimson -g "alien:4-31"
# !      ./cpu-map.sh -p ($pgrep osd) -g "rocksdb:4-7"
# !
# !  Important: the "affinity-range" must be a vlid argument for taskset.
########################################################
# Please edit the following to suit your own needs
#
# Regex to define the group of threads names we want to set their affinity:
proc_group_re="alien-store-tp|rocksdb|bstore|cfin"
# Associate array to identify the group and the thread names
declare -A thr_grp_re_name_map=([alien]="$proc_group_re")
# Ditto for their default affinity range:
declare -A thr_grp_range_map=([alien]="4-7")
# You can define further groups as needed

# Range of CPU cores available: i.e. nprocs - rectors
free_cpu_avail="8-31"

# Regex of threads to ignore: do not change their affinity
proc_ignore_re="crimson-osd|reactor|log|syscall"
########################################################
# Examples:

# Run with 4 cores on a dual socket 8 CPucores per socket system, no hyperthreading
#declare -A thr_grp_map=([alien-store-tp]="4-7" [rocksdb]="4-7" [bstore]="4-7" [cfin]="4-7" )

########################################################
# svcdev3 -- 56 cpus
#declare -A thr_grp_map=([io_context_pool]="0-12" [msg-worker]="13-25" [ceph-osd]="26-38" )
#free_cpu_avail="39-55"
#proc_group_re="io_context_pool|msg-worker|ceph-osd"

########################################################
# sv1-cephX-- 32 cpus
#declare -A thr_grp_map=([io_context_pool]="0-9" [msg-worker]="10-19" [ceph-osd]="20-29" )

# Run with  2 cores:
# busiest threads is msgr-worker-1, the rest of the threads segregated to the other core -- this is per OSD process
#declare -A thr_grp_map=([msgr-worker]="0-0")
#free_cpu_avail="1-1"
# Regex to define the group of threads we want on its own cpu core
#proc_group_re="msgr-worker"

# Run with  4 cores:
#declare -A thr_grp_map=([msgr-worker]="0-0" [bstore_kv]="1-1" [tp_osd_tp]="2-2")
#free_cpu_avail="3-3"
#proc_group_re="msgr-worker|bstore_kv|tp_osd_tp"

# Run with  8 cores:
#declare -A thr_grp_map=([msgr-worker]="0-1" [bstore_kv]="2-3" [tp_osd_tp]="4-5")
#free_cpu_avail="6-7"
#proc_group_re="msgr-worker|bstore_kv|tp_osd_tp"

# Run with  16 cores:
#declare -A thr_grp_map=([msgr-worker]="0-1" [bstore_kv]="2-3" [tp_osd_tp]="4-5" [rocksdb]="6-7")
#free_cpu_avail="8-15"
#proc_group_re="msgr-worker|bstore_kv|tp_osd_tp|rocksdb"

########################################################

# cores 16-31 are for FIO only

usage() {
    cat $0 | grep ^"# !" | cut -d"!" -f2-
}

# Given a thread name, find its affinity from $thr_grp_map[]
getaffinity() {
    local name="$1"
    local regex="$2"
    if [ -z "$regex" ]; then
        echo ''
    fi
}

# process arguments
while getopts 'p:n:g:' option; do
  case "$option" in
    p) PROCESSES=$OPTARG # this should be a , separated list of pids
        ;;
    n) PROCESSES=$(pgrep --newest --exact $OPTARG)
        ;;
        # TBD. extend this argument into a map {thread_name:cpu-range}
    g) CPUGROUP_STR=$OPTARG 
       CPUGROUP=true
	IFS=':' read -r -a cpu_grp_lst <<< "$CPUGROUP_STR"
	#echo ${cpu_grp_lst[@]} #ok
	# cpu_grp_lst[0] is the group name,
	# cpu_grp_lst[1] is the group cpu range
        ;;
    :) printf "missing argument for -%s\n" "$OPTARG" >&2
       usage >&2
       exit 1
       ;;
    \?) printf "illegal option: -%s\n" "$OPTARG" >&2
       usage >&2
       exit 1
       ;;
  esac
done

if [ $# -eq 0 ]; then usage >&2; exit 1; fi

declare -a other=()
next_avail_cpu=0
max_cpus=$(nproc)
cpu_used=1
j=0 # last index of the other[] array

function get_affinity() {
	local i="$1"
}

IFS=', ' read -r -a proc_list <<< "$PROCESSES"
for PID in "${proc_list[@]}"; do
    if [ -e "/proc/$PID/task" ]; then
        # get list of threads for given PID
	THREADS=$(ls /proc/$PID/task)
	for i in $THREADS; do
		if [ "$CPUGROUP" = true ]; then
			pgroup_re=${thr_grp_re_name_map[${cpu_grp_lst[0]}]}
			t_name=$(grep -E "$pgroup_re" /proc/$i/comm )
			if [ -n "$t_name" ]; then
				affinity=${cpu_grp_lst[1]}
			else
				# this thread is not in the group, check if we want to skip it
				t_name=$(grep -E "$proc_ignore_re" /proc/$i/comm )

				if [ -n "$t_name" ] && [ "$CPUGROUP" = true ]; then
					echo "Skipping $i"
					continue
				else
					# so its affinity will be on the remaining cpu set
					other[$(( j++ ))]="$i"
				fi
			fi
		else
			# if we use them all CPUs, rotate
			affinity=$(( next_avail_cpu++ % max_cpus ))
			cpu_used=$(( cpu_used | ( 1 << $affinity )))
			printf "cpu_used: 0x%x\n" $cpu_used
		fi
		echo "$t_name: taskset -c -p $affinity $i"
		taskset -c -p $affinity $i
	done # threads
    else
        echo "Process $PID does not exist"
    fi
done # PID

# Assign the affinity to the other threads on the remaining non-used cpus
echo "Threads in the remaining set all with the remaining processor ids:"
#echo "${other[@]}"
for x in "${other[@]}"; do
    if [ "$CPUGROUP" ]; then
        echo taskset -c -p $free_cpu_avail $x
    else
        rem_affinity=$(printf "0x%x\n" $(( cpu_used ^ 0xfffffff )))
        echo taskset -p  $rem_affinity $x
    fi
done
