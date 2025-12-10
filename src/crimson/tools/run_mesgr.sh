#!/usr/bin/env bash

# ! Usage: ./run_mesgr.sh [-t <type>] [-d rundir]
# !		 
# ! Run progressive smp count to copmpare the Crimson messenger.
# ! -d : indicate the run directory cd to
# ! -t : messenger type: crimson/async: perf-crimson-msgr (default) or perf-async-msgr 

# Redirect to stdout/stderr to a log file
# exec 3>&1 4>&2
# trap 'exec 2>&4 1>&3' 0 1 2 3
# exec 1>/tmp/run_balanced_osd.log 2>&1

# Define the CPU core sets for server and client for each type of balanced vs separated
declare -A mesgr_cpu_table
declare -A mesgr_cpu_row
declare -A pids

# Max Balanced NUMA configuration:
mesgr_cpu_row['server']="0-13,56-69,28-41,84-97"
mesgr_cpu_row['client']="14-27,70-83,42-55,98-111"
string=$(declare -p mesgr_cpu_row)
mesgr_cpu_table['balanced']=${string}
# Max Separated NUMA configuration:
mesgr_cpu_row['server']="0-27"
mesgr_cpu_row['client']="28-55"
string=$(declare -p mesgr_cpu_row)
mesgr_cpu_table['separated']=${string}

#Single table for the CPU cores and type of balanced vs separated configurations
# JSON representation: # key is smp==num_clients, val is a cpu-set string
# balanced: a phys core and its HT sibling per NUMA node
# separated: only phys cores for each NUMA node, per server/client
json='{
  "balanced": {
    "server": {
      "2": "0,56",
      "4": "0,56,28,84",
      "8": "0-1,56-57,28-29,84-85",
      "14": "0-3,56-59,28-30,84-86",
      "16": "0-3,56-59,28-31,84-87",
      "20": "0-4,56-60,28-32,84-88",
      "24": "0-5,56-61,28-33,84-89",
      "28": "0-6,56-62,28-34,84-90",
      "32": "0-7,56-63,28-35,84-91",
      "40": "0-9,56-65,28-37,84-93",
      "56": "0-13,56-69,28-41,84-97"
    },
    "client": {
      "2": "1,57",
      "4": "1,57,29,85",
      "8": "2-3,58-59,30-31,86-87",
      "14": "4-7,60-63,32-34,88-90",
      "16": "4-7,60-63,32-35,88-91",
      "20": "5-9,61-65,33-37,90-94",
      "24": "6-11,62-67,34-39,90-95",
      "28": "7-13,63-69,35-41,91-97",
      "32": "8-15,64-71,36-43,92-99",
      "40": "10-19,66-75,38-47,94-103",
      "56": "14-27,70-83,42-55,98-111"
    }
  },
  "separated": {
    "server": {
      "2": "0-1",
      "4": "0-3",
      "8": "0-7",
      "14": "0-13",
      "16": "0-15",
      "18": "0-17",
      "20": "0-19",
      "24": "0-23",
      "28": "0-27"
    },
    "client": {
      "2": "28-29",
      "4": "28-31",
      "8": "28-35",
      "14": "28-41",
      "16": "28-43",
      "18": "28-45",
      "20": "28-47",
      "24": "28-51",
      "28": "28-55"
    }
  }
}'

# Need a better name, this will do for now
# Define a function to traverse this json and call fun_run_fixed_msgr_test with the json to use and the port for server
# and client, we could extract each of "left" and "right" and use them as arguments to the function

max_json='{
  "left": {
     "balanced": {
        "server": {
            "14": "0-3,56-59,28-30,84-86"
        },
        "client": {
            "14": "4-7,60-63,32-34,88-90"
        }
    },
    "separated": {
        "server": {
            "14": "0-13"
        },
        "client": {
            "14": "28-41"
        }
    }
  },
  "right": {
    "balanced": {
        "server": {
            "14": "8-11,64-67,35-37,91-93"
        },
    "client": {
        "14": "12-15,68-71,38-40,94-96"
    }
  },
  "separated": {
    "server": {
        "14": "14-27"
    },
    "client": {
        "14": "42-55"
    }
  }
 }
}'
# Original set using physical and HT siblings:
#SERVER_CPU_CORES="0-13,56-69,28-41,84-97"
#CLIENT_CPU_CORES="14-27,70-83,42-55,98-111"
# Second set of CPU cores with HT disabled
# SERVER_CPU_CORES="0-27"
# CLIENT_CPU_CORES="28-55"
# # The range of CPU cores to test -- preliminary version might not be accurate
# SMP_RANGE_CPU="2 4 8 14 28" # 42 56"
declare -A smp_range
# smp_range['balanced']="2 4 8 16 28 32 40 56"
# smp_range['separated']="2 4 8 16 28"
# Small range to focus over the peak/drop performance
# smp_range['balanced']="16 20 24 28"
# smp_range['separated']="16 18 20 24 28"

# Only for this run:
smp_range['balanced']="28"
smp_range['separated']="28"
# The port to use: if we use an int to indicate the number of process, we might
# simply add such number to the base port If we decide to add a -n parameter to
# indicate the number of processes, we need to calculate the CPU cores to use:
# divide n by the number of physical CPU cores, similar to what we do for
# balance OSD. For example, given 56 physical cores, we can use 2x14 cores per
# server, 2x14 cores per client (that is, 56/4 = 56/n/2 or 56/(2*n))
declare -A max_server_port 
max_server_port['left']="v2:127.0.0.1:9010" # default
max_server_port['right']="v2:127.0.0.1:9011"

declare -A max_smp_range
max_smp_range['left']="14"
max_smp_range['right']="14"

MESG_TYPE=crimson
RUN_DIR="/tmp"
BAL_TYPE=balanced

NUM_CPU_SOCKETS=2 # Hardcoded since NUMA has two sockets
# The following values consider already the CPU cores reserved for FIO -- no longer used since we have the SERVER_CPU_CORES and use taskset
MAX_NUM_PHYS_CPUS_PER_SOCKET=28
MAX_NUM_HT_CPUS_PER_SOCKET=56
NUMA_NODES_OUT=/tmp/numa_nodes.json

# Globals:
LATENCY_TARGET=false 
NUM_SAMPLES=10
DELAY_SAMPLES=6 # in secs
POST_PROC=""
FLAME=false
# Single option to set a max of 2x14 cores per server, 2x14 cores per client, this needs to define two sockets
MAX=false

RUNTIME=60 # seconds
source /root/bin/common.sh 
source /root/bin/monitoring.sh

#############################################################################################
# Original lscpu: o05
# NUMA:
#  NUMA node(s):           2
#  NUMA node0 CPU(s):      0-27,56-83
#  NUMA node1 CPU(s):      28-55,84-111
#############################################################################################
# Obtain the CPU id mapping per thread
# Returns a _list of _threads.out files
fun_set_mesgr_pids() {
    local PIDS=$1
    local TEST_PREFIX=$2

    echo -e "${GREEN}== pid:${PIDS} Constructing list of threads and affinity for msgr ${TEST_PREFIX} ==${NC}"
    local OUTNAME="${TEST_PREFIX}_threads.out"
    [ -f "${OUTNAME}" ] && rm -f ${OUTNAME}
    # Count number, name and affinity of the threads
    taskset -acp ${PIDS} >> ${TEST_PREFIX}_tasks.out
    ps -p ${PIDS} -L -o pid,tid,comm,psr --no-headers > ${TEST_PREFIX}_thrs.out
    # Separate csl of pids
    # IFS=', ' read -r -a array <<< "$PIDS"
    # for pid in "${array[@]}"; do
    #     taskset -acp $pid >> _tasks.out
    # done
    paste ${TEST_PREFIX}_thrs.out ${TEST_PREFIX}_tasks.out >> "${OUTNAME}"
    #cat ${OUTNAME}
    rm -f  ${TEST_PREFIX}_thrs.out ${TEST_PREFIX}_tasks.out
    echo ${OUTNAME} >> "${TEST_PREFIX}_threads_list"
    # construct a json file with the thread list per pid 
}

#############################################################################################
  # From the _threads.out files: parse them into .json (might be as part of the prev step?)
  # produce a dict which keys are the cpu uid (numeric), values is a list of threads-types
  # take longest string to define the cell width, and produce an ascii grid
  # For messenguer, since its the same process name and threads, we need to use the PID to identify client and server, for example:
  # {
  #   "server": { "pids": [368474], "color": "orange" },
  #   "client": { "pids": [368475,368476], "color": "yellow" },
  # }
  # This sub should also be moved to the common.sh
fun_validate_set() {
  local TEST_NAME=$1
  # On multiple instances, this could be a race hazard
  [ ! -f "${NUMA_NODES_OUT}" ] && lscpu --json > ${NUMA_NODES_OUT}
  # Needs extending to support multiple msgrs type client vs server, and lscpu -e --json layout as well
  # Also needs to produce a .json file with the thread list per pid
  python3 /root/bin/tasksetcpu.py -c $TEST_NAME -u ${NUMA_NODES_OUT} -d ${RUN_DIR}
}


#############################################################################################
_old_measure() {
  local TEST_OUT=$1
  local TEST_TOP_OUT_LIST=$2
  local PID=$3 #comma sep list of pids

  #IFS=',' read -r -a pid_array <<< "$1"
  # CPU core util (global) and CPU thread util for the pid given
  top -b -H -1 -p "${PID}" -n ${NUM_SAMPLES} >> ${TEST_OUT} &
  echo "${TEST_OUT}" >> ${TEST_TOP_OUT_LIST}
}

#############################################################################################
#Scans and prints the grids, useful for manual tests
fun_show_grid() {
    local test_name=$1
    local pids=$2

    fun_set_mesgr_pids ${pids} ${test_name}
    #fun_validate_set ${test_name}_threads_list
}
#############################################################################################
# Monitor a single process server/client    
    # if [ "${type}" == "client" ]; then 
    #     sleep 5 # ramp up time
    # fi
    # local max_retry=5
    # local counter=0
#     pids_list=$(pgrep perf-crimson-msgr)
    # until [ ! -z "$pids_list" ];
    # do
    #     sleep 1
    #     [[ counter -eq $max_retry ]] && echo "Failed!" #&& exit 1
    #     echo "Trying again. Try #$counter"
    #     ((counter++))
    #     pids_list=$(pgrep perf-crimson-msgr)
    # done
    # #msgr_pids=$( fun_join_by ',' "$pids_list" )
    #echo -e "${GREEN}== ${msgr_pids} ==${NC}"

fun_monitor() {
    local test_name=$1
    local pid=$2
    local TOP_OUT_LIST="${test_name}_top_list"
    local test_top_out="${test_name}_top.out"

    mon_perf  ${pid} ${test_name} ${FLAME}
    ( mon_measure ${pid} ${test_top_out} ${TOP_OUT_LIST} ) &
    #mon_measure ${test_top_out} ${TOP_OUT_LIST} ${pid}
    fun_show_grid ${test_name} ${pid}
}

#############################################################################################
# Set some global (urgs) variables for each test
fun_set_globals() {
    local test_name=$1

   # TODO: refactor these as a single associative array
    test_log="${test_name}.log"
    test_out="${test_name}.out"
    test_top_out="${test_name}_top.out"
    test_top_json="${test_name}_top.json"
    TOP_OUT_LIST="${test_name}_top_list"
    TOP_PID_JSON="${test_name}_pid.json"
    CPU_PID_JSON="${test_name}_cpu_pid.json"
    CPU_AVG="${test_name}_cpu_avg.json"
    test_zip="${test_name}.zip"
}

#############################################################################################
# Monitor the execution of the whole set (that can be more than a pair server/client)
fun_monitor_pair() {
    local MESG_TYPE=$1
    local SMP=$2
    local BAL_TYPE=$3
    local list_clients=""
    local list_servers=""
    
    for key in "${!pids[@]}"; do
        test_name="${key}_msgr_${MESG_TYPE}_${SMP}smp_${num_clients}clients_${BAL_TYPE}"
        #fun_set_globals ${test_name}
        # launch perf concurrently
        fun_monitor ${test_name} ${pids[${key}]}
        if [[ $key == *"client"* ]]; then
            list_clients="${list_clients} ${pids[${key}]}"
        else
            list_servers="${list_servers} ${pids[${key}]}"
        fi
    done
    echo -e "${GREEN}== Waiting clients ${list_clients} ==${NC}"
    wait ${list_clients}
    # Kill all the server msgr process only
    for key in "${!pids[@]}"; do
        if [[ $key == *"server"* ]]; then
            echo -e "${GREEN}== Killing server ${pids[${key}]} ==${NC}"
            if [ ${MESG_TYPE} == "async" ]; then
                pkill perf-async-msgr
            fi
            kill -SIGINT ${pids[${key}]}
            if [ $? -ne 0 ]; then
                echo -e "${RED}== Failed to kill server ${pids[${key}]} ==${NC}"
                kill -9 ${pids[${key}]}
            fi
        fi
    done

    test_name="msgr_${MESG_TYPE}_${SMP}smp_${num_clients}clients_${BAL_TYPE}"
    fun_set_globals ${test_name}
    # Produce charts from top output, for the whole set
    msgr_pids=$( fun_join_by ',' "${pids[@]}" )
    printf '{ "MSGR": [%s] }\n' "$msgr_pids" > ${test_name}_all.pid.json #${TOP_PID_JSON}
    # Produce a .json with the list of pids to monitor 
    svr_pids=$( fun_join_by ',' "${list_servers}" )
    cli_pids=$( fun_join_by ',' "${list_clients}" )
    # json=$(printf '{ "server": [%s], "client": [%s] }\n' "$svr_pids" "$cli_pids")
    # echo "${json}" > ${TOP_PID_JSON}
        # "label": "msgr_${MESG_TYPE}_${SMP}smp_${num_clients}clients_${BAL_TYPE}",
        # "server_pids": [${svr_pids}],
        # "client_pids": [${cli_pids}]
    read -r -d '' json <<EOF || true
    {
        "MSG_SERVER": {
            "cores": "${mesgr_cpu_row[server]}",
            "pids": [${svr_pids}]
        },
        "MSG_CLIENT": {
            "cores": "${mesgr_cpu_row[client]}",
            "pids": [${cli_pids}]
        }
    }
EOF
    #echo "$json" | jq . >> ${CPU_PID_JSON}
    echo "$json" >> ${CPU_PID_JSON}
    # We could use the existing -c option of top_parser.py to specify the cpu cores to monitor via this new json file

    mon_filter_top_cpu "${test_name}_top.out" ${CPU_AVG} ${CPU_PID_JSON} #${TOP_PID_JSON}
    run_gnuplot
    #for p in server client; do
    # for key in "${!pids[@]}"; do
    #     test_name="${key}_msgr_${MESG_TYPE}_${SMP}smp_${num_clients}clients_${BAL_TYPE}"
    #     if [[ $key == *"server"* ]]; then
    #         cpu_cores=${mesgr_cpu_row["server"]}
    #     else
    #         cpu_cores=${mesgr_cpu_row["client"]}
    #     fi
    #     mon_filter_top "${test_name}_top.out" ${CPU_AVG} ${TOP_PID_JSON}
    #     run_gnuplot
    #     #fun_pp_top "${test_name}_top.out" "${cpu_cores}" ${CPU_AVG} ${TOP_PID_JSON}
    # done
    # fun_pp_flamegraphs
    # Ugly, I know, refactoring needed
    test_name="msgr_${MESG_TYPE}_${SMP}smp_${num_clients}clients_${BAL_TYPE}"
    fun_set_globals ${test_name}
    fun_pp_archive ${test_zip}

}

#############################################################################################
# Run a single type of messenger test -- version with single execution client/server
fun_run_fixed_msgr_test() {
    local MESG_TYPE=$1
    local SMP=$2
    local BAL_TYPE=$3
    local key=$4

    local num_clients=$((SMP)) # 1:1 client-server ratio, was smp -1 before
    local server_port=${max_server_port["left"]}

    declare -A mesgr_ops_table
    declare -A test_row
    # eval "${mesgr_cpu_table["$BAL_TYPE"]}"
    # local SERVER_CPU_CORES=${mesgr_cpu_row["server"]}
    # local CLIENT_CPU_CORES=${mesgr_cpu_row["client"]}
    if [ "${MAX}" == "true" ]; then
        bash_server=".${key}.${BAL_TYPE}.server.\"${SMP}\""
        bash_client=".${key}.${BAL_TYPE}.client.\"${SMP}\""
        SERVER_CPU_CORES=$( echo ${max_json} | jq -r $bash_server )
        CLIENT_CPU_CORES=$( echo ${max_json} | jq -r $bash_client )
        server_port=${max_server_port[${key}]}
    else
        bash_server=".${BAL_TYPE}.server.\"${SMP}\""
        bash_client=".${BAL_TYPE}.client.\"${SMP}\""
        SERVER_CPU_CORES=$( echo ${json} | jq -r $bash_server )
        CLIENT_CPU_CORES=$( echo ${json} | jq -r $bash_client )
    fi

    # echo -e "${RED}== SERVER_CPU_CORES: ${SERVER_CPU_CORES} ==${NC}"
    # echo -e "${RED}== CLIENT_CPU_CORES: ${CLIENT_CPU_CORES} ==${NC}"

    # Crimson messenger: seastar cpuset overrides --smp=${SMP}
    test_row["server"]="/ceph/build/bin/perf-crimson-msgr --poll-mode --mode=2 --server-fixed-cpu=0  --cpuset=${SERVER_CPU_CORES} --server-addr ${server_port}"
    test_row['client']="/ceph/build/bin/perf-crimson-msgr --poll-mode --mode=1 --depth=512 --clients=${num_clients} --conns-per-client=2 --cpuset=${CLIENT_CPU_CORES} --msgtime=60 --client-skip-core-0=0 --server-addr=${server_port}"
    string=$(declare -p test_row)
    mesgr_ops_table["crimson"]=${string}
    
    # Async messenger: only the server is async, client is crimson
    test_row["server"]="taskset -ac ${SERVER_CPU_CORES} /ceph/build/bin/perf-async-msgr --threads=${SMP} --addr ${server_port}"
    test_row['client']="/ceph/build/bin/perf-crimson-msgr --poll-mode --mode=1 --depth=512 --clients=${num_clients} --conns-per-client=2 --cpuset=${CLIENT_CPU_CORES} --msgtime=60 --client-skip-core-0=0 --server-addr=${server_port}"
    string=$(declare -p test_row)
    mesgr_ops_table["async"]=${string}

    test_name="${key}msgr_${MESG_TYPE}_${SMP}smp_${num_clients}clients_${BAL_TYPE}"
    #fun_set_globals ${test_name}
    echo -e "${GREEN}==test_name: ${test_name}==${NC}"
    # Launch them all, then monitor them
    for p in server client; do
        #echo -e "${RED}==${p}: ${test_row[$p]}==${NC}"
        # Launch process:
        eval "${mesgr_ops_table["$MESG_TYPE"]}"
        cmd="${test_row["${p}"]}"
        echo "${cmd}"  | tee >> ${test_name}_${p}.log
        echo -e "${GREEN}==${p}: ${cmd}==${NC}"

        $cmd 2>&1 > ${test_name}_${p}.out &
        pids[${key}_${p}]=$!
    done
}
#############################################################################################
# Post-process flamegraphs
fun_pp_flamegraphs() {
    # local TEST_OUT=$1
    # local TEST_TOP_OUT_LIST=$2
    # local PID=$3 #comma sep list of pids
    #
    # perf script -i ${TEST_OUT}.perf.out | stackcollapse-perf.pl | sed 's/0x[0-9a-f]*//g' | awk '{print $1,$2,$3,$4,$5,$6}' | sort | uniq -c | awk '{print $2,$3,$4,$5,$6,$7,$8}' > ${TEST_OUT}.flamegraph.out
    # flamegraph.pl ${TEST_OUT}.flamegraph.out > ${TEST_OUT}.flamegraph.svg
    for x in $(ls *perf.out); do
      #y=${x/perf.out/scripted.gz}
      z=${x/perf.out/fg.svg}
      echo "==$(date) == Perf script $x: $y =="
      perf script -i $x | c++filt | stackcollapse-perf.pl > ${x}_fold # orig multi-reactor
      flamegraph.pl ${x}_fold > $z
      sed -e 's/perf-crimson-ms/reactor/g' -e 's/reactor-[0-9]\+/reactor/g'  -e 's/msgr-worker-[0-9]\+/msgr-worker/g'  ${x}_fold > ${x}_merged
      python3 /root/bin/pp_crimson_flamegraphs.py -i ${x}_merged | flamegraph.pl --title "${x}" > ${x}_coalesced.svg
      gzip -9 $x ${x}_fold
      # Need to merge the reactors into a single one to compare
      # diff:
      # difffolded.pl ${x}_fold ${y}_fold | flamegraph.pl  > ${x}_${y}_diff.svg
    done
}

#############################################################################################
# Post-process data into charts
_old_pp_top() {
    local TEST_OUT=$1
    local CORES=$2
    local CPU_AVG=$3
    local TOP_PID_JSON=$4
    local test_top_json="${TEST_OUT}_top.json"

    cat ${TEST_OUT} | jc --top --pretty > ${test_top_json}
    python3 /root/bin/parse-top.py -d ${RUN_DIR} --config=${test_top_json} --cpu="${CORES}" --avg=${CPU_AVG} \
        --pids=${TOP_PID_JSON} 2>&1 > /dev/null
}

#########################################
run_gnuplot() {
    for x in *.plot; do
        gnuplot $x 2>&1 > /dev/null
    done
}

#########################################
# Archive the test results  with charts generated
fun_pp_archive() {
    local test_zip=$1
 
    zip -9mqj ${test_zip} *.json *.out *.log *.plot *.png *_list *.dat *.svg
}
#########################################
# Post-process data into charts
fun_post_process_cold() {
  local TEST_RESULT=$1
  for x in ${TEST_RESULT}*_top.out; do
      y=${x/_top.out/_cpu_avg.json}
      z=${x/_top.out/_pid.json}

      mon_filter_top $x ${y} ${z}
      run_gnuplot
      #fun_pp_top $x "0-111" ${CPU_AVG} ${TOP_PID_JSON}
  done
}

#########################################
# Run  async vs crimson messenger tests
# Iterate over the number of CPU cores for --smp == num_clients
fun_run_msgr_tests() {
    local BAL_TYPE=$1
    local MESG_TYPE=$2

    if [ "${MAX}" == "true" ]; then
        echo -e "${GREEN}== Running multiple server/client pairs ==${NC}"
        for KEY in left right; do
            fun_run_fixed_msgr_test ${MESG_TYPE} ${max_smp_range[${KEY}]} ${BAL_TYPE} ${KEY} 
        done
        # Since the pids[] array has got the left and right client/server, then monitor them all
        sleep 5 # ramp up time
        fun_monitor_pair ${MESG_TYPE} ${max_smp_range[${KEY}]} ${BAL_TYPE}
        wait
    else 
        for SMP in ${smp_range[${BAL_TYPE}]} ; do #${SMP_RANGE_CPU}
            echo -e "${GREEN}== ${MESG_TYPE}  SMP: ${SMP} ==${NC}"
            #for KEY in "${!mesgr_ops_table[@]}"; do
            fun_run_fixed_msgr_test ${MESG_TYPE} ${SMP} ${BAL_TYPE} ""
            sleep 5 # ramp up time
            fun_monitor_pair ${MESG_TYPE} ${SMP} ${BAL_TYPE}
            wait
        done
    fi
}

#########################################
# Run balanced vs separated tests
fun_run_bal_tests() {
    local BAL_TYPE=$1
    local MESG_TYPE=$2

    if [ "$BAL_TYPE" == "all" ]; then
        echo -e "${GREEN}== Running all balanced ==${NC}"
        for BAL_TYPE in balanced separated; do
            fun_run_msgr_tests  ${BAL_TYPE} ${MESG_TYPE}
        done
    else
        fun_run_msgr_tests  ${BAL_TYPE} ${MESG_TYPE}
    fi
}

#########################################
# Main:
#
#cd /ceph/build/bin

while getopts 'b:d:t:sg:fx' option; do
  case "$option" in
    b) BAL_TYPE=$OPTARG
        ;;
    d) RUN_DIR=$OPTARG
        ;;
    s) fun_show_grid $OPTARG
       exit
        ;;
    t) MESG_TYPE=$OPTARG
        ;;
    f) FLAME=true
        ;;
    # Later, extend to n max number of processes: max_num_cpus/(2*n)
    x) MAX=true
        ;;
    g) POST_PROC=$OPTARG #=true
        ;;
    :) printf "missing argument for -%s\n" "$OPTARG" >&2
       usage >&2
       exit 1
       ;;
  esac
 done

 # Create the run directory if it does not exist
 [ ! -d "${RUN_DIR}" ] && mkdir -p ${RUN_DIR}
 cd ${RUN_DIR} 
 if [ ! -z "$POST_PROC" ]; then
    echo -e "${GREEN}== Post-processing ${POST_PROC} ==${NC}"
    # TBC. fun_post_process_cold ${}
    exit
 fi

 if [ "$MESG_TYPE" == "all" ]; then
     for MESG_TYPE in crimson async; do
         fun_run_bal_tests ${BAL_TYPE} ${MESG_TYPE}
     done
 else
     fun_run_bal_tests ${BAL_TYPE} ${MESG_TYPE}
 fi

exit

#########################################
