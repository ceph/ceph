#!/bin/bash
 
# A simple script used by Red Hat to start teuthology-worker processes.
 
ARCHIVE=${ARCHIVE:-"$HOME/archive"}
WORKER_LOGS=$ARCHIVE/worker_logs
 
function start_workers_for_tube {
    echo "Starting $2 workers for $1"
    for i in `seq 1 $2`
    do
        teuthology-worker -v --archive-dir $ARCHIVE --tube $1 --log-dir $WORKER_LOGS &
    done
}
 
function start_all {
    start_workers_for_tube plana 50
    start_workers_for_tube mira 50
    start_workers_for_tube vps 80
    start_workers_for_tube burnupi 10
    start_workers_for_tube tala 5
    start_workers_for_tube saya 10
    start_workers_for_tube multi 100
}
 
function main {
    printf '%s\n' "$*"
    if [[ -z "$*" ]]
    then
        start_all
    elif [ ! -z "$2" ] && [ "$2" -gt "0" ]
    then
        start_workers_for_tube $1 $2
    else
        echo "usage: $0 [tube_name number_of_workers]" >&2
        exit 1
    fi
}
 
main "$@"
