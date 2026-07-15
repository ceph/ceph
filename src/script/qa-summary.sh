#!/usr/bin/env bash 
set -e

#
# qa-summary.sh - Script to automate QA Batch summaries
#
# Help and usage:
#    qa-summary.sh --help

function print_help {
    cat <<EOM >&2

Help:

   qa-summary.sh --help

Usage:
   qa-summary.sh < test_failure_tickets.txt {--detail}

Before running the script, prep a 'test_failure_tickets.txt' file
(name is subjective) containing links to all the tracker tickets
you want to format in your test failures summary.
For example:

$ cat test_failure_tickets.txt
  https://tracker.ceph.com/issues/68586
  https://tracker.ceph.com/issues/69827
  https://tracker.ceph.com/issues/67869
  https://tracker.ceph.com/issues/71344
  https://tracker.ceph.com/issues/70669
  https://tracker.ceph.com/issues/71506
  https://tracker.ceph.com/issues/71182

To print more detail about assignee, priority, and status
for each ticket, append the "--detail" flag to your command.

EOM
}

DETAIL=false

function extract_json {
   redmine_url=$1
   json_output="$(curl --silent "${redmine_url}.json")"
   echo "$json_output"
}


if [ "$1" == "--help" ]; then
    print_help
    exit
fi

if [ "$1" == "--detail" ]; then
    DETAIL=true
fi

# Check for std input
if [[ -t 0 ]]
then
    echo "ERROR: Must provide a file of tracker tickets URLs to summarize as std input (<)." \
	 "See 'qa-summary.sh --help' for proper usage."
    exit
fi

printf "\nFailures, unrelated:\n\n"
failure_num=1
while IFS= read -r arg || [ -n "$arg" ]; do
    json_output="$(extract_json $arg)"
    project="$(echo "$json_output" | jq -r '.issue.project.name')"
    subject="$(echo "$json_output" | jq -r '.issue.subject')"
    assignee="$(echo "$json_output" | jq -r '.issue.assigned_to.name')"
    priority="$(echo "$json_output" | jq -r '.issue.priority.name')"
    stat="$(echo "$json_output" | jq -r '.issue.status.name')"
    if [ -z "${project}" ]; then
         echo "Could not find a project for the following ticket: $arg"
         exit
    fi
    if [ -z "${subject}" ]; then
         echo "Could not find a subject for the following ticket: $arg"
	 exit
    fi
    if [ -z "${priority}" ]; then
         echo "Could not find a priority for the following ticket: $arg"
         exit
    fi
    if [ -z "${stat}" ]; then
         echo "Could not find a status for the following ticket: $arg"
         exit
    fi
    echo "$failure_num. $arg - $subject - ($project)"
    if $DETAIL; then
        echo "   Priority: $priority"
        echo "   Assignee: $assignee"
        echo "   Status: $stat"
    fi
    ((failure_num++))
done

printf "\nDONE!\n\n"
