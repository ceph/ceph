#!/bin/bash -e
#
# ceph-backport.sh
#
# Ceph backporting script
#
# Assumes you have forked ceph/ceph.git, cloned your fork, and are running the
# script in the local clone!
#
# With this script, backporting workflow for backport issue
# http://tracker.ceph.com/issues/19206 (a jewel backport)
# becomes something like this:
#
# git remote add ceph http://github.com/ceph/ceph.git
# git fetch ceph
# git checkout -b wip-19206-jewel ceph/jewel
# git cherry-pick -x ...
# ceph-backport.sh 19206 jewel
#
# The script takes care of opening the backport PR, updating the tracker issue,
# and cross-linking the backport PR with the tracker issue.
#
# However, before you start you will need to find the right values for
# the following:
#
# redmine_key     # "My account" -> "API access key" -> "Show"
# redmine_user_id # "Logged in as foobar", click on foobar link, Redmine User ID
                  # is in the URL, i.e. http://tracker.ceph.com/users/[redmine_user_id]
# github_token    # https://github.com/settings/tokens -> Generate new token ->
                  # ensure it has "Full control of private repositories" scope
# github_user     # Your github username
#
# Once you have the actual values for these three variables, create a file
# $HOME/bin/backport_common.sh with the following contents
# 
# redmine_key=[your_redmine_key]
# redmine_user_id=[your_redmine_user_id]
# github_token=[your_github_personal_access_token]
# github_user=[your_github_username]
#
# you can also optionally add the remote repo's name in this file, like
#
# github_repo=[your_github_repo_name]
#
# If you don't add it, it will default to "origin".
#
# Obviously, since this file contains secrets, you should protect it from
# exposure using all available means (restricted file privileges, encrypted
# filesystem, etc.). Without correct values for these four variables, this
# script will not work!
#
source $HOME/bin/backport_common.sh

function failed_required_variable_check () {
    local varname=$1
    echo "$0: $varname not defined. Did you create $HOME/bin/backport_common.sh?"
    echo "(For instructions, see comment block at beginning of script)"
    exit 1
}

test "$redmine_key"     || failed_required_variable_check redmine_key
test "$redmine_user_id" || failed_required_variable_check redmine_user_id
test "$github_token"    || failed_required_variable_check github_token
test "$github_user"     || failed_required_variable_check github_user
: "${github_repo:=origin}"

function usage () {
    echo "Usage:"
    echo "   $0 [BACKPORT_TRACKER_ISSUE_NUMBER] [MILESTONE]"
    echo
    echo "Example:"
    echo "   $0 19206 jewel"
    echo
    echo "If MILESTONE is not given on the command line, the script will"
    echo "try to use the value of the MILESTONE environment variable, if set."
    echo
    echo "The script must be run from inside the local git clone"
    exit 1
}

[[ $1 =~ ^[0-9]+$ ]] || usage
issue=$1
echo "Backport issue: $issue"

milestone=
test "$2" && milestone="$2"
if [ -z "$milestone" ] ; then
    test "$MILESTONE" && milestone="$MILESTONE"
fi
test "$milestone" || usage
echo "Milestone: $milestone"

# ------------------------------------
# How to find out the milestone number
# ------------------------------------
# can't seem to extract the milestone number with the API
# milestone numbers can be obtained with:
#   curl --verbose -X GET https://api.github.com/repos/ceph/ceph/milestones

if [[ "x$milestone" = "xhammer" ]] ; then
    milestone_number=5
    target_branch=hammer
elif [[ "x$milestone" = "xjewel" ]] ; then
    milestone_number=8
    target_branch=jewel
elif [[ "x$milestone" = "xkraken" ]] ; then
    milestone_number=9
    target_branch=kraken
elif [[ "x$milestone" = "xluminous" ]] ; then
    milestone_number=10
    target_branch=luminous
elif [[ "x$milestone" = "xmimic" ]] ; then
    milestone_number=11
    target_branch=mimic
elif [[ "x$milestone" = "xnautilus" ]] ; then
    milestone_number=12
    target_branch=nautilus
else
    echo "Please enter hammer, jewel, kraken, luminous, mimic, or nautilus"
    exit 1
fi
echo "Milestone is $milestone and milestone number is $milestone_number"

if [ $(curl --silent http://tracker.ceph.com/issues/$issue.json | jq -r .issue.tracker.name) != "Backport" ]
then
    echo "http://tracker.ceph.com/issues/$issue is not a backport (edit and change tracker?)"
    exit 1
fi

title=$(curl --silent 'http://tracker.ceph.com/issues/'$issue.json?key=$redmine_key | jq .issue.subject | tr -d '\\"')
echo "Issue title: $title" 

git push -u $github_repo wip-$issue-$milestone

number=$(curl --silent --data-binary '{"title":"'"$title"'","head":"'$github_user':wip-'$issue-$milestone'","base":"'$target_branch'","body":"http://tracker.ceph.com/issues/'$issue'"}' 'https://api.github.com/repos/ceph/ceph/pulls?access_token='$github_token | jq .number)
echo "Opened pull request $number"

component=core ; curl --silent --data-binary '{"milestone":'$milestone_number',"assignee":"'$github_user'","labels":["bug fix","'$component'"]}' 'https://api.github.com/repos/ceph/ceph/issues/'$number'?access_token='$github_token
firefox https://github.com/ceph/ceph/pull/$number
redmine_status=2 # In Progress
curl --verbose -X PUT --header 'Content-type: application/json' --data-binary '{"issue":{"description":"https://github.com/ceph/ceph/pull/'$number'","status_id":'$redmine_status',"assigned_to_id":'$redmine_user_id'}}' 'http://tracker.ceph.com/issues/'$issue.json?key=$redmine_key
echo "Staged http://tracker.ceph.com/issues/$issue"

firefox http://tracker.ceph.com/issues/$issue
