#!/bin/bash -e
#
# ceph-backport.sh - Ceph backporting script
#
# Credits: This script is based on work done by Loic Dachary
#
# With proper setup, this script takes care of opening the backport PR,
# updating the corresponding backport tracker issue, and cross-linking the
# backport PR with the tracker issue.
#
# However, before you start, some setup is required. Please be patient and
# read the "Setup instructions" and "Instructions for use" sections, below,
# carefully. If you still have issues, you could read the "Troubleshooting
# notes" section as well.
#
#
# Setup instructions
# ------------------
#
# It is strongly suggested to copy the latest version of the script (from
# the "master" branch) into your PATH. In particular, do not use any version
# of the script from a stable (named) branch such as "nautilus", as these
# versions are not maintained. Only the version in master is maintained.
#
# You will need to find the right values for the following:
#
# redmine_key     # "My account" -> "API access key" -> "Show"
# redmine_user_id # "Logged in as foobar", click on foobar link, Redmine User ID
                  # is in the URL, i.e. https://tracker.ceph.com/users/[redmine_user_id]
# github_token    # https://github.com/settings/tokens -> Generate new token ->
                  # ensure it has "Full control of private repositories" scope
# github_user     # Your github username
#
# Once you have the actual values for the above variables, create a file
# $HOME/bin/backport_common.sh with the following contents
#
# redmine_key=[your_redmine_key]
# redmine_user_id=[your_redmine_user_id]
# github_token=[your_github_personal_access_token]
# github_user=[your_github_username]
#
# You can also optionally add yours and ceph remote repo's name in this file,
# like
#
# github_repo=[your_github_repo_name]
# ceph_repo=[ceph_github_repo_name]
#
# If you don't add it, it will default to "origin" and "upstream", respectively.
#
# Obviously, since this file contains secrets, you should protect it from
# exposure using all available means (restricted file privileges, encrypted
# filesystem, etc.). Without correct values for these four variables, this
# script will not work!
#
#
# Instructions for use
# --------------------
#
# Assumes you have forked ceph/ceph.git, cloned your fork, and are running the
# script in the local clone!
#
# With this script, backporting workflow for backport issue
# https://tracker.ceph.com/issues/19206 (a jewel backport)
# becomes something like this:
#
# For simple backports you can just run:
#
# ceph-backport.sh 19206 --prepare
# ceph-backport.sh 19206
#
# Alternatively, instead of running the script with --prepare you can prepare
# the backport manually:
#
# git remote add ceph http://github.com/ceph/ceph.git
# git fetch ceph
# git checkout -b wip-19206-jewel ceph/jewel
# git cherry-pick -x ...
# ceph-backport.sh 19206
#
# Optionally, you can set the component label that will be added to the PR with
# an environment variable:
#
# COMPONENT=dashboard ceph-backport.sh 40056
#
#
# Troubleshooting notes
# ---------------------
#
# If the script inexplicably fails with:
#
#     error: a cherry-pick or revert is already in progress
#     hint: try "git cherry-pick (--continue | --quit | --abort)"
#     fatal: cherry-pick failed
#
# This is because HEAD is not where git expects it to be:
#
#     $ git cherry-pick --abort
#     warning: You seem to have moved HEAD. Not rewinding, check your HEAD!
#
# This can be fixed by issuing the command:
#
#     $ git cherry-pick --quit
#
#
# Reporting bugs
# --------------
#
# Please report any bugs in this script to https://tracker.ceph.com/projects/ceph/issues/new
#
# (Ideally, the bug report would include a typescript obtained while
# reproducing the bug with the --debug option. To understand what is meant by
# "typescript", see "man script".)
#
#
# Other backporting resources
# ---------------------------
#
# See https://tracker.ceph.com/projects/ceph-releases/wiki/HOWTO_backport_commits
# for more info on cherry-picking.
#
# Happy backporting!
# Nathan
#
source $HOME/bin/backport_common.sh
this_script=$(basename "$0")
verbose=

if [[ $* == *--debug* ]]; then
    set -x
    verbose="1"
fi

if [[ $* == *--verbose* ]]; then
    verbose="1"
fi

function log {
    local level="$1"
    shift
    local msg="$@"
    prefix="${this_script}: "
    verbose_only=
    case $level in
        err*)
            prefix="${prefix}ERROR: "
            ;;
        info)
            :
            ;;
        bare)
            prefix=
            ;;
        warn|warning)
            prefix="${prefix}WARNING: "
            ;;
        debug|verbose)
            prefix="${prefix}DEBUG: "
            verbose_only="1"
            ;;
    esac
    if [ "$verbose_only" -a ! "$verbose" ] ; then
        true
    else
        msg="${prefix}${msg}"
        echo "$msg" >&2
    fi
}

function error {
    log error $@
}

function warning {
    log warning $@
}

function info {
    log info $@
}

function debug {
    log debug $@
}

function failed_required_variable_check {
    local varname="$1"
    error "$varname not defined. Did you create $HOME/bin/backport_common.sh?"
    info "(For detailed instructions, see comment block at the beginning of the script)"
    exit 1
}

debug Checking mandatory variables
test "$redmine_key"     || failed_required_variable_check redmine_key
test "$redmine_user_id" || failed_required_variable_check redmine_user_id
test "$github_token"    || failed_required_variable_check github_token
test "$github_user"     || failed_required_variable_check github_user
true "${github_repo:=origin}"
true "${ceph_repo:=upstream}"
redmine_endpoint="https://tracker.ceph.com"
github_endpoint="https://github.com/ceph/ceph"
original_issue=
original_pr=

function usage {
    log bare
    log bare "Usage:"
    log bare "   ${this_script} BACKPORT_TRACKER_ISSUE_NUMBER [--debug] [--prepare] [--verbose]"
    log bare
    log bare "Example:"
    log bare "   ${this_script} 19206 --prepare"
    log bare
    log bare "The script must be run from inside a local git clone."
    log bare
    log bare "Documentation can be found in the comment block at the top of the script itself."
    exit 1
}

function populate_original_issue {
    if [ -z "$original_issue" ] ; then
        original_issue=$(curl --silent ${redmine_url}.json?include=relations |
            jq '.issue.relations[] | select(.relation_type | contains("copied_to")) | .issue_id')
    fi
}

function populate_original_pr {
    if [ "$original_issue" ] ; then
        if [ -z "$original_pr" ] ; then
            original_pr=$(curl --silent ${redmine_endpoint}/issues/${original_issue}.json |
                          jq -r '.issue.custom_fields[] | select(.id | contains(21)) | .value')
        fi
    fi
}

function prepare {
    local offset=0
    populate_original_issue
    if [ -z "$original_issue" ] ; then
        error "Could not find original issue"
        info "Does ${redmine_url} have a \"Copied from\" relation?"
        exit 1
    fi
    info "Parent issue: ${redmine_endpoint}/issues/${original_issue}"

    populate_original_pr
    if [ -z "$original_pr" ]; then
        error "Could not find original PR"
        info "Is the \"Pull request ID\" field of ${redmine_endpoint}/issues/${original_issue} populated?"
        exit 1
    fi
    info "Parent issue ostensibly fixed by: ${github_endpoint}/pull/${original_pr}"

    debug "Counting commits in ${github_endpoint}/pull/${original_pr}"
    number=$(curl --silent https://api.github.com/repos/ceph/ceph/pulls/${original_pr}?access_token=${github_token} | jq .commits)
    if [ -z "$number" ] ; then
        error "Could not determine the number of commits in ${github_endpoint}/pull/${original_pr}"
        return 1
    fi
    info "Found $number commits in ${github_endpoint}/pull/${original_pr}"

    git fetch $ceph_repo
    debug "Fetched latest commits from upstream"

    git checkout $ceph_repo/$milestone -b $local_branch

    git fetch $ceph_repo pull/$original_pr/head:pr-$original_pr

    debug "Cherry picking $number commits from ${github_endpoint}/pull/${original_pr} into local branch $local_branch"
    debug "If this operation does not succeed, you will need to resolve the conflicts manually"
    let offset=$number-1 || true # don't fail on set -e when result is 0
    for ((i=$offset; i>=0; i--))
    do
        debug "Cherry-picking commit $(git log --oneline --max-count=1 --no-decorate)"
        git cherry-pick -x "pr-$original_pr~$i"
    done
    info "Cherry picked $number commits from ${github_endpoint}/pull/${original_pr} into local branch $local_branch"
}

if git show-ref HEAD >/dev/null 2>&1 ; then
    debug "In a local git clone. Good."
else
    error "This script must be run from inside a local git clone"
    exit 1
fi

if [ $verbose ] ; then
    debug "Redmine user: ${redmine_user_id}"
    debug "GitHub user:  ${github_user}"
    debug "Fork remote:  ${github_repo}"
    if git remote -v | egrep ^${github_repo}\\s+ ; then
        true  # remote exists; good
    else
        error "github_repo is set to ->$github_repo<- but this remote does not exist"
        exit 1
    fi
    debug "Ceph remote:  ${ceph_repo}"
    if git remote -v | egrep ^${ceph_repo}\\s+ ; then
        true  # remote exists; good
    else
        error "ceph_repo is set to ->$ceph_repo<- but this remote does not exist"
        exit 1
    fi
fi

if [[ $1 =~ ^[0-9]+$ ]] ; then
    issue=$1
else
    error "Invalid or missing argument"
    usage  # does not return
fi

redmine_url="${redmine_endpoint}/issues/${issue}"
debug "Considering Redmine issue: $redmine_url - is it in the Backport tracker?"

tracker=$(curl --silent "${redmine_url}.json" | jq -r '.issue.tracker.name')
if [ "$tracker" = "Backport" ]; then
    debug "Yes, $redmine_url is a Backport issue"
else
    error "Issue $redmine_url is not a Backport"
    info "(This script only works with Backport tracker issues.)"
    exit 1
fi

debug "Looking up release/milestone of $redmine_url"
milestone=$(curl --silent "${redmine_url}.json" | jq -r '.issue.custom_fields[0].value')
if [ "$milestone" ] ; then
    debug "Release/milestone: $milestone"
else
    error "could not obtain release/milestone from ${redmine_url}"
    exit 1
fi

# milestone numbers can be obtained manually with:
#   curl --verbose -X GET https://api.github.com/repos/ceph/ceph/milestones
milestone_number=$(curl -s -X GET 'https://api.github.com/repos/ceph/ceph/milestones?access_token='$github_token | jq --arg milestone $milestone '.[] | select(.title==$milestone) | .number')
if test -n "$milestone_number" ; then
    target_branch="$milestone"
else
    error "Unsupported milestone"
    info "Valid values are $(curl -s -X GET 'https://api.github.com/repos/ceph/ceph/milestones?access_token='$github_token | jq '.[].title')"
    info "(This probably means the Release field of ${redmine_url} is populated with"
    info "an unexpected value - i.e. it does not match any of the GitHub milestones.)"
    exit 1
fi
info "Milestone/release is $milestone"
debug "Milestone number is $milestone_number"

local_branch=wip-${issue}-${target_branch}

if [ $(curl --silent ${redmine_url}.json | jq -r .issue.tracker.name) != "Backport" ]
then
    error "${redmine_endpoint}/issues/${issue} is not in the Backport tracker"
    exit 1
fi

if [[ $* == *--prepare* ]]; then
    debug "'--prepare' found, will only prepare the backport"
    prepare
fi

debug "Pushing local branch $local_branch to remote $github_repo"
git push -u $github_repo $local_branch

debug "Generating backport PR description"
populate_original_issue
populate_original_pr
desc="backport tracker: ${redmine_url}"
if [ "$original_pr" -o "$original_issue" ] ; then
    desc="${desc}\n\n---\n"
    [ "$original_pr"    ] && desc="${desc}\nbackport of ${github_endpoint}/pull/${original_pr}"
    [ "$original_issue" ] && desc="${desc}\nparent tracker: ${redmine_endpoint}/issues/${original_issue}"
fi
desc="${desc}\n\nthis backport was staged using ${github_endpoint}/blob/master/src/script/ceph-backport.sh"

debug "Generating backport PR title"
title="${milestone}: $(curl --silent https://api.github.com/repos/ceph/ceph/pulls/${original_pr} | jq -r '.title')"
if [[ $title =~ \" ]] ; then
    title=$(echo $title | sed -e 's/"/\\"/g')
fi

debug "Opening backport PR"
number=$(curl --silent --data-binary '{"title":"'"$title"'","head":"'$github_user':'$local_branch'","base":"'$target_branch'","body":"'"${desc}"'"}' 'https://api.github.com/repos/ceph/ceph/pulls?access_token='$github_token | jq -r .number)
component=${COMPONENT:-core}
info "Opened backport PR ${github_endpoint}/pull/$number"
debug "Setting ${component} label"
curl --silent --data-binary '{"milestone":'$milestone_number',"labels":["'$component'"]}' 'https://api.github.com/repos/ceph/ceph/issues/'$number'?access_token='$github_token >/dev/null
info "Set ${component} label in PR"
pgrep firefox >/dev/null && firefox ${github_endpoint}/pull/$number

debug "Updating backport tracker issue in Redmine"
redmine_status=2 # In Progress
curl -X PUT --header 'Content-type: application/json' --data-binary '{"issue":{"description":"https://github.com/ceph/ceph/pull/'$number'","status_id":'$redmine_status',"assigned_to_id":'$redmine_user_id'}}' ${redmine_url}'.json?key='$redmine_key
info "${redmine_url} updated"
pgrep firefox >/dev/null && firefox ${redmine_url}
