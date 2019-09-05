#!/bin/bash -e
#
# ceph-backport.sh - Ceph backporting script
#
# Credits: This script is based on work done by Loic Dachary
#
# With proper setup, this script automates the entire process of creating a
# backport -- all it needs is the number of the Backport tracker issue.
# Working in a local clone, the script creates a properly named wip branch for
# the backport, fetches the commits from GitHub, and cherry-picks them. If all
# the commits cherry-pick cleanly, the script goes on to open the backport PR,
# update the Backport tracker issue, and cross-linking the backport PR with the
# tracker issue.
#
# Some setup is required for the script to work -- this is described
# in the "Setup instructions" and "Instructions for use" sections, below.
# If issues persist even after reading and following those instructions
# carefully, the "Troubleshooting notes" and "Reporting bugs" sections might
# come in handy, as well.
#
#
# Setup instructions
# ------------------
#
# It is strongly suggested to copy the latest version of the script (from
# the "master" branch) into the PATH. In particular, do not use any version
# of the script from a stable (named) branch such as "nautilus", as these
# versions are not maintained. Only the version in master is maintained.
#
# The script needs correct values for the following:
#
# redmine_key     # "My account" -> "API access key" -> "Show"
# redmine_user_id # "Logged in as foobar", click on foobar link, Redmine User ID
                  # is in the URL, i.e. https://tracker.ceph.com/users/[redmine_user_id]
# github_token    # https://github.com/settings/tokens -> Generate new token ->
                  # ensure it has "Full control of private repositories" scope
# github_user     # Your github username
#
# If one or more of these variables are not found in the environment, the
# script will attempt to source a file $HOME/bin/backport_common.sh.
# (Obviously, since that file might contain secrets, it should be protected from
# exposure using all available means - restricted file privileges, encrypted
# filesystem, etc.)
#
# The above variables must be set explicitly, as the script has no way of
# determining reasonable defaults. In addition, the following two variables
# should at least be checked against the output of "git remote -v" in your clone:
#
# github_repo     # The "git remote" name of your Ceph fork on GitHub;
#                 # defaults to "origin" if not set
# ceph_repo       # The "git remote" name of the Ceph repo on GitHub;
#                 # defaults to "upstream" if not set
#
# Without correct values for all of the above variables, this script will not
# work!
#
#
# Instructions for use
# --------------------
#
# First, ensure that the latest version of this script (from
# src/script/ceph-backport.sh in the "master" branch of ceph/ceph.git) has been
# copied somewhere in your PATH.
#
# Second, change the working directory ("cd") to the top-level directory of the
# local clone.
#
# Third, choose a Backport tracker issue you would like to stage a backport for.
# Let's assume the Backport tracker issue you chose has the number 31459 and 
# the backport targets luminous.
#
# Then run:
#
#     ceph-backport.sh 31459
#
# Assuming 31459 is a properly-linked Backport issue and the "Pull request ID"
# field of the parent issue has been correctly populated, the script will
# automatically:
#
# 1. determine which stable branch the backport is targeting (luminous)
# 2. create a local backport branch, wip-31459-luminous, based on the tip of
#    luminous
# 3. determine parent issue and master PR and cherry-pick the commits from the
#    master PR
#
# If there were no cherry-pick conflicts, the script will continue (see steps
# 4-6, below).
#
# If any commit fails to cherry-pick cleanly, the script will abort, giving
# the user an opportunity to resolve the conflicts manually and re-run the
# script. When the script sees that the local backport branch (wip-31459-luminous)
# already exists, it assumes that cherry-picking has been completed, and starts
# from step 4:
#
# 4. push the wip branch to the user's fork
# 5. open the backport PR on GitHub with the correct Milestone setting
# 6. properly cross-link the PR with the Backport tracker issue
#
# Finally, if a process called "firefox" is running, the script will open the
# PR and the updated tracker issue in that web browser to facilitate visual
# confirmation.
#
# Alternatively, instead of running the script with --prepare the user can
# prepare the backport manually. For a a luminous backport whose Backport
# tracker ID is 31459, the process would be:
#
# git remote add ceph http://github.com/ceph/ceph.git
# git fetch ceph
# git checkout -b wip-31459-luminous ceph/jewel
# git cherry-pick -x ...
# (resolve conflicts if necessary)
#
# CAVEAT: the local branch name must be "wip-31459-luminous", where 31459 is
# the number of a Redmine issue in the Backport tracker, with Release set to
# luminous. The script will not work, otherwise.
#
# Finally, run the script:
#
#     ceph-backport.sh 31459
#
# Optionally, the component label that will be added to the PR can be set with
# an environment variable:
#
#     COMPONENT=dashboard ceph-backport.sh 31459
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
# Be sure to mention the exact version of git you are using.
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

this_script=$(basename "$0")

function debug {
    log debug $@
}

function error {
    log error $@
}

function failed_required_variable_check {
    local varname="$1"
    error "$varname not defined. Did you create $HOME/bin/backport_common.sh?"
    info "(For detailed instructions, see comment block at the beginning of the script)"
    exit 1
}

function info {
    log info $@
}

function init_mandatory_vars {
    debug Initializing mandatory variables
    test "$redmine_key"     || failed_required_variable_check redmine_key
    test "$redmine_user_id" || failed_required_variable_check redmine_user_id
    test "$github_token"    || failed_required_variable_check github_token
    test "$github_user"     || failed_required_variable_check github_user
    true "${github_repo:=origin}"
    true "${ceph_repo:=upstream}"
    true "${redmine_endpoint:="https://tracker.ceph.com"}"
    true "${github_endpoint:="https://github.com/ceph/ceph"}"
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
}

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
            original_pr_url="${github_endpoint}/pull/${original_pr}"
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
    info "Parent issue ostensibly fixed by: ${original_pr_url}"

    debug "Counting commits in ${original_pr_url}"
    number=$(curl --silent https://api.github.com/repos/ceph/ceph/pulls/${original_pr}?access_token=${github_token} | jq .commits)
    if [ -z "$number" ] ; then
        error "Could not determine the number of commits in ${original_pr_url}"
        return 1
    fi
    info "Found $number commits in ${original_pr_url}"

    debug "Fetching latest commits from $ceph_repo"
    git fetch $ceph_repo

    debug "Initializing local branch $local_branch to $milestone"
    if git show-ref --verify --quiet refs/heads/$local_branch ; then
        error "Cannot initialize $local_branch - local branch already exists"
        exit 1
    else
        git checkout $ceph_repo/$milestone -b $local_branch
    fi

    debug "Fetching latest commits from ${original_pr_url}"
    git fetch $ceph_repo pull/$original_pr/head:pr-$original_pr

    info "Attempting to cherry pick $number commits from ${original_pr_url} into local branch $local_branch"
    let offset=$number-1 || true # don't fail on set -e when result is 0
    for ((i=$offset; i>=0; i--)) ; do
        debug "Cherry-picking commit $(git log --oneline --max-count=1 --no-decorate pr-$original_pr~$i)"
        if git cherry-pick -x "pr-$original_pr~$i" ; then
            true
        else
            if [ "$VERBOSE" ] ; then
                git status
            fi
            error "Cherry pick failed"
            info "Next, manually fix conflicts and complete the current cherry-pick"
            if [ "$i" -gt "0" ] ; then
                info "Then, cherry-pick the remaining commits from ${original_pr_url}, i.e.:"
                for ((j=$i-1; j>=0; j--)) ; do
                    info "-> missing commit: $(git log --oneline --max-count=1 --no-decorate pr-$original_pr~$j)"
                done
                info "Finally, re-run the script"
            else
                info "Then re-run the script"
            fi
            exit 1
        fi
    done
    info "Cherry picking completed without conflicts"
}

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

function warning {
    log warning $@
}


if git show-ref HEAD >/dev/null 2>&1 ; then
    debug "In a local git clone. Good."
else
    error "This script must be run from inside a local git clone"
    exit 1
fi


#
# process command-line arguments
#

munged_options=$(getopt -o dhpv --long "debug,help,prepare,verbose" -n "$this_script" -- "$@")
eval set -- "$munged_options"

DEBUG=""
HELP=""
ISSUE=""
EXPLICIT_PREPARE=""
VERBOSE=""
while true ; do
    case "$1" in
        --debug|-d) DEBUG="$1" ; shift ;;
        --help|-h) HELP="$1" ; shift ;;
        --prepare|-p) EXPLICIT_PREPARE="$1" ; shift ;;
        --verbose|-v) VERBOSE="$1" ; shift ;;
        --) shift ; ISSUE="$1" ; break ;;
        *) echo "Internal error" ; false ;;
    esac
done

if [[ $ISSUE =~ ^[0-9]+$ ]] ; then
    issue=$ISSUE
else
    error "Invalid or missing argument"
    usage  # does not return
fi

if [ "$DEBUG" ]; then
    set -x
    VERBOSE="--verbose"
fi

if [ "$HELP" ]; then
    usage  # does not return
fi

if [ "$VERBOSE" ]; then
    VERBOSE="--verbose"
fi


#
# initialize mandatory variables and check values for sanity
#

BACKPORT_COMMON=$HOME/bin/backport_common.sh
[ -f "$BACKPORT_COMMON" ] && source $HOME/bin/backport_common.sh
init_mandatory_vars


#
# query remote Redmine API for information about the Backport tracker issue
#

redmine_url="${redmine_endpoint}/issues/${issue}"
debug "Considering Redmine issue: $redmine_url - is it in the Backport tracker?"

remote_api_output=$(curl --silent "${redmine_url}.json")
tracker=$(echo $remote_api_output | jq -r '.issue.tracker.name')
if [ "$tracker" = "Backport" ]; then
    debug "Yes, $redmine_url is a Backport issue"
else
    error "Issue $redmine_url is not a Backport"
    info "(This script only works with Backport tracker issues.)"
    exit 1
fi

debug "Looking up release/milestone of $redmine_url"
milestone=$(echo $remote_api_output | jq -r '.issue.custom_fields[0].value')
if [ "$milestone" ] ; then
    debug "Release/milestone: $milestone"
else
    error "could not obtain release/milestone from ${redmine_url}"
    exit 1
fi

tracker_title=$(echo $remote_api_output | jq -r '.issue.subject')
debug "Title of $redmine_url is ->$tracker_title<-"

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


#
# --prepare phase
#

local_branch=wip-${issue}-${target_branch}
if git show-ref --verify --quiet refs/heads/$local_branch ; then
    if [ "$EXPLICIT_PREPARE" ] ; then
        error "local branch $local_branch already exists -- cannot --prepare"
        exit 1
    fi
    warning "local branch $local_branch already exists: skipping cherry-pick phase!"
    PREPARE=""
else
    info "$local_branch does not exist: will create it and attempt automated cherry-pick"
    PREPARE="--prepare"
fi
[ "$PREPARE" ] && prepare


#
# at this point, local branch exists and is assumed to contain cherry-pick(s)
#

debug "Pushing local branch $local_branch to remote $github_repo"
git push -u $github_repo $local_branch

original_issue=""
original_pr=""
original_pr_url=""

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
if [ "$original_pr" ] ; then
    title="${milestone}: $(curl --silent https://api.github.com/repos/ceph/ceph/pulls/${original_pr} | jq -r '.title')"
else
    if [[ $tracker_title =~ ^${milestone}: ]] ; then
        title="${tracker_title}"
    else
        title="${milestone}: ${tracker_title}"
    fi
fi
if [[ $title =~ \" ]] ; then
    title=$(echo $title | sed -e 's/"/\\"/g')
fi

debug "Opening backport PR"
remote_api_output=$(curl --silent --data-binary '{"title":"'"$title"'","head":"'$github_user':'$local_branch'","base":"'$target_branch'","body":"'"${desc}"'"}' 'https://api.github.com/repos/ceph/ceph/pulls?access_token='$github_token)
number=$(echo "$remote_api_output" | jq -r .number)
if [ "$number" = "null" ] ; then
    error "Remote API call failed"
    echo "$remote_api_output"
    exit 1
fi
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
