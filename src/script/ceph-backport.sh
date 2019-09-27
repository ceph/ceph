#!/bin/bash -e
#
# ceph-backport.sh - Ceph backporting script
#
# Credits: This script is based on work done by Loic Dachary
#
#
# This script automates the process of staging a backport starting from a
# Backport tracker issue.
#
# Setup, usage and troubleshooting:
#
#     ceph-backport.sh --help
#     ceph-backport.sh --setup-advice
#     ceph-backport.sh --usage-advice
#     ceph-backport.sh --troubleshooting-advice
#
#

this_script=$(basename "$0")
how_to_get_setup_advice="For setup advice, run:  ${this_script} --setup-advice"

if [[ $* == *--debug* ]]; then
    set -x
fi

function cherry_pick_phase {
    local offset=0
    populate_original_issue
    if [ -z "$original_issue" ] ; then
        error "Could not find original issue"
        info "Does ${redmine_url} have a \"Copied from\" relation?"
        false
    fi
    info "Parent issue: ${redmine_endpoint}/issues/${original_issue}"

    populate_original_pr
    if [ -z "$original_pr" ]; then
        error "Could not find original PR"
        info "Is the \"Pull request ID\" field of ${redmine_endpoint}/issues/${original_issue} populated?"
        false
    fi
    info "Parent issue ostensibly fixed by: ${original_pr_url}"

    debug "Counting commits in ${original_pr_url}"
    number=$(curl --silent https://api.github.com/repos/ceph/ceph/pulls/${original_pr}?access_token=${github_token} | jq .commits)
    if [ -z "$number" ] ; then
        error "Could not determine the number of commits in ${original_pr_url}"
        return 1
    fi
    info "Found $number commits in ${original_pr_url}"

    debug "Fetching latest commits from $upstream_remote"
    git fetch $upstream_remote

    debug "Initializing local branch $local_branch to $milestone"
    if git show-ref --verify --quiet refs/heads/$local_branch ; then
        error "Cannot initialize $local_branch - local branch already exists"
        false
    else
        git checkout $upstream_remote/$milestone -b $local_branch
    fi

    debug "Fetching latest commits from ${original_pr_url}"
    git fetch $upstream_remote pull/$original_pr/head:pr-$original_pr

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
            if [ "$i" -gt "0" ] >/dev/null 2>&1 ; then
                info "Then, cherry-pick the remaining commits from ${original_pr_url}, i.e.:"
                for ((j=$i-1; j>=0; j--)) ; do
                    info "-> missing commit: $(git log --oneline --max-count=1 --no-decorate pr-$original_pr~$j)"
                done
                info "Finally, re-run the script"
            else
                info "Then re-run the script"
            fi
            false
        fi
    done
    info "Cherry picking completed without conflicts"
}

function debug {
    log debug "$@"
}

function deduce_remote {
    local remote_type="$1"
    local remote=""
    local url_component=""
    if [ "$remote_type" = "upstream" ] ; then
        url_component="ceph"
    elif [ "$remote_type" = "fork" ] ; then
        url_component="$github_user"
    else
        error "Internal error in deduce_remote"
        exit 1
    fi
    remote=$(git remote -v | egrep --ignore-case '(://|@)github.com(/|:)'$url_component'/ceph(\s|\.|\/)' | head -n1 | cut -f 1)
    if [ "$remote" ] ; then
        true
    else
        error "Cannot auto-determine ${remote_type}_remote"
        info "Please set this variable explicitly and also file a bug report at ${redmine_endpoint}"
        exit 1
    fi
    echo "$remote"
}

function eol {
    log mtt=$1
    error "$mtt is EOL"
    false
}

function error {
    log error "$@"
}

function failed_mandatory_var_check {
    local varname="$1"
    error "$varname not defined"
    setup_ok=""
}

function info {
    log info "$@"
}

function init_github_user {
    debug "Initializing mandatory variables - GitHub user"
    if [ "$github_user" ] ; then
        true
    else
        warning "github_user variable not set, falling back to \$USER"
        github_user="$USER"
        if [ "$github_user" ] ; then
            true
        else
            failed_mandatory_var_check github_user
            info "$how_to_get_setup_advice"
            exit 1
        fi
    fi
}

function init_mandatory_vars {
    debug "Initializing mandatory variables - endpoints"
    redmine_endpoint="${redmine_endpoint:-"https://tracker.ceph.com"}"
    github_endpoint="${github_endpoint:-"https://github.com/ceph/ceph"}"
    debug "Initializing mandatory variables - GitHub remotes"
    upstream_remote="${upstream_remote:-$(deduce_remote upstream)}"
    fork_remote="${fork_remote:-$(deduce_remote fork)}"
}

function setup_summary {
    local not_set="!!! NOT SET !!!"
    local redmine_user_id_display="$not_set"
    local redmine_key_display="$not_set"
    local github_user_display="$not_set"
    local github_token_display="$not_set"
    local upstream_remote_display="$not_set"
    local fork_remote_display="$not_set"
    [ "$redmine_user_id" ] && redmine_user_id_display="$redmine_user_id"
    [ "$redmine_key" ] && redmine_key_display="(OK, not shown)"
    [ "$github_user" ] && github_user_display="$github_user"
    [ "$github_token" ] && github_token_display="(OK, not shown)"
    [ "$upstream_remote" ] && upstream_remote_display="$upstream_remote"
    [ "$fork_remote" ] && fork_remote_display="$fork_remote"
    debug Re-checking mandatory variables
    test "$redmine_key"      || failed_mandatory_var_check redmine_key
    test "$redmine_user_id"  || failed_mandatory_var_check redmine_user_id
    test "$github_user"      || failed_mandatory_var_check github_user
    test "$github_token"     || failed_mandatory_var_check github_token
    test "$upstream_remote"  || failed_mandatory_var_check upstream_remote
    test "$fork_remote"      || failed_mandatory_var_check fork_remote
    test "$redmine_endpoint" || failed_mandatory_var_check redmine_endpoint
    test "$github_endpoint"  || failed_mandatory_var_check github_endpoint
    if [ "$SETUP_ONLY" ] ; then
        read -r -d '' setup_summary <<EOM || true > /dev/null 2>&1
redmine_user_id  $redmine_user_id_display
redmine_key      $redmine_key_display
github_user      $github_user_display
github_token     $github_token_display
upstream_remote  $upstream_remote_display
fork_remote      $fork_remote_display
EOM
        log bare "================================"
        log bare "Setup summary"
        log bare "--------------------------------"
        log bare "variable name    value"
        log bare "--------------------------------"
        log bare "$setup_summary"
        log bare "================================"
    elif [ "$VERBOSE" ] ; then
        debug "redmine_user_id  $redmine_user_id_display"
        debug "redmine_key      $redmine_key_display"
        debug "github_user      $github_user_display"
        debug "github_token     $github_token_display"
        debug "upstream_remote  $upstream_remote_display"
        debug "fork_remote      $fork_remote_display"
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
    if [ "$verbose_only" -a -z "$VERBOSE" ] ; then
        true
    else
        msg="${prefix}${msg}"
        echo "$msg" >&2
    fi
}

function milestone_number_from_remote_api {
    local mtt=$1  # milestone to try
    local mn=""   # milestone number
    warning "Milestone ->$mtt<- unknown to script - falling back to GitHub API"
    remote_api_output=$(curl -s -X GET 'https://api.github.com/repos/ceph/ceph/milestones?access_token='$github_token)
    mn=$(echo $remote_api_output | jq --arg milestone $mtt '.[] | select(.title==$milestone) | .number')
    if [ "$mn" -gt "0" ] >/dev/null 2>&1 ; then
        echo "$mn"
    else
        error "Could not determine milestone number of ->$milestone<-"
        if [ "$VERBOSE" ] ; then
            info "GitHub API said:"
            log bare "$remote_api_output"
        fi
        info "Valid values are $(curl -s -X GET 'https://api.github.com/repos/ceph/ceph/milestones?access_token='$github_token | jq '.[].title')"
        info "(This probably means the Release field of ${redmine_url} is populated with"
        info "an unexpected value - i.e. it does not match any of the GitHub milestones.)"
        false
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

function setup_advice {
    cat <<EOM
Setup advice
------------

${this_script} expects to be run inside a local clone of the Ceph git repo.
Some initial setup is required for the script to become fully functional.

First, obtain the correct values for the following variables:

redmine_key     # "My account" -> "API access key" -> "Show"
redmine_user_id # "Logged in as foobar", click on foobar link, Redmine User ID
                # is in the URL, i.e. https://tracker.ceph.com/users/[redmine_user_id]
github_token    # https://github.com/settings/tokens -> Generate new token ->
                # ensure it has "Full control of private repositories" scope
github_user     # Your github username

The above variables must be set explicitly, as the script has no way of
determining reasonable defaults. If you prefer, you can ensure the variables
are set in the environment before running the script. Alternatively, you can
create a file, \$HOME/bin/backport_common.sh (this exact path), with the
variable assignments in it. The script will detect that the file exists and
"source" it.

In any case, care should be taken to keep the values of redmine_key and
github_token secret.

The script expects to run in a local clone of a Ceph repo with
at least two remotes defined - pointing to:

    https://github.com/ceph/ceph.git
    https://github.com/\$github_user/ceph.git

In other words, the upstream GitHub repo and the user's fork thereof. It makes
no difference what these remotes are called - the script will determine the
right remote names automatically.

To find out whether you have any obvious problems with your setup before
actually using the script to stage a backport, run:

    ${this_script} --setup

EOM
}

function troubleshooting_advice {
    cat <<EOM
Troubleshooting notes
---------------------

If the script inexplicably fails with:

    error: a cherry-pick or revert is already in progress
    hint: try "git cherry-pick (--continue | --quit | --abort)"
    fatal: cherry-pick failed

This is because HEAD is not where git expects it to be:

    $ git cherry-pick --abort
    warning: You seem to have moved HEAD. Not rewinding, check your HEAD!

This can be fixed by issuing the command:

    $ git cherry-pick --quit

EOM
}

# to update known milestones, consult:
#   curl --verbose -X GET https://api.github.com/repos/ceph/ceph/milestones
function try_known_milestones {
    local mtt=$1  # milestone to try
    local mn=""   # milestone number
    case $mtt in
        cuttlefish) eol "$mtt" ;;
        dumpling) eol "$mtt" ;;
        emperor) eol "$mtt" ;;
        firefly) eol "$mtt" ;;
        giant) eol "$mtt" ;;
        hammer) eol "$mtt" ;;
        infernalis) eol "$mtt" ;;
        jewel) mn="8" ;;
        kraken) eol "$mtt" ;;
        luminous) mn="10" ;;
        mimic) mn="11" ;;
        nautilus) mn="12" ;;
    esac
    echo "$mn"
}

function usage {
    cat <<EOM >&2
Documentation:

   ${this_script} --setup-advice | less
   ${this_script} --usage-advice | less
   ${this_script} --troubleshooting-advice | less

Usage:
   ${this_script} [OPTIONS] BACKPORT_TRACKER_ISSUE_NUMBER

Options:
    -c/--component COMPONENT (for use with --set-milestone)
    --debug            (turns on "set -x")
    -m/--set-milestone (requires elevated GitHub privs)
    -s/--setup         (just check the setup)
    -v/--verbose

Example:
   ${this_script} -c dashboard -m 31459
   (if cherry-pick conflicts are present, finish cherry-picking phase manually
   and run the script again with the same arguments)

CAVEAT: The script must be run from inside a local git clone.
EOM
}

function usage_advice {
    cat <<EOM
Usage advice
------------

Once you have completed setup (see --setup-advice), you can run the script
with the ID of a Backport tracker issue. For example, to stage the backport
https://tracker.ceph.com/issues/41502, run:

    ${this_script} 41502

If the commits in the corresponding master PR cherry-pick cleanly, the script
will automatically perform all steps required to stage the backport:

Cherry-pick phase:

1. fetching the latest commits from the upstream remote
2. creating a wip branch for the backport
3. figuring out which upstream PR contains the commits to cherry-pick
4. cherry-picking the commits

PR phase:

5. pushing the wip branch to your fork
6. opening the backport PR with compliant title and description describing
   the backport
7. (optionally) setting the milestone and label in the PR
8. updating the Backport tracker issue

If any of the commits do not cherry-pick cleanly, the script will abort in
step 4. In this case, you can either finish the cherry-picking manually
or abort the cherry-pick. In any case, when and if the local wip branch is
ready (all commits cherry-picked), if you run the script again, like so:

    ${this_script} 41502

the script will detect that the wip branch already exists and skip over
steps 1-4, starting from step 5 ("PR phase"). In other words, if the wip branch
already exists for any reason, the script will assume that the cherry-pick
phase (steps 1-4) is complete.

As this implies, you can do steps 1-4 manually. Provided the wip branch name
is in the format wip-\$TRACKER_ID-\$STABLE_RELEASE (e.g. "wip-41502-mimic"),
the script will detect the wip branch and start from step 5.

For details on all the options the script takes, run:

    ${this_script} --help

For more information on Ceph backporting, see:

    https://github.com/ceph/ceph/tree/master/SubmittingPatches-backports.rst

EOM
}

function warning {
    log warning "$@"
}


#
# are we in a local git clone?
#

if git status >/dev/null 2>&1 ; then
    debug "In a local git clone. Good."
else
    error "This script must be run from inside a local git clone"
    false
fi


#
# process command-line arguments
#

munged_options=$(getopt -o c:dhmpsv --long "component:,debug,help,prepare,set-milestone,setup,setup-advice,troubleshooting-advice,usage-advice,verbose" -n "$this_script" -- "$@")
eval set -- "$munged_options"

ADVICE=""
DEBUG=""
SET_MILESTONE=""
EXPLICIT_COMPONENT=""
EXPLICIT_PREPARE=""
HELP=""
ISSUE=""
SETUP_ADVICE=""
SETUP_ONLY=""
TROUBLESHOOTING_ADVICE=""
USAGE_ADVICE=""
VERBOSE=""
while true ; do
    case "$1" in
        --component|-c) shift ; EXPLICIT_COMPONENT="$1" ; shift ;;
        --debug|-d) DEBUG="$1" ; shift ;;
        --help|-h) ADVICE="1" ; HELP="$1" ; shift ;;
        --prepare|-p) EXPLICIT_PREPARE="$1" ; shift ;;
        --set-milestone|-m) SET_MILESTONE="$1" ; shift ;;
        --setup|-s) SETUP_ONLY="$1" ; shift ;;
        --setup-advice) ADVICE="1" ; SETUP_ADVICE="$1" ; shift ;;
        --troubleshooting-advice) ADVICE="$1" ; TROUBLESHOOTING_ADVICE="$1" ; shift ;;
        --usage-advice) ADVICE="$1" ; USAGE_ADVICE="$1" ; shift ;;
        --verbose|-v) VERBOSE="$1" ; shift ;;
        --) shift ; ISSUE="$1" ; break ;;
        *) echo "Internal error" ; false ;;
    esac
done

if [ "$ADVICE" ] ; then
    [ "$HELP" ] && usage
    [ "$SETUP_ADVICE" ] && setup_advice
    [ "$USAGE_ADVICE" ] && usage_advice
    [ "$TROUBLESHOOTING_ADVICE" ] && troubleshooting_advice
    exit 0
fi

[ "$SETUP_ONLY" ] && ISSUE="0"
if [[ $ISSUE =~ ^[0-9]+$ ]] ; then
    issue=$ISSUE
else
    error "Invalid or missing argument"
    usage
    false
fi

if [ "$DEBUG" ]; then
    set -x
    VERBOSE="--verbose"
fi

if [ "$VERBOSE" ]; then
    info "Verbose mode ON"
    VERBOSE="--verbose"
fi


#
# initialize mandatory variables and check values for sanity
#

BACKPORT_COMMON=$HOME/bin/backport_common.sh
[ -f "$BACKPORT_COMMON" ] && source $HOME/bin/backport_common.sh
setup_ok="1"
init_github_user
init_mandatory_vars
setup_summary
if [ "$setup_ok" ] ; then
    if [ "$SETUP_ONLY" ] ; then
        log bare "Overall setup is OK"
        exit 0
    elif [ "$VERBOSE" ] ; then
        debug "Overall setup is OK"
    fi
else
    if [ "$SETUP_ONLY" ] ; then
        log bare "Setup is NOT OK"
        log bare "$how_to_get_setup_advice"
        false
    else
        error "Problem detected in your setup"
        info "Run the script with --setup for a full report"
        info "$how_to_get_setup_advice"
        false
    fi
fi


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
    false
fi

debug "Looking up release/milestone of $redmine_url"
milestone=$(echo $remote_api_output | jq -r '.issue.custom_fields[0].value')
if [ "$milestone" ] ; then
    debug "Release/milestone: $milestone"
else
    error "could not obtain release/milestone from ${redmine_url}"
    false
fi

tracker_title=$(echo $remote_api_output | jq -r '.issue.subject')
debug "Title of $redmine_url is ->$tracker_title<-"

tracker_assignee_id=$(echo $remote_api_output | jq -r '.issue.assigned_to.id')
tracker_assignee_name=$(echo $remote_api_output | jq -r '.issue.assigned_to.name')
debug "$redmine_url is assigned to $tracker_assignee_name (ID $tracker_assignee_id)"

if [ "$tracker_assignee_id" = "null" -o "$tracker_assignee_id" = "$redmine_user_id" ] ; then
    true
else
    error "$redmine_url is assigned to $tracker_assignee_name (ID $tracker_assignee_id)"
    info "Cowardly refusing to work on an issue that is assigned to someone else"
    false
fi

milestone_number=$(try_known_milestones "$milestone")
if [ "$milestone_number" -gt "0" ] >/dev/null 2>&1 ; then
    target_branch="$milestone"
else
    milestone_number=$(milestone_number_from_remote_api "$milestone")
fi
info "Milestone/release is $milestone"
debug "Milestone number is $milestone_number"


#
# cherry-pick phase
#

local_branch=wip-${issue}-${target_branch}
if git show-ref --verify --quiet refs/heads/$local_branch ; then
    if [ "$EXPLICIT_PREPARE" ] ; then
        error "local branch $local_branch already exists -- cannot --prepare"
        false
    fi
    info "local branch $local_branch already exists: skipping cherry-pick phase"
else
    info "$local_branch does not exist: will create it and attempt automated cherry-pick"
    cherry_pick_phase
fi


#
# PR phase
#

current_branch=$(git rev-parse --abbrev-ref HEAD)
[ "$current_branch" = "$local_branch" ] || git checkout $local_branch

debug "Pushing local branch $local_branch to remote $fork_remote"
git push -u $fork_remote $local_branch

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
    log bare "$remote_api_output"
    false
fi

if [ "$SET_MILESTONE" -o "$CEPH_BACKPORT_SET_MILESTONE" ] ; then
    if [ "$EXPLICIT_COMPONENT" ] ; then
        component="$EXPLICIT_COMPONENT"
    else
        component=${COMPONENT:-core}
    fi
    info "Opened backport PR ${github_endpoint}/pull/$number"
    debug "Setting ${component} label and ${milestone} milestone"
    curl --silent --data-binary '{"milestone":'$milestone_number',"labels":["'$component'"]}' 'https://api.github.com/repos/ceph/ceph/issues/'$number'?access_token='$github_token >/dev/null 2>&1
    info "Set component label and milestone in PR"
else
    debug "Not setting component and milestone in PR (--set-milestone was not given and CEPH_BACKPORT_SET_MILESTONE not set)"
fi

pgrep firefox >/dev/null && firefox ${github_endpoint}/pull/$number

debug "Updating backport tracker issue in Redmine"
redmine_status=2 # In Progress
curl -X PUT --header 'Content-type: application/json' --data-binary '{"issue":{"description":"https://github.com/ceph/ceph/pull/'$number'","status_id":'$redmine_status',"assigned_to_id":'$redmine_user_id'},"notes":"Updated automatically by ceph-backport.sh"}' ${redmine_url}'.json?key='$redmine_key
info "${redmine_url} updated"
pgrep firefox >/dev/null && firefox ${redmine_url}
