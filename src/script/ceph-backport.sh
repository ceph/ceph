#!/usr/bin/env bash 
set -e
#
# ceph-backport.sh - Ceph backporting script
#
# Credits: This script is based on work done by Loic Dachary
#
#
# This script automates the process of staging a backport starting from a
# Backport tracker issue.
#
# Setup:
#
#     ceph-backport.sh --setup
#
# Usage and troubleshooting:
#
#     ceph-backport.sh --help
#     ceph-backport.sh --usage | less
#     ceph-backport.sh --troubleshooting | less
#

full_path="$0"

SCRIPT_VERSION="16.0.0.6848"
active_milestones=""
backport_pr_labels=""
backport_pr_number=""
backport_pr_title=""
backport_pr_url=""
deprecated_backport_common="$HOME/bin/backport_common.sh"
existing_pr_milestone_number=""
github_token=""
github_token_file="$HOME/.github_token"
github_user=""
milestone=""
non_interactive=""
original_issue=""
original_issue_url=""
original_pr=""
original_pr_url=""
redmine_key=""
redmine_key_file="$HOME/.redmine_key"
redmine_login=""
redmine_user_id=""
setup_ok=""
this_script=$(basename "$full_path")
gh_pr_template="../../../ceph/.github/pull_request_template.md"

if [[ $* == *--debug* ]]; then
    set -x
fi

# associative array keyed on "component" strings from PR titles, mapping them to
# GitHub PR labels that make sense in backports
declare -A comp_hash=(
["auth"]="core"
["bluestore"]="bluestore"
["build/ops"]="build/ops"
["ceph.spec"]="build/ops"
["ceph-volume"]="ceph-volume"
["cephadm"]="cephadm"
["cephfs"]="cephfs"
["cmake"]="build/ops"
["config"]="config"
["client"]="cephfs"
["common"]="common"
["core"]="core"
["dashboard"]="dashboard"
["deb"]="build/ops"
["doc"]="documentation"
["grafana"]="monitoring"
["mds"]="cephfs"
["messenger"]="core"
["mon"]="core"
["msg"]="core"
["mgr/cephadm"]="cephadm"
["mgr/dashboard"]="dashboard"
["mgr/prometheus"]="monitoring"
["mgr"]="core"
["monitoring"]="monitoring"
["orch"]="orchestrator"
["osd"]="core"
["perf"]="performance"
["prometheus"]="monitoring"
["pybind"]="pybind"
["py3"]="python3"
["python3"]="python3"
["qa"]="tests"
["rbd"]="rbd"
["rgw"]="rgw"
["rpm"]="build/ops"
["tests"]="tests"
["tool"]="tools"
)

declare -A flagged_pr_hash=()

function run {
    printf '%s\n' "$*" >&2
    "$@"
}

function abort_due_to_setup_problem {
    error "problem detected in your setup"
    info "Run \"${this_script} --setup\" to fix"
    false
}

function assert_fail {
    local message="$1"
    error "(internal error) $message"
    info "This could be reported as a bug!"
    false
}

function backport_pr_needs_label {
    local check_label="$1"
    local label
    local needs_label="yes"
    while read -r label ; do
        if [ "$label" = "$check_label" ] ; then
            needs_label=""
        fi
    done <<< "$backport_pr_labels"
    echo "$needs_label"
}

function backport_pr_needs_milestone {
    if [ "$existing_pr_milestone_number" ] ; then
        echo ""
    else
        echo "yes"
    fi
}

function bail_out_github_api {
    local api_said="$1"
    local hint="$2"
    info "GitHub API said:"
    log bare "$api_said"
    if [ "$hint" ] ; then
        info "(hint) $hint"
    fi
    abort_due_to_setup_problem
}

function blindly_set_pr_metadata {
    local pr_number="$1"
    local json_blob="$2"
    curl -u ${github_user}:${github_token} --silent --data-binary "$json_blob" "https://api.github.com/repos/ceph/ceph/issues/${pr_number}" >/dev/null 2>&1 || true
}

function check_milestones {
    local milestones_to_check
    milestones_to_check="$(echo "$1" | tr '\n' ' ' | xargs)"
    info "Active milestones: $milestones_to_check"
    for m in $milestones_to_check ; do
        info "Examining all PRs targeting base branch \"$m\""
        vet_prs_for_milestone "$m"
    done
    dump_flagged_prs
}

function check_tracker_status {
    local -a ok_statuses=("new" "need more info")
    local ts="$1"
    local error_msg
    local tslc="${ts,,}"
    local tslc_is_ok=
    for oks in "${ok_statuses[@]}"; do
        if [ "$tslc" = "$oks" ] ; then
            debug "Tracker status $ts is OK for backport to proceed"
            tslc_is_ok="yes"
            break
        fi
    done
    if [ "$tslc_is_ok" ] ; then
        true
    else
        if [ "$tslc" = "in progress" ] ; then
            error_msg="backport $redmine_url is already in progress"
        else
            error_msg="backport $redmine_url is closed (status: ${ts})"
        fi
        if [ "$FORCE" ] || [ "$EXISTING_PR" ] ; then
            warning "$error_msg"
        else
            error "$error_msg"
        fi
    fi
    echo "$tslc_is_ok"
}

function cherry_pick_phase {
    local base_branch
    local default_val
    local i
    local merged
    local number_of_commits
    local offset
    local sha1_to_cherry_pick
    local singular_or_plural_commit
    local yes_or_no_answer
    populate_original_issue
    if [ -z "$original_issue" ] ; then
        error "Could not find original issue"
        info "Does ${redmine_url} have a \"Copied from\" relation?"
        false
    fi
    info "Parent issue: ${original_issue_url}"

    populate_original_pr
    if [ -z "$original_pr" ]; then
        error "Could not find original PR"
        info "Is the \"Pull request ID\" field of ${original_issue_url} populated?"
        false
    fi
    info "Parent issue ostensibly fixed by: ${original_pr_url}"

    verbose "Examining ${original_pr_url}"
    remote_api_output=$(curl -u ${github_user}:${github_token} --silent "https://api.github.com/repos/ceph/ceph/pulls/${original_pr}")
    base_branch=$(echo "${remote_api_output}" | jq -r '.base.label')
    if [ "$base_branch" = "ceph:master" -o "$base_branch" = "ceph:main" ] ; then
        true
    else
        if [ "$FORCE" ] ; then
            warning "base_branch ->$base_branch<- is something other than \"ceph:master\" or \"ceph:main\""
            info "--force was given, so continuing anyway"
        else
            error "${original_pr_url} is targeting ${base_branch}: cowardly refusing to perform automated cherry-pick"
            info "Out of an abundance of caution, the script only automates cherry-picking of commits from PRs targeting \"ceph:master\" or \"ceph:main\"."
            info "You can still use the script to stage the backport, though. Just prepare the local branch \"${local_branch}\" manually and re-run the script."
            false
        fi
    fi
    base_sha="$(printf '%s' "${remote_api_output}" | jq -r .base.sha)"
    head_sha="$(printf '%s' "${remote_api_output}" | jq -r .head.sha)"
    merged=$(echo "${remote_api_output}" | jq -r '.merged')
    if [ "$merged" = "true" ] ; then
        # Use the merge commit in case the branch HEAD changes after merge.
        merge_commit_sha=$(printf '%s' "$remote_api_output" | jq -r '.merge_commit_sha')
        if [ -z "$merge_commit_sha" ]; then
            error "Could not determine the merge commit of ${original_pr_url}"
            bail_out_github_api "$remote_api_output"
            false
        fi
        cherry_pick_sha="${merge_commit_sha}^..${merge_commit_sha}^2"
    else
        if [ "$FORCE" ] ; then
            warning "${original_pr_url} is not merged yet"
            info "--force was given, so continuing anyway"
            cherry_pick_sha="${base_sha}..${head_sha}"
        else
            error "${original_pr_url} is not merged yet"
            info "Cowardly refusing to perform automated cherry-pick"
            false
        fi
    fi

    set -x
    git fetch "$upstream_remote"

    if git show-ref --verify --quiet "refs/heads/$local_branch" ; then
        if [ "$FORCE" ] ; then
            if [ "$non_interactive" ] ; then
                git checkout "$local_branch"
                git reset --hard "${upstream_remote}/${milestone}"
            else
                echo
                echo "A local branch $local_branch already exists and the --force option was given."
                echo "If you continue, any local changes in $local_branch will be lost!"
                echo
                default_val="y"
                echo -n "Do you really want to overwrite ${local_branch}? (default: ${default_val}) "
                yes_or_no_answer="$(get_user_input "$default_val")"
                [ "$yes_or_no_answer" ] && yes_or_no_answer="${yes_or_no_answer:0:1}"
                if [ "$yes_or_no_answer" = "y" ] ; then
                    git checkout "$local_branch"
                    git reset --hard "${upstream_remote}/${milestone}"
                else
                    info "OK, bailing out!"
                    false
                fi
            fi
        else
            set +x
            maybe_restore_set_x
            error "Cannot initialize $local_branch - local branch already exists"
            false
        fi
    else
        git checkout "${upstream_remote}/${milestone}" -b "$local_branch"
    fi

    git fetch "$upstream_remote" "pull/$original_pr/head:pr-$original_pr"

    set +x
    maybe_restore_set_x
    info "Attempting to cherry pick ${original_pr_url} into local branch $local_branch"
    if ! run git cherry-pick -x "$cherry_pick_sha"; then
        [ "$VERBOSE" ] && git status
        error "Cherry pick failed due to conflicts?"
        info "Manually fix conflicts and complete the current cherry-pick:"
        info "    git cherry-pick --continue"
        info "Finally, re-run this script"
        false
    fi

    info "Cherry picking completed without conflicts"
}

function clear_line {
    log overwrite "                                                                             \r"
}

function clip_pr_body {
    local pr_body="$*"
    local clipped=""
    local last_line_was_blank=""
    local line=""
    local pr_json_tempfile=$(mktemp)
    echo "$pr_body" | sed -n '/<!--.*/q;p' > "$pr_json_tempfile"
    while IFS= read -r line; do
        if [ "$(trim_whitespace "$line")" ] ; then
            last_line_was_blank=""
            clipped="${clipped}${line}\n"
        else
            if [ "$last_line_was_blank" ] ; then
                true
            else
                clipped="${clipped}\n"
            fi
        fi
    done < "$pr_json_tempfile"
    rm "$pr_json_tempfile"
    echo "$clipped"
}

function debug {
    log debug "$@"
}

function display_version_message_and_exit {
    echo "$this_script: Ceph backporting script, version $SCRIPT_VERSION"
    exit 0 
}

function dump_flagged_prs {
    local url=
    clear_line
    if [ "${#flagged_pr_hash[@]}" -eq "0" ] ; then
        info "All backport PRs appear to have milestone set correctly"
    else
        warning "Some backport PRs had problematic milestone settings"
        log bare "==========="
        log bare "Flagged PRs"
        log bare "-----------"
        for url in "${!flagged_pr_hash[@]}" ; do
            log bare "$url - ${flagged_pr_hash[$url]}"
        done
        log bare "==========="
    fi
}

function eol {
    local mtt="$1"
    error "$mtt is EOL"
    false
}

function error {
    log error "$@"
}

function existing_pr_routine {
    local base_branch
    local clipped_pr_body
    local new_pr_body
    local new_pr_title
    local pr_body
    local pr_json_tempfile
    local remote_api_output
    local update_pr_body
    remote_api_output="$(curl -u ${github_user}:${github_token} --silent "https://api.github.com/repos/ceph/ceph/pulls/${backport_pr_number}")"
    backport_pr_title="$(echo "$remote_api_output" | jq -r '.title')"
    if [ "$backport_pr_title" = "null" ] ; then
        error "could not get PR title of existing PR ${backport_pr_number}"
        bail_out_github_api "$remote_api_output"
    fi
    existing_pr_milestone_number="$(echo "$remote_api_output" | jq -r '.milestone.number')"
    if [ "$existing_pr_milestone_number" = "null" ] ; then
        existing_pr_milestone_number=""
    fi
    backport_pr_labels="$(echo "$remote_api_output" | jq -r '.labels[].name')"
    pr_body="$(echo "$remote_api_output" | jq -r '.body')"
    if [ "$pr_body" = "null" ] ; then
        error "could not get PR body of existing PR ${backport_pr_number}"
        bail_out_github_api "$remote_api_output"
    fi
    base_branch=$(echo "${remote_api_output}" | jq -r '.base.label')
    base_branch="${base_branch#ceph:}"
    if [ -z "$(is_active_milestone "$base_branch")" ] ; then
        error "existing PR $backport_pr_url is targeting $base_branch which is not an active milestone"
        info "Cowardly refusing to work on a backport to $base_branch"
        false
    fi
    clipped_pr_body="$(clip_pr_body "$pr_body")"
    verbose_en "Clipped body of existing PR ${backport_pr_number}:\n${clipped_pr_body}"
    if [[ "$backport_pr_title" =~ ^${milestone}: ]] ; then
        verbose "Existing backport PR ${backport_pr_number} title has ${milestone} prepended"
    else
        warning "Existing backport PR ${backport_pr_number} title does NOT have ${milestone} prepended"
        new_pr_title="${milestone}: $backport_pr_title"
        if [[ "$new_pr_title" =~ \" ]] ; then
            new_pr_title="${new_pr_title//\"/\\\"}"
        fi
        verbose "New PR title: ${new_pr_title}"
    fi
    redmine_url_without_scheme="${redmine_url//http?:\/\//}"
    verbose "Redmine URL without scheme: $redmine_url_without_scheme"
    if [[ "$clipped_pr_body" =~ $redmine_url_without_scheme ]] ; then
        info "Existing backport PR ${backport_pr_number} already mentions $redmine_url"
        if [ "$FORCE" ] ; then
            warning "--force was given, so updating the PR body anyway"
            update_pr_body="yes"
        fi
    else
        warning "Existing backport PR ${backport_pr_number} does NOT mention $redmine_url - adding it"
        update_pr_body="yes"
    fi
    if [ "$update_pr_body" ] ; then
        new_pr_body="backport tracker: ${redmine_url}"
        if [ "${original_pr_url}" ] ; then
            new_pr_body="${new_pr_body}
possibly a backport of ${original_pr_url}"
        fi
        if [ "${original_issue_url}" ] ; then
            new_pr_body="${new_pr_body}
parent tracker: ${original_issue_url}"
        fi
        new_pr_body="${new_pr_body}

---

original PR body:

$clipped_pr_body

---

updated using ceph-backport.sh version ${SCRIPT_VERSION}"
    fi
    maybe_update_pr_title_body "${new_pr_title}" "${new_pr_body}"
}

function failed_mandatory_var_check {
    local varname="$1"
    local error="$2"
    verbose "$varname $error"
    setup_ok=""
}

function flag_pr {
    local pr_num="$1"
    local pr_url="$2"
    local flag_reason="$3"
    warning "flagging PR#${pr_num} because $flag_reason"
    flagged_pr_hash["${pr_url}"]="$flag_reason"
}

function from_file {
    local what="$1"
    xargs 2>/dev/null < "$HOME/.${what}" || true
}

function get_user_input {
    local default_val="$1"
    local user_input=
    read -r user_input
    if [ "$user_input" ] ; then
        echo "$user_input"
    else
        echo "$default_val"
    fi
}

# takes a string and a substring - returns position of substring within string,
# or -1 if not found
# NOTE: position of first character in string is 0
function grep_for_substr {
    local str="$1"
    local look_for_in_str="$2"
    str="${str,,}"
    munged="${str%%${look_for_in_str}*}"
    if [ "$munged" = "$str" ] ; then
        echo "-1"
    else
        echo "${#munged}"
    fi
}

# takes PR title, attempts to guess component
function guess_component {
    local comp=
    local pos="0"
    local pr_title="$1"
    local winning_comp=
    local winning_comp_pos="9999"
    for comp in "${!comp_hash[@]}" ; do
        pos=$(grep_for_substr "$pr_title" "$comp")
        # echo "$comp: $pos"
        [ "$pos" = "-1" ] && continue
        if [ "$pos" -lt "$winning_comp_pos" ] ; then
             winning_comp_pos="$pos"
             winning_comp="$comp"
        fi
    done
    [ "$winning_comp" ] && echo "${comp_hash["$winning_comp"]}" || echo ""
}

function info {
    log info "$@"
}

function init_endpoints {
    verbose "Initializing remote API endpoints"
    redmine_endpoint="${redmine_endpoint:-"https://tracker.ceph.com"}"
    github_endpoint="${github_endpoint:-"https://github.com/ceph/ceph"}"
}

function init_fork_remote {
    [ "$github_user" ] || assert_fail "github_user not set"
    [ "$EXPLICIT_FORK" ] && info "Using explicit fork ->$EXPLICIT_FORK<- instead of personal fork."
    fork_remote="${fork_remote:-$(maybe_deduce_remote fork)}"
}

function init_github_token {
    github_token="$(from_file github_token)"
    if [ "$github_token" ] ; then
        true
    else
        warning "$github_token_file not populated: initiating interactive setup routine"
        INTERACTIVE_SETUP_ROUTINE="yes"
    fi
}

function init_redmine_key {
    redmine_key="$(from_file redmine_key)"
    if [ "$redmine_key" ] ; then
        true
    else
        warning "$redmine_key_file not populated: initiating interactive setup routine"
        INTERACTIVE_SETUP_ROUTINE="yes"
    fi
}

function init_upstream_remote {
    upstream_remote="${upstream_remote:-$(maybe_deduce_remote upstream)}"
}

function interactive_setup_routine {
    local default_val
    local original_github_token
    local original_redmine_key
    local total_steps
    local yes_or_no_answer
    original_github_token="$github_token"
    original_redmine_key="$redmine_key"
    total_steps="4"
    if [ -e "$deprecated_backport_common" ] ; then
        github_token=""
        redmine_key=""
        # shellcheck disable=SC1090
        source "$deprecated_backport_common" 2>/dev/null || true
        total_steps="$((total_steps+1))"
    fi
    echo
    echo "Welcome to the ${this_script} interactive setup routine!"
    echo
    echo "---------------------------------------------------------------------"
    echo "Setup step 1 of $total_steps - GitHub token"
    echo "---------------------------------------------------------------------"
    echo "For information on how to generate a GitHub personal access token"
    echo "to use with this script, go to https://github.com/settings/tokens"
    echo "then click on \"Generate new token\" and make sure the token has"
    echo "\"Full control of private repositories\" scope."
    echo
    echo "For more details, see:"
    echo "https://help.github.com/en/articles/creating-a-personal-access-token-for-the-command-line"
    echo
    echo -n "What is your GitHub token? "
    default_val="$github_token"
    [ "$github_token" ] && echo "(default: ${default_val})"
    github_token="$(get_user_input "$default_val")"
    if [ "$github_token" ] ; then
        true
    else
        error "You must provide a valid GitHub personal access token"
        abort_due_to_setup_problem
    fi
    [ "$github_token" ] || assert_fail "github_token not set, even after completing Step 1 of interactive setup"
    echo
    echo "---------------------------------------------------------------------"
    echo "Setup step 2 of $total_steps - GitHub user"
    echo "---------------------------------------------------------------------"
    echo "The script will now attempt to determine your GitHub user (login)"
    echo "from the GitHub token provided in the previous step. If this is"
    echo "successful, there is a good chance that your GitHub token is OK."
    echo
    echo "Communicating with the GitHub API..."
    set_github_user_from_github_token
    [ "$github_user" ] || abort_due_to_setup_problem
    echo
    echo -n "Is the GitHub username (login) \"$github_user\" correct? "
    default_val="y"
    [ "$github_token" ] && echo "(default: ${default_val})"
    yes_or_no_answer="$(get_user_input "$default_val")"
    [ "$yes_or_no_answer" ] && yes_or_no_answer="${yes_or_no_answer:0:1}"
    if [ "$yes_or_no_answer" = "y" ] ; then
        if [ "$github_token" = "$original_github_token" ] ; then
            true
        else
            debug "GitHub personal access token changed"
            echo "$github_token" > "$github_token_file"
            chmod 0600 "$github_token_file"
            info "Wrote GitHub personal access token to $github_token_file"
        fi
    else
        error "GitHub user does not look right"
        abort_due_to_setup_problem
    fi
    [ "$github_token" ] || assert_fail "github_token not set, even after completing Steps 1 and 2 of interactive setup"
    [ "$github_user" ] || assert_fail "github_user not set, even after completing Steps 1 and 2 of interactive setup"
    echo
    echo "---------------------------------------------------------------------"
    echo "Setup step 3 of $total_steps - remote repos"
    echo "---------------------------------------------------------------------"
    echo "Searching \"git remote -v\" for remote repos"
    echo
    init_upstream_remote
    init_fork_remote
    vet_remotes
    echo "Upstream remote is \"$upstream_remote\""
    echo "Fork remote is \"$fork_remote\""
    [ "$setup_ok" ] || abort_due_to_setup_problem
    [ "$github_token" ] || assert_fail "github_token not set, even after completing Steps 1-3 of interactive setup"
    [ "$github_user" ] || assert_fail "github_user not set, even after completing Steps 1-3 of interactive setup"
    [ "$upstream_remote" ] || assert_fail "upstream_remote not set, even after completing Steps 1-3 of interactive setup"
    [ "$fork_remote" ] || assert_fail "fork_remote not set, even after completing Steps 1-3 of interactive setup"
    echo
    echo "---------------------------------------------------------------------"
    echo "Setup step 4 of $total_steps - Redmine key"
    echo "---------------------------------------------------------------------"
    echo "To generate a Redmine API access key, go to https://tracker.ceph.com"
    echo "After signing in, click: \"My account\""
    echo "Now, find \"API access key\"."
    echo "Once you know the API access key, enter it below."
    echo
    echo -n "What is your Redmine key? "
    default_val="$redmine_key"
    [ "$redmine_key" ] && echo "(default: ${default_val})"
    redmine_key="$(get_user_input "$default_val")"
    if [ "$redmine_key" ] ; then
        set_redmine_user_from_redmine_key
        if [ "$setup_ok" ] ; then
            true
        else
            info "You must provide a valid Redmine API access key"
            abort_due_to_setup_problem
        fi
        if [ "$redmine_key" = "$original_redmine_key" ] ; then
            true
        else
            debug "Redmine API access key changed"
            echo "$redmine_key" > "$redmine_key_file"
            chmod 0600 "$redmine_key_file"
            info "Wrote Redmine API access key to $redmine_key_file"
        fi
    else
        error "You must provide a valid Redmine API access key"
        abort_due_to_setup_problem
    fi
    [ "$github_token" ] || assert_fail "github_token not set, even after completing Steps 1-4 of interactive setup"
    [ "$github_user" ] || assert_fail "github_user not set, even after completing Steps 1-4 of interactive setup"
    [ "$upstream_remote" ] || assert_fail "upstream_remote not set, even after completing Steps 1-4 of interactive setup"
    [ "$fork_remote" ] || assert_fail "fork_remote not set, even after completing Steps 1-4 of interactive setup"
    [ "$redmine_key" ] || assert_fail "redmine_key not set, even after completing Steps 1-4 of interactive setup"
    [ "$redmine_user_id" ] || assert_fail "redmine_user_id not set, even after completing Steps 1-4 of interactive setup"
    [ "$redmine_login" ] || assert_fail "redmine_login not set, even after completing Steps 1-4 of interactive setup"
    if [ "$total_steps" -gt "4" ] ; then
        echo
        echo "---------------------------------------------------------------------"
        echo "Step 5 of $total_steps - delete deprecated $deprecated_backport_common file"
        echo "---------------------------------------------------------------------"
    fi
    maybe_delete_deprecated_backport_common
    vet_setup --interactive
}

function is_active_milestone {
    local is_active=
    local milestone_under_test="$1"
    for m in $active_milestones ; do
        if [ "$milestone_under_test" = "$m" ] ; then
            verbose "Milestone $m is active"
            is_active="yes"
            break
        fi
    done
    echo "$is_active"
}

function log {
    local level="$1"
    local trailing_newline="yes"
    local in_hex=""
    shift
    local msg="$*"
    prefix="${this_script}: "
    verbose_only=
    case $level in
        bare)
            prefix=
            ;;
        debug)
            prefix="${prefix}DEBUG: "
            verbose_only="yes"
            ;;
        err*)
            prefix="${prefix}ERROR: "
            ;;
        hex)
            in_hex="yes"
            ;;
        info)
            :
            ;;
        overwrite)
            trailing_newline=
            prefix=
            ;;
        verbose)
            verbose_only="yes"
            ;;
        verbose_en)
            verbose_only="yes"
            trailing_newline=
            ;;
        warn|warning)
            prefix="${prefix}WARNING: "
            ;;
    esac
    if [ "$in_hex" ] ; then
        print_in_hex "$msg"
    elif [ "$verbose_only" ] && [ -z "$VERBOSE" ] ; then
        true
    else
        msg="${prefix}${msg}"
        if [ "$trailing_newline" ] ; then
            echo "${msg}" >&2
        else
            echo -en "${msg}" >&2
        fi
    fi
}

function maybe_deduce_remote {
    local remote_type="$1"
    local remote=""
    local url_component=""
    if [ "$remote_type" = "upstream" ] ; then
        url_component="ceph"
    elif [ "$remote_type" = "fork" ] ; then
        if [ "$EXPLICIT_FORK" ] ; then
            url_component="$EXPLICIT_FORK"
        else
            url_component="$github_user"
        fi
    else
        assert_fail "bad remote_type ->$remote_type<- in maybe_deduce_remote"
    fi
    remote=$(git remote -v | grep --extended-regexp --ignore-case '(://|@)github.com(/|:|:/)'${url_component}'/ceph(\s|\.|\/)' | head -n1 | cut -f 1)
    echo "$remote"
}

function maybe_delete_deprecated_backport_common {
    local default_val
    local user_inp
    if [ -e "$deprecated_backport_common" ] ; then
        echo "You still have a $deprecated_backport_common file,"
        echo "which was used to store configuration parameters in version"
        echo "15.0.0.6270 and earlier versions of ${this_script}."
        echo
        echo "Since $deprecated_backport_common has been deprecated in favor"
        echo "of the interactive setup routine, which has been completed"
        echo "successfully, the file should be deleted now."
        echo
        echo -n "Delete it now? (default: y) "
        default_val="y"
        user_inp="$(get_user_input "$default_val")"
        user_inp="$(echo "$user_inp" | tr '[:upper:]' '[:lower:]' | xargs)"
        if [ "$user_inp" ] ; then
            user_inp="${user_inp:0:1}"
            if [ "$user_inp" = "y" ] ; then
                set -x
                rm -f "$deprecated_backport_common"
                set +x
                maybe_restore_set_x
            fi
        fi
        if [ -e "$deprecated_backport_common" ] ; then
            error "$deprecated_backport_common still exists. Bailing out!"
            false
        fi
    fi
}

function maybe_restore_set_x {
    if [ "$DEBUG" ] ; then
        set -x
    fi
}

function maybe_update_pr_milestone_labels {
    local component
    local data_binary
    local data_binary
    local label
    local needs_milestone
    if [ "$EXPLICIT_COMPONENT" ] ; then
        debug "Component given on command line: using it"
        component="$EXPLICIT_COMPONENT"
    else
        debug "Attempting to guess component"
        component=$(guess_component "$backport_pr_title")
    fi
    data_binary="{"
    needs_milestone="$(backport_pr_needs_milestone)"
    if [ "$needs_milestone" ] ; then
        debug "Attempting to set ${milestone} milestone in ${backport_pr_url}"
        data_binary="${data_binary}\"milestone\":${milestone_number}"
    else
        info "Backport PR ${backport_pr_url} already has ${milestone} milestone"
    fi
    if [ "$(backport_pr_needs_label "$component")" ] ; then
        debug "Attempting to add ${component} label to ${backport_pr_url}"
        if [ "$needs_milestone" ] ; then
            data_binary="${data_binary},"
        fi
        data_binary="${data_binary}\"labels\":[\"${component}\""
        while read -r label ; do
            if [ "$label" ] ; then
                data_binary="${data_binary},\"${label}\""
            fi
        done <<< "$backport_pr_labels"
        data_binary="${data_binary}]}"
    else
        info "Backport PR ${backport_pr_url} already has label ${component}"
        data_binary="${data_binary}}"
    fi
    if [ "$data_binary" = "{}" ] ; then
        true
    else
        blindly_set_pr_metadata "$backport_pr_number" "$data_binary"
    fi
}

function maybe_update_pr_title_body {
    local new_title="$1"
    local new_body="$2"
    local data_binary
    if [ "$new_title" ] && [ "$new_body" ] ; then
        data_binary="{\"title\":\"${new_title}\", \"body\":\"$(munge_body "${new_body}")\"}"
    elif [ "$new_title" ] ; then
        data_binary="{\"title\":\"${new_title}\"}"
        backport_pr_title="${new_title}"
    elif [ "$new_body" ] ; then
        data_binary="{\"body\":\"$(munge_body "${new_body}")\"}"
        #log hex "${data_binary}"
        #echo -n "${data_binary}"
    fi
    if [ "$data_binary" ] ; then
        blindly_set_pr_metadata "${backport_pr_number}" "$data_binary"
    fi
}

function milestone_number_from_remote_api {
    local mtt="$1"  # milestone to try
    local mn=""     # milestone number
    local milestones
    remote_api_output=$(curl -u ${github_user}:${github_token} --silent -X GET "https://api.github.com/repos/ceph/ceph/milestones")
    mn=$(echo "$remote_api_output" | jq --arg milestone "$mtt" '.[] | select(.title==$milestone) | .number')
    if [ "$mn" -gt "0" ] >/dev/null 2>&1 ; then
        echo "$mn"
    else
        error "Could not determine milestone number of ->$milestone<-"
        verbose_en "GitHub API said:\n${remote_api_output}\n"
        remote_api_output=$(curl -u ${github_user}:${github_token} --silent -X GET "https://api.github.com/repos/ceph/ceph/milestones")
        milestones=$(echo "$remote_api_output" | jq '.[].title')
        info "Valid values are ${milestones}"
        info "(This probably means the Release field of ${redmine_url} is populated with"
        info "an unexpected value - i.e. it does not match any of the GitHub milestones.)"
        false
    fi
}

function munge_body {
    echo "$new_body" | tr '\r' '\n' | sed 's/$/\\n/' | tr -d '\n'
}

function number_to_url {
    local number_type="$1"
    local number="$2"
    if [ "$number_type" = "github" ] ; then
        echo "${github_endpoint}/pull/${number}"
    elif [ "$number_type" = "redmine" ] ; then
        echo "${redmine_endpoint}/issues/${number}"
    else
        assert_fail "internal error in number_to_url: bad type ->$number_type<-"
    fi
}

function populate_original_issue {
    if [ -z "$original_issue" ] ; then
        original_issue=$(curl --silent "${redmine_url}.json?include=relations" |
            jq '.issue.relations[] | select(.relation_type | contains("copied_to")) | .issue_id')
        original_issue_url="$(number_to_url "redmine" "${original_issue}")"
    fi
}

function populate_original_pr {
    if [ "$original_issue" ] ; then
        if [ -z "$original_pr" ] ; then
            original_pr=$(curl --silent "${original_issue_url}.json" |
                          jq -r '.issue.custom_fields[] | select(.id | contains(21)) | .value')
            original_pr_url="$(number_to_url "github" "${original_pr}")"
        fi
    fi
}

function print_in_hex {
    local str="$1"
    local c

    for (( i=0; i < ${#str}; i++ ))
    do
       c=${str:$i:1}
       if [[ $c == ' ' ]]
       then
          printf "[%s] 0x%X\n" " " \'\ \' >&2
       else
          printf "[%s] 0x%X\n" "$c" \'"$c"\' >&2
       fi
    done
}

function set_github_user_from_github_token {
    local quiet="$1"
    local api_error
    local curl_opts
    setup_ok=""
    [ "$github_token" ] || assert_fail "set_github_user_from_github_token: git_token not set"
    curl_opts="--silent -u :${github_token} https://api.github.com/user"
    [ "$quiet" ] || set -x
    remote_api_output="$(curl $curl_opts)"
    set +x
    github_user=$(echo "${remote_api_output}" | jq -r .login 2>/dev/null | grep -v null || true)
    api_error=$(echo "${remote_api_output}" | jq -r .message 2>/dev/null | grep -v null || true)
    if [ "$api_error" ] ; then
        info "GitHub API said: ->$api_error<-"
        info "If you can't figure out what's wrong by examining the curl command and its output, above,"
        info "please also study https://developer.github.com/v3/users/#get-the-authenticated-user"
        github_user=""
    else
        [ "$github_user" ] || assert_fail "set_github_user_from_github_token: failed to set github_user"
        info "my GitHub username is $github_user"
        setup_ok="yes"
    fi
}

function set_redmine_user_from_redmine_key {
    [ "$redmine_key" ] || assert_fail "set_redmine_user_from_redmine_key was called, but redmine_key not set"
    local api_key_from_api
    remote_api_output="$(curl --silent "https://tracker.ceph.com/users/current.json?key=$redmine_key")"
    redmine_login="$(echo "$remote_api_output" | jq -r '.user.login')"
    redmine_user_id="$(echo "$remote_api_output" | jq -r '.user.id')"
    api_key_from_api="$(echo "$remote_api_output" | jq -r '.user.api_key')"
    if [ "$redmine_login" ] && [ "$redmine_user_id" ] && [ "$api_key_from_api" = "$redmine_key" ] ; then
        [ "$redmine_user_id" ] || assert_fail "set_redmine_user_from_redmine_key: failed to set redmine_user_id"
        [ "$redmine_login" ] || assert_fail "set_redmine_user_from_redmine_key: failed to set redmine_login"
        info "my Redmine username is $redmine_login (ID $redmine_user_id)"
        setup_ok="yes"
    else
        error "Redmine API access key $redmine_key is invalid"
        redmine_login=""
        redmine_user_id=""
        setup_ok=""
    fi
}

function tracker_component_is_in_desired_state {
    local comp="$1"
    local val_is="$2"
    local val_should_be="$3"
    local in_desired_state
    if [ "$val_is" = "$val_should_be" ] ; then
        debug "Tracker $comp is in the desired state"
        in_desired_state="yes"
    fi
    echo "$in_desired_state"
}

function tracker_component_was_updated {
    local comp="$1"
    local val_old="$2"
    local val_new="$3"
    local was_updated
    if [ "$val_old" = "$val_new" ] ; then
        true
    else
        debug "Tracker $comp was updated!"
        was_updated="yes"
    fi
    echo "$was_updated"
}

function trim_whitespace {
    local var="$*"
    # remove leading whitespace characters
    var="${var#"${var%%[![:space:]]*}"}"
    # remove trailing whitespace characters
    var="${var%"${var##*[![:space:]]}"}"
    echo -n "$var"
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
        octopus) mn="13" ;;
        pacific) mn="14" ;;
        quincy) mn="15" ;;
        reef) mn="16" ;;
    esac
    echo "$mn"
}

function update_version_number_and_exit {
    set -x
    local raw_version
    local munge_first_hyphen
    # munge_first_hyphen will look like this: 15.0.0.5774-g4c2f2eda969
    local script_version_number
    raw_version="$(git describe --long --match 'v*' | sed 's/^v//')"  # example: "15.0.0-5774-g4c2f2eda969"
    munge_first_hyphen="${raw_version/-/.}"  # example: "15.0.0.5774-g4c2f2eda969"
    script_version_number="${munge_first_hyphen%-*}"  # example: "15.0.0.5774"
    sed -i -e "s/^SCRIPT_VERSION=.*/SCRIPT_VERSION=\"${script_version_number}\"/" "$full_path"
    exit 0
}

function usage {
    cat <<EOM >&2
Setup:

   ${this_script} --setup

Documentation:

   ${this_script} --help
   ${this_script} --usage | less
   ${this_script} --troubleshooting | less

Usage:
   ${this_script} BACKPORT_TRACKER_ISSUE_NUMBER

Options (not needed in normal operation):
    --cherry-pick-only    (stop after cherry-pick phase)
    --component/-c COMPONENT
                          (explicitly set the component label; if omitted, the
                           script will try to guess the component)
    --debug               (turns on "set -x")
    --existing-pr BACKPORT_PR_ID
                          (use this when the backport PR is already open)
    --force               (exercise caution!)
    --fork EXPLICIT_FORK  (use EXPLICIT_FORK instead of personal GitHub fork)
    --milestones          (vet all backport PRs for correct milestone setting)
    --setup/-s            (run the interactive setup routine - NOTE: this can 
                           be done any number of times)
    --setup-report        (check the setup and print a report)
    --update-version      (this option exists as a convenience for the script
                           maintainer only: not intended for day-to-day usage)
    --verbose/-v          (produce more output than normal)
    --version             (display version number and exit)

Example:
   ${this_script} 31459
   (if cherry-pick conflicts are present, finish cherry-picking phase manually
   and then run the script again with the same argument)

CAVEAT: The script must be run from inside a local git clone.
EOM
}

function usage_advice {
    cat <<EOM
Usage advice
------------

Once you have completed --setup, you can run the script with the ID of
a Backport tracker issue. For example, to stage the backport
https://tracker.ceph.com/issues/41502, run:

    ${this_script} 41502

Provided the commits in the corresponding main PR cherry-pick cleanly, the
script will automatically perform all steps required to stage the backport:

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

When run with --cherry-pick-only, the script will stop after the cherry-pick
phase.

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

    https://github.com/ceph/ceph/tree/main/SubmittingPatches-backports.rst

EOM
}

function verbose {
    log verbose "$@"
}

function verbose_en {
    log verbose_en "$@"
}

function vet_pr_milestone {
    local pr_number="$1"
    local pr_title="$2"
    local pr_url="$3"
    local milestone_stanza="$4"
    local milestone_title_should_be="$5"
    local milestone_number_should_be
    local milestone_number_is=
    local milestone_title_is=
    milestone_number_should_be="$(try_known_milestones "$milestone_title_should_be")"
    log overwrite "Vetting milestone of PR#${pr_number}\r"
    if [ "$milestone_stanza" = "null" ] ; then
        blindly_set_pr_metadata "$pr_number" "{\"milestone\": $milestone_number_should_be}"
        warning "$pr_url: set milestone to \"$milestone_title_should_be\""
        flag_pr "$pr_number" "$pr_url" "milestone not set"
    else
        milestone_title_is=$(echo "$milestone_stanza" | jq -r '.title')
        milestone_number_is=$(echo "$milestone_stanza" | jq -r '.number')
        if [ "$milestone_number_is" -eq "$milestone_number_should_be" ] ; then
            true
        else
            blindly_set_pr_metadata "$pr_number" "{\"milestone\": $milestone_number_should_be}"
            warning "$pr_url: changed milestone from \"$milestone_title_is\" to \"$milestone_title_should_be\""
            flag_pr "$pr_number" "$pr_url" "milestone set to wrong value \"$milestone_title_is\""
        fi
    fi
}

function vet_prs_for_milestone {
    local milestone_title="$1"
    local pages_of_output=
    local pr_number=
    local pr_title=
    local pr_url=
    # determine last page (i.e., total number of pages)
    remote_api_output="$(curl -u ${github_user}:${github_token} --silent --head "https://api.github.com/repos/ceph/ceph/pulls?base=${milestone_title}" | grep -E '^Link' || true)"
    if [ "$remote_api_output" ] ; then
         # Link: <https://api.github.com/repositories/2310495/pulls?base=luminous&page=2>; rel="next", <https://api.github.com/repositories/2310495/pulls?base=luminous&page=2>; rel="last"
         # shellcheck disable=SC2001
         pages_of_output="$(echo "$remote_api_output" | sed 's/^.*&page\=\([0-9]\+\)>; rel=\"last\".*$/\1/g')"
    else
         pages_of_output="1"
    fi
    verbose "GitHub has $pages_of_output pages of pull request data for \"base:${milestone_title}\""
    for ((page=1; page<=pages_of_output; page++)) ; do
        verbose "Fetching PRs (page $page of ${pages_of_output})"
        remote_api_output="$(curl -u ${github_user}:${github_token} --silent -X GET "https://api.github.com/repos/ceph/ceph/pulls?base=${milestone_title}&page=${page}")"
        prs_in_page="$(echo "$remote_api_output" | jq -r '. | length')"
        verbose "Page $page of remote API output contains information on $prs_in_page PRs"
        for ((i=0; i<prs_in_page; i++)) ; do
            pr_number="$(echo "$remote_api_output" | jq -r ".[${i}].number")"
            pr_title="$(echo "$remote_api_output" | jq -r ".[${i}].title")"
            pr_url="$(number_to_url "github" "${pr_number}")"
            milestone_stanza="$(echo "$remote_api_output" | jq -r ".[${i}].milestone")"
            vet_pr_milestone "$pr_number" "$pr_title" "$pr_url" "$milestone_stanza" "$milestone_title"
        done
        clear_line
    done
}

function vet_remotes {
    if [ "$upstream_remote" ] ; then
        verbose "Upstream remote is $upstream_remote"
    else
        error "Cannot auto-determine upstream remote"
        "(Could not find any upstream remote in \"git remote -v\")"
        false
    fi
    if [ "$fork_remote" ] ; then
        verbose "Fork remote is $fork_remote"
    else
        error "Cannot auto-determine fork remote"
        if [ "$EXPLICIT_FORK" ] ; then
            info "(Could not find $EXPLICIT_FORK fork of ceph/ceph in \"git remote -v\")"
        else
            info "(Could not find GitHub user ${github_user}'s fork of ceph/ceph in \"git remote -v\")"
        fi
        setup_ok=""
    fi
}

function vet_setup {
    local argument="$1"
    local not_set="!!! NOT SET !!!"
    local invalid="!!! INVALID !!!"
    local redmine_endpoint_display
    local redmine_user_id_display
    local github_endpoint_display
    local github_user_display
    local upstream_remote_display
    local fork_remote_display
    local redmine_key_display
    local github_token_display
    debug "Entering vet_setup with argument $argument"
    if [ "$argument" = "--report" ] || [ "$argument" = "--normal-operation" ] ; then
        [ "$github_token" ] && [ "$setup_ok" ] && set_github_user_from_github_token quiet
        init_upstream_remote
        [ "$github_token" ] && [ "$setup_ok" ] && init_fork_remote
        vet_remotes
        [ "$redmine_key" ] && set_redmine_user_from_redmine_key
    fi
    if [ "$github_token" ] ; then
        if [ "$setup_ok" ] ; then
            github_token_display="(OK; value not shown)"
        else
            github_token_display="$invalid"
        fi
    else
        github_token_display="$not_set"
    fi
    if [ "$redmine_key" ] ; then
        if [ "$setup_ok" ] ; then
            redmine_key_display="(OK; value not shown)"
        else
            redmine_key_display="$invalid"
        fi
    else
        redmine_key_display="$not_set"
    fi
    redmine_endpoint_display="${redmine_endpoint:-$not_set}"
    redmine_user_id_display="${redmine_user_id:-$not_set}"
    github_endpoint_display="${github_endpoint:-$not_set}"
    github_user_display="${github_user:-$not_set}"
    upstream_remote_display="${upstream_remote:-$not_set}"
    fork_remote_display="${fork_remote:-$not_set}"
    test "$redmine_endpoint" || failed_mandatory_var_check redmine_endpoint "not set"
    test "$redmine_user_id"  || failed_mandatory_var_check redmine_user_id "could not be determined"
    test "$redmine_key"      || failed_mandatory_var_check redmine_key "not set"
    test "$github_endpoint"  || failed_mandatory_var_check github_endpoint "not set"
    test "$github_user"      || failed_mandatory_var_check github_user "could not be determined"
    test "$github_token"     || failed_mandatory_var_check github_token "not set"
    test "$upstream_remote"  || failed_mandatory_var_check upstream_remote "could not be determined"
    test "$fork_remote"      || failed_mandatory_var_check fork_remote "could not be determined"
    if [ "$argument" = "--report" ] || [ "$argument" == "--interactive" ] ; then
        read -r -d '' setup_summary <<EOM || true > /dev/null 2>&1
redmine_endpoint $redmine_endpoint
redmine_user_id  $redmine_user_id_display
redmine_key      $redmine_key_display
github_endpoint  $github_endpoint
github_user      $github_user_display
github_token     $github_token_display
upstream_remote  $upstream_remote_display
fork_remote      $fork_remote_display
EOM
        log bare
        log bare "============================================="
        log bare "        ${this_script} setup report"
        log bare "============================================="
        log bare "variable name    value"
        log bare "---------------------------------------------"
        log bare "$setup_summary"
        log bare "---------------------------------------------"
    else
        verbose "redmine_endpoint $redmine_endpoint_display"
        verbose "redmine_user_id  $redmine_user_id_display"
        verbose "redmine_key      $redmine_key_display"
        verbose "github_endpoint  $github_endpoint_display"
        verbose "github_user      $github_user_display"
        verbose "github_token     $github_token_display"
        verbose "upstream_remote  $upstream_remote_display"
        verbose "fork_remote      $fork_remote_display"
    fi
    if [ "$argument" = "--report" ] || [ "$argument" = "--interactive" ] ; then
        if [ "$setup_ok" ] ; then
            info "setup is OK"
        else
            info "setup is NOT OK"
        fi
        log bare "=============================================="
        log bare
    fi
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
    abort_due_to_setup_problem
fi

#
# do we have jq available?
#

if type jq >/dev/null 2>&1 ; then
    debug "jq is available. Good."
else
    error "This script uses jq, but it does not seem to be installed"
    abort_due_to_setup_problem
fi

#
# is jq available?
#

if command -v jq >/dev/null ; then
    debug "jq is available. Good."
else
    error "This script needs \"jq\" in order to work, and it is not available"
    abort_due_to_setup_problem
fi


#
# process command-line arguments
#

munged_options=$(getopt -o c:dhsv --long "cherry-pick-only,component:,debug,existing-pr:,force,fork:,help,milestones,prepare,setup,setup-report,troubleshooting,update-version,usage,verbose,version" -n "$this_script" -- "$@")
eval set -- "$munged_options"

ADVICE=""
CHECK_MILESTONES=""
CHERRY_PICK_ONLY=""
CHERRY_PICK_PHASE="yes"
DEBUG=""
EXISTING_PR=""
EXPLICIT_COMPONENT=""
EXPLICIT_FORK=""
FORCE=""
HELP=""
INTERACTIVE_SETUP_ROUTINE=""
ISSUE=""
PR_PHASE="yes"
SETUP_OPTION=""
TRACKER_PHASE="yes"
TROUBLESHOOTING_ADVICE=""
USAGE_ADVICE=""
VERBOSE=""
while true ; do
    case "$1" in
        --cherry-pick-only) CHERRY_PICK_PHASE="yes" ; PR_PHASE="" ; TRACKER_PHASE="" ; shift ;;
        --component|-c) shift ; EXPLICIT_COMPONENT="$1" ; shift ;;
        --debug|-d) DEBUG="$1" ; shift ;;
        --existing-pr) shift ; EXISTING_PR="$1" ; CHERRY_PICK_PHASE="" ; PR_PHASE="" ; shift ;;
        --force) FORCE="$1" ; shift ;;
        --fork) shift ; EXPLICIT_FORK="$1" ; shift ;;
        --help|-h) ADVICE="1" ; HELP="$1" ; shift ;;
        --milestones) CHECK_MILESTONES="$1" ; shift ;;
        --prepare) CHERRY_PICK_PHASE="yes" ; PR_PHASE="" ; TRACKER_PHASE="" ; shift ;;
        --setup*|-s) SETUP_OPTION="$1" ; shift ;;
        --troubleshooting) ADVICE="$1" ; TROUBLESHOOTING_ADVICE="$1" ; shift ;;
        --update-version) update_version_number_and_exit ;;
        --usage) ADVICE="$1" ; USAGE_ADVICE="$1" ; shift ;;
        --verbose|-v) VERBOSE="$1" ; shift ;;
        --version) display_version_message_and_exit ;;
        --) shift ; ISSUE="$1" ; break ;;
        *) echo "Internal error" ; false ;;
    esac
done

if [ "$ADVICE" ] ; then
    [ "$HELP" ] && usage
    [ "$USAGE_ADVICE" ] && usage_advice
    [ "$TROUBLESHOOTING_ADVICE" ] && troubleshooting_advice
    exit 0
fi

if [ "$SETUP_OPTION" ] || [ "$CHECK_MILESTONES" ] ; then
    ISSUE="0"
fi

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
# make sure setup has been completed
#

init_endpoints
init_github_token
init_redmine_key
setup_ok="OK"
if [ "$SETUP_OPTION" ] ; then
    vet_setup --report
    maybe_delete_deprecated_backport_common
    if [ "$setup_ok" ] ; then
        exit 0
    else
        default_val="y"
        echo -n "Run the interactive setup routine now? (default: ${default_val}) "
        yes_or_no_answer="$(get_user_input "$default_val")"
        [ "$yes_or_no_answer" ] && yes_or_no_answer="${yes_or_no_answer:0:1}"
        if [ "$yes_or_no_answer" = "y" ] ; then
            INTERACTIVE_SETUP_ROUTINE="yes"
        else
            if [ "$FORCE" ] ; then
                warning "--force was given; proceeding with broken setup"
            else
                info "Bailing out!"
                exit 1
            fi
        fi
    fi
fi
if [ "$INTERACTIVE_SETUP_ROUTINE" ] ; then
    interactive_setup_routine
else
    vet_setup --normal-operation
    maybe_delete_deprecated_backport_common
fi
if [ "$INTERACTIVE_SETUP_ROUTINE" ] || [ "$SETUP_OPTION" ] ; then
    echo
    if [ "$setup_ok" ] ; then
        if [ "$ISSUE" ] && [ "$ISSUE" != "0" ] ; then
            true
        else
            exit 0
        fi
    else
        exit 1
    fi
fi
vet_remotes
[ "$setup_ok" ] || abort_due_to_setup_problem

#
# query remote GitHub API for active milestones
#

verbose "Querying GitHub API for active milestones"
remote_api_output="$(curl -u ${github_user}:${github_token} --silent -X GET "https://api.github.com/repos/ceph/ceph/milestones")"
active_milestones="$(echo "$remote_api_output" | jq -r '.[] | .title')"
if [ "$active_milestones" = "null" ] ; then
    error "Could not determine the active milestones"
    bail_out_github_api "$remote_api_output"
fi

if [ "$CHECK_MILESTONES" ] ; then
    check_milestones "$active_milestones"
    exit 0
fi

#
# query remote Redmine API for information about the Backport tracker issue
#

redmine_url="$(number_to_url "redmine" "${issue}")"
debug "Considering Redmine issue: $redmine_url - is it in the Backport tracker?"

remote_api_output="$(curl --silent "${redmine_url}.json")"
tracker="$(echo "$remote_api_output" | jq -r '.issue.tracker.name')"
if [ "$tracker" = "Backport" ]; then
    debug "Yes, $redmine_url is a Backport issue"
else
    error "Issue $redmine_url is not a Backport"
    info "(This script only works with Backport tracker issues.)"
    false
fi

debug "Looking up release/milestone of $redmine_url"
milestone="$(echo "$remote_api_output" | jq -r '.issue.custom_fields[0].value')"
if [ "$milestone" ] ; then
    debug "Release/milestone: $milestone"
else
    error "could not obtain release/milestone from ${redmine_url}"
    false
fi

debug "Looking up status of $redmine_url"
tracker_status_id="$(echo "$remote_api_output" | jq -r '.issue.status.id')"
tracker_status_name="$(echo "$remote_api_output" | jq -r '.issue.status.name')"
if [ "$tracker_status_name" ] ; then
    debug "Tracker status: $tracker_status_name"
    if [ "$FORCE" ] || [ "$EXISTING_PR" ] ; then
        test "$(check_tracker_status "$tracker_status_name")" || true
    else
        test "$(check_tracker_status "$tracker_status_name")"
    fi
else
    error "could not obtain status from ${redmine_url}"
    false
fi

tracker_title="$(echo "$remote_api_output" | jq -r '.issue.subject')"
debug "Title of $redmine_url is ->$tracker_title<-"

tracker_description="$(echo "$remote_api_output" | jq -r '.issue.description')"
debug "Description of $redmine_url is ->$tracker_description<-"

tracker_assignee_id="$(echo "$remote_api_output" | jq -r '.issue.assigned_to.id')"
tracker_assignee_name="$(echo "$remote_api_output" | jq -r '.issue.assigned_to.name')"
if [ "$tracker_assignee_id" = "null" ] || [ "$tracker_assignee_id" = "$redmine_user_id" ] ; then
    true
else
    error_msg_1="$redmine_url is assigned to someone else: $tracker_assignee_name (ID $tracker_assignee_id)"
    error_msg_2="(my ID is $redmine_user_id)"
    if [ "$FORCE" ] || [ "$EXISTING_PR" ] ; then
        warning "$error_msg_1"
        info "$error_msg_2"
        info "--force and/or --existing-pr given: continuing execution"
    else
        error "$error_msg_1"
        info "$error_msg_2"
        info "Cowardly refusing to continue"
        false
    fi
fi

if [ -z "$(is_active_milestone "$milestone")" ] ; then
    error "$redmine_url is a backport to $milestone which is not an active milestone"
    info "Cowardly refusing to work on a backport to an inactive release"
    false
fi

milestone_number=$(try_known_milestones "$milestone")
if [ "$milestone_number" -gt "0" ] >/dev/null 2>&1 ; then
    debug "Milestone ->$milestone<- is known to have number ->$milestone_number<-: skipping remote API call"
else
    warning "Milestone ->$milestone<- is unknown to the script: falling back to GitHub API"
    milestone_number=$(milestone_number_from_remote_api "$milestone")
fi
target_branch="$milestone"
info "milestone/release is $milestone"
debug "milestone number is $milestone_number"

if [ "$CHERRY_PICK_PHASE" ] ; then
    local_branch=wip-${issue}-${target_branch}
    if git show-ref --verify --quiet "refs/heads/$local_branch" ; then
        if [ "$FORCE" ] ; then
            warning "local branch $local_branch already exists"
            info "--force was given: will clobber $local_branch and attempt automated cherry-pick"
            cherry_pick_phase
        elif [ "$CHERRY_PICK_ONLY" ] ; then
            error "local branch $local_branch already exists"
            info "Cowardly refusing to clobber $local_branch as it might contain valuable data"
            info "(hint) run with --force to clobber it and attempt the cherry-pick"
            false
        fi
        if [ "$FORCE" ] || [ "$CHERRY_PICK_ONLY" ] ; then
            true
        else
            info "local branch $local_branch already exists: skipping cherry-pick phase"
        fi
    else
        info "$local_branch does not exist: will create it and attempt automated cherry-pick"
        cherry_pick_phase
    fi
fi

if [ "$PR_PHASE" ] ; then
    current_branch=$(git rev-parse --abbrev-ref HEAD)
    if [ "$current_branch" = "$local_branch" ] ; then
        true
    else
        set -x
        git checkout "$local_branch"
        set +x
        maybe_restore_set_x
    fi
    
    set -x
    git push -u "$fork_remote" "$local_branch"
    set +x
    maybe_restore_set_x
    
    original_issue=""
    original_pr=""
    original_pr_url=""
    
    debug "Generating backport PR description"
    populate_original_issue
    populate_original_pr
    desc="backport tracker: ${redmine_url}"
    if [ "$original_pr" ] || [ "$original_issue" ] ; then
        desc="${desc}\n\n---\n"
        [ "$original_pr"    ] && desc="${desc}\nbackport of $(number_to_url "github" "${original_pr}")"
        [ "$original_issue" ] && desc="${desc}\nparent tracker: $(number_to_url "redmine" "${original_issue}")"
    fi
    desc="${desc}\n\nthis backport was staged using ceph-backport.sh version ${SCRIPT_VERSION}\nfind the latest version at ${github_endpoint}/blob/main/src/script/ceph-backport.sh"
    desc="$desc\n\n"

    while read line; do
        desc="$desc$line"
    done < ${gh_pr_template}
    
    debug "Generating backport PR title"
    if [ "$original_pr" ] ; then
        backport_pr_title="${milestone}: $(curl --silent https://api.github.com/repos/ceph/ceph/pulls/${original_pr} | jq -r '.title')"
    else
        if [[ $tracker_title =~ ^${milestone}: ]] ; then
            backport_pr_title="${tracker_title}"
        else
            backport_pr_title="${milestone}: ${tracker_title}"
        fi
    fi
    if [[ "$backport_pr_title" =~ \" ]] ; then
        backport_pr_title="${backport_pr_title//\"/\\\"}"
    fi
    
    debug "Opening backport PR"
    if [ "$EXPLICIT_FORK" ] ; then
        source_repo="$EXPLICIT_FORK"
    else
        source_repo="$github_user"
    fi
    remote_api_output=$(curl -u ${github_user}:${github_token} --silent --data-binary "{\"title\":\"${backport_pr_title}\",\"head\":\"${source_repo}:${local_branch}\",\"base\":\"${target_branch}\",\"body\":\"${desc}\"}" "https://api.github.com/repos/ceph/ceph/pulls")
    backport_pr_number=$(echo "$remote_api_output" | jq -r .number)
    if [ -z "$backport_pr_number" ] || [ "$backport_pr_number" = "null" ] ; then
        error "failed to open backport PR"
        bail_out_github_api "$remote_api_output"
    fi
    backport_pr_url="$(number_to_url "github" "$backport_pr_number")"
    info "Opened backport PR ${backport_pr_url}"
fi

if [ "$EXISTING_PR" ] ; then
    populate_original_issue
    populate_original_pr
    backport_pr_number="$EXISTING_PR"
    backport_pr_url="$(number_to_url "github" "$backport_pr_number")"
    existing_pr_routine
fi

if [ "$PR_PHASE" ] || [ "$EXISTING_PR" ] ; then
    maybe_update_pr_milestone_labels
    pgrep firefox >/dev/null && firefox "${backport_pr_url}"
fi

if [ "$TRACKER_PHASE" ] ; then
    debug "Considering Backport tracker issue ${redmine_url}"
    status_should_be=2 # In Progress
    desc_should_be="${backport_pr_url}"
    assignee_should_be="${redmine_user_id}"
    if [ "$EXISTING_PR" ] ; then
        data_binary="{\"issue\":{\"description\":\"${desc_should_be}\",\"status_id\":${status_should_be}}}"
    else
        data_binary="{\"issue\":{\"description\":\"${desc_should_be}\",\"status_id\":${status_should_be},\"assigned_to_id\":${assignee_should_be}}}"
    fi
    remote_api_status_code="$(curl --write-out '%{http_code}' --output /dev/null --silent -X PUT --header "Content-type: application/json" --data-binary "${data_binary}" "${redmine_url}.json?key=$redmine_key")"
    if [ "$FORCE" ] || [ "$EXISTING_PR" ] ; then 
        true
    else
        if [ "${remote_api_status_code:0:1}" = "2" ] ; then
            true
        elif [ "${remote_api_status_code:0:1}" = "4" ] ; then
            warning "remote API ${redmine_endpoint} returned status ${remote_api_status_code}"
            info "This merely indicates that you cannot modify issue fields at ${redmine_endpoint}"
            info "and does not limit your ability to do backports."
        else
            error "Remote API ${redmine_endpoint} returned unexpected response code ${remote_api_status_code}"
        fi
    fi
    # check if anything actually changed on the Redmine issue
    remote_api_output=$(curl --silent "${redmine_url}.json?include=journals")
    status_is="$(echo "$remote_api_output" | jq -r '.issue.status.id')"
    desc_is="$(echo "$remote_api_output" | jq -r '.issue.description')"
    assignee_is="$(echo "$remote_api_output" | jq -r '.issue.assigned_to.id')"
    tracker_was_updated=""
    tracker_is_in_desired_state="yes"
    [ "$(tracker_component_was_updated "status" "$tracker_status_id" "$status_is")" ] && tracker_was_updated="yes"
    [ "$(tracker_component_was_updated "desc" "$tracker_description" "$desc_is")" ] && tracker_was_updated="yes"
    if [ "$EXISTING_PR" ] ; then
         true
    else
         [ "$(tracker_component_was_updated "assignee" "$tracker_assignee_id" "$assignee_is")" ] && tracker_was_updated="yes"
    fi
    [ "$(tracker_component_is_in_desired_state "status" "$status_is" "$status_should_be")" ] || tracker_is_in_desired_state=""
    [ "$(tracker_component_is_in_desired_state "desc" "$desc_is" "$desc_should_be")" ] || tracker_is_in_desired_state=""
    if [ "$EXISTING_PR" ] ; then
        true
    else
        [ "$(tracker_component_is_in_desired_state "assignee" "$assignee_is" "$assignee_should_be")" ] || tracker_is_in_desired_state=""
    fi
    if [ "$tracker_is_in_desired_state" ] ; then
        [ "$tracker_was_updated" ] && info "Backport tracker ${redmine_url} was updated"
        info "Backport tracker ${redmine_url} is in the desired state"
        pgrep firefox >/dev/null && firefox "${redmine_url}"
        exit 0
    fi
    if [ "$tracker_was_updated" ] ; then
        warning "backport tracker ${redmine_url} was updated, but is not in the desired state. Please check it."
        pgrep firefox >/dev/null && firefox "${redmine_url}"
        exit 1
    else
        data_binary="{\"issue\":{\"notes\":\"please link this Backport tracker issue with GitHub PR ${desc_should_be}\nceph-backport.sh version ${SCRIPT_VERSION}\"}}"
        remote_api_status_code=$(curl --write-out '%{http_code}' --output /dev/null --silent -X PUT --header "Content-type: application/json" --data-binary "${data_binary}" "${redmine_url}.json?key=$redmine_key")
        if [ "${remote_api_status_code:0:1}" = "2" ] ; then
            info "Comment added to ${redmine_url}"
        fi
        exit 0
    fi
fi
