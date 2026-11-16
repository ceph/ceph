#!/usr/bin/env bash
# qa_triage: find the known problems among a teuthology run's failures. It lives
# in a repository of its own; this clones it into qa-triage/ here, which git
# ignores, the first time, brings it up to date each time after, and runs it.
# Run it from anywhere, or from a link to it on your PATH:
#
#   src/script/qa_triage/qa_triage.sh https://pulpito.ceph.com/<run>/
#
# It clones over ssh, with your GitHub key; where that fails, as on a host whose
# key for GitHub is a deploy key for another repository, it clones over https with
# gh's sign-in, if gh is signed in. Where GitHub takes no ssh key from this host and
# gh is installed but not signed in, it stops and asks you to run gh auth login.
# QA_TRIAGE_REPO names another repository to clone, and QA_TRIAGE_CHECKOUT another
# directory to keep it in. Its README.md says the rest.

set -euo pipefail

# This directory, even when run through a link.
source=${BASH_SOURCE[0]}
while [ -L "$source" ]; do
    dir=$(cd "$(dirname "$source")" && pwd)
    source=$(readlink "$source")
    [[ $source == /* ]] || source=$dir/$source
done
here=$(cd "$(dirname "$source")" && pwd)

https=https://github.com/aainscow/qa-triage.git
repo=${QA_TRIAGE_REPO:-git@github.com:aainscow/qa-triage.git}
checkout=${QA_TRIAGE_CHECKOUT:-$here/qa-triage}

# Git asks gh for GitHub's https credentials: for this script and what it runs,
# qa_triage's own pulls and pushes of the library too, not for git elsewhere.
use_gh() {
    command -v gh > /dev/null 2>&1 && gh auth status > /dev/null 2>&1 || return 1
    local n=${GIT_CONFIG_COUNT:-0}
    export "GIT_CONFIG_KEY_$n=credential.https://github.com.helper"
    export "GIT_CONFIG_VALUE_$n=!gh auth git-credential"
    export GIT_CONFIG_COUNT=$((n + 1))
}

# As use_gh, and git reaches GitHub's ssh URLs over https too: for a clone made over
# ssh, on a host whose ssh key GitHub no longer takes.
over_https() {
    use_gh || return 1
    local n=$GIT_CONFIG_COUNT
    export "GIT_CONFIG_KEY_$n=url.https://github.com/.insteadOf"
    export "GIT_CONFIG_VALUE_$n=git@github.com:"
    export GIT_CONFIG_COUNT=$((n + 1))
}

# GitHub takes no ssh key from this host: it has none, or none GitHub knows.
key_refused() {
    local said
    said=$(ssh -T -o BatchMode=yes -o ConnectTimeout=10 git@github.com 2>&1 || true)
    [[ $said == *'Permission denied'* ]]
}

# gh is installed, but not signed in.
gh_signed_out() {
    command -v gh > /dev/null 2>&1 && ! gh auth status > /dev/null 2>&1
}

# Stop: without an ssh key or gh's sign-in, qa_triage can neither stay up to date nor
# share what people teach it.
insist_on_gh() {
    echo "qa_triage: GitHub does not accept an ssh key from this host, and gh is not" \
         "signed in, so qa_triage cannot $1, or share what you teach it. Sign in to" \
         "GitHub with gh, then run qa_triage again:" >&2
    echo "    gh auth login" >&2
    echo "qa_triage then uses gh's sign-in for its own git commands only." >&2
    exit 2
}

if [ ! -d "$checkout/.git" ]; then
    echo "qa_triage: cloning $repo into $checkout" >&2
    if ! git clone -q "$repo" "$checkout"; then
        if [ -z "${QA_TRIAGE_REPO:-}" ] && use_gh && git clone -q "$https" "$checkout"; then
            echo "qa_triage: cloned it over https, with gh's sign-in, as ssh could not" >&2
        elif [[ $repo == git@github.com:* ]] && gh_signed_out && key_refused; then
            insist_on_gh "clone itself"
        else
            echo "qa_triage: could not clone $repo, which is private: its owner adds" \
                 "people as collaborators. If you are one, and your ssh key for GitHub" \
                 "is not your own (a deploy key, say), sign in with gh auth login, or" \
                 "set QA_TRIAGE_REPO=$https" >&2
            exit 2
        fi
    fi
else
    origin=$(git -C "$checkout" remote get-url origin 2> /dev/null || true)
    case $origin in
        https://github.com/*) use_gh || true ;;
    esac
    pulled=0
    out=$(git -C "$checkout" pull -q --ff-only 2>&1) && pulled=1
    if [ $pulled = 0 ] && [[ $origin == git@github.com:* ]] && key_refused; then
        # GitHub no longer takes this host's ssh key: over https with gh's sign-in,
        # for this pull and for qa_triage's own pulls and pushes of the library.
        if over_https; then
            out=$(git -C "$checkout" pull -q --ff-only 2>&1) && pulled=1
        elif gh_signed_out; then
            insist_on_gh "bring itself up to date"
        fi
    elif [ $pulled = 0 ] && [[ $origin == https://github.com/* ]] && gh_signed_out &&
        [[ $out == *'Authentication failed'* || $out == *'could not read Username'* ||
           $out == *'terminal prompts disabled'* || $out == *'Repository not found'* ]]; then
        insist_on_gh "bring itself up to date"
    fi
    if [ $pulled = 0 ]; then
        # Changes made in it and never shared hold it back: set aside, where git keeps
        # them, so that it runs as everyone's does.
        if [ -n "$(git -C "$checkout" status --porcelain)" ] &&
            git -C "$checkout" stash push -q --include-untracked \
                -m "qa_triage: changes never shared, set aside on $(date '+%F %R')"; then
            echo "qa_triage: $checkout had changes made in it and never shared: they are" \
                 "set aside, so that it is up to date. git -C $checkout stash list lists" \
                 "them, and stash pop brings them back." >&2
            git -C "$checkout" pull -q --ff-only > /dev/null 2>&1 && out= || true
        fi
        if [ -n "$out" ]; then
            # Commits made in it and never pushed are left as they are: they are not
            # for this script to move or delete.
            if [ -n "$(git -C "$checkout" rev-list '@{u}..HEAD' 2> /dev/null)" ]; then
                echo "qa_triage: $checkout has commits made in it and never pushed, so it" \
                     "runs as it is, not up to date: git -C $checkout log @{u}..HEAD lists" \
                     "them." >&2
            else
                echo "qa_triage: could not bring $checkout up to date, so it runs as it is:" \
                     "${out##*$'\n'}" >&2
            fi
        fi
    fi
fi

exec "$checkout/qa_triage.sh" "$@"
