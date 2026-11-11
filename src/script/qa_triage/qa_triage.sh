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
# gh's sign-in, if gh is signed in. QA_TRIAGE_REPO names another repository to
# clone, and QA_TRIAGE_CHECKOUT another directory to keep it in. Its README.md says
# the rest.

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

if [ ! -d "$checkout/.git" ]; then
    echo "qa_triage: cloning $repo into $checkout" >&2
    if ! git clone -q "$repo" "$checkout"; then
        if [ -z "${QA_TRIAGE_REPO:-}" ] && use_gh && git clone -q "$https" "$checkout"; then
            echo "qa_triage: cloned it over https, with gh's sign-in, as ssh could not" >&2
        else
            echo "qa_triage: could not clone $repo, which is private: its owner adds" \
                 "people as collaborators. If you are one, and your ssh key for GitHub" \
                 "is not your own (a deploy key, say), sign in with gh auth login, or" \
                 "set QA_TRIAGE_REPO=$https" >&2
            exit 2
        fi
    fi
else
    case $(git -C "$checkout" remote get-url origin 2> /dev/null) in
        https://github.com/*) use_gh || true ;;
    esac
    # The commits of what was learned here and not pushed yet go on top of what
    # others pushed since; a rebase that stops is undone, never left part way.
    rebase() {
        out=$(git -C "$checkout" pull -q --rebase 2>&1) && return 0
        git -C "$checkout" rebase --abort > /dev/null 2>&1 || true
        return 1
    }
    aside() {
        echo "qa_triage: $checkout had $1 made in it and never shared: they are set" \
             "aside, so that it is up to date. $2" >&2
    }
    if ! out=$(git -C "$checkout" pull -q --ff-only 2>&1); then
        # Changes made in it and never shared hold it back: set aside, where git keeps
        # them, so that it runs as everyone's does.
        if [ -n "$(git -C "$checkout" status --porcelain)" ] &&
            git -C "$checkout" stash push -q --include-untracked \
                -m "qa_triage: changes never shared, set aside on $(date '+%F %R')"; then
            aside changes "git -C $checkout stash list lists them, and stash pop brings them back."
        fi
        if ! rebase; then
            # Commits that do not go onto what others pushed: on a branch of their own.
            branch=qa-triage/set-aside-$(date '+%Y%m%d-%H%M%S')
            if [ -n "$(git -C "$checkout" rev-list '@{u}..HEAD' 2> /dev/null)" ] &&
                git -C "$checkout" branch "$branch" HEAD &&
                git -C "$checkout" reset -q --keep '@{u}' && rebase; then
                aside commits "git -C $checkout log @{u}..$branch lists them."
            else
                echo "qa_triage: could not bring $checkout up to date, so it runs as it is:" \
                     "${out##*$'\n'}" >&2
            fi
        fi
    fi
fi

exec "$checkout/qa_triage.sh" "$@"
