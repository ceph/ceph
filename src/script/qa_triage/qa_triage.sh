#!/usr/bin/env bash
# qa_triage: find the known problems among a teuthology run's failures. It lives
# in a repository of its own; this clones it into qa-triage/ here, which git
# ignores, the first time, brings it up to date each time after, and runs it.
# Run it from anywhere, or from a link to it on your PATH:
#
#   src/script/qa_triage/qa_triage.sh https://pulpito.ceph.com/<run>/
#
# QA_TRIAGE_REPO names another repository to clone, and QA_TRIAGE_CHECKOUT
# another directory to keep it in. Its README.md says the rest.

set -euo pipefail

# This directory, even when run through a link.
source=${BASH_SOURCE[0]}
while [ -L "$source" ]; do
    dir=$(cd "$(dirname "$source")" && pwd)
    source=$(readlink "$source")
    [[ $source == /* ]] || source=$dir/$source
done
here=$(cd "$(dirname "$source")" && pwd)

repo=${QA_TRIAGE_REPO:-git@github.com:aainscow/qa-triage.git}
checkout=${QA_TRIAGE_CHECKOUT:-$here/qa-triage}

if [ ! -d "$checkout/.git" ]; then
    echo "qa_triage: cloning $repo into $checkout" >&2
    if ! git clone -q "$repo" "$checkout"; then
        echo "qa_triage: could not clone $repo, which is private: its owner adds" \
             "people as collaborators" >&2
        exit 2
    fi
elif ! out=$(git -C "$checkout" pull -q --ff-only 2>&1); then
    echo "qa_triage: could not bring $checkout up to date, so it runs as it is:" \
         "${out##*$'\n'}" >&2
fi

exec "$checkout/qa_triage.sh" "$@"
