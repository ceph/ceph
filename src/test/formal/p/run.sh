#!/bin/bash
# run.sh <model-dir> [schedules] [jobs]: check every test case of a P model
# against its expected outcome, jobs at a time (default 20000 schedules, 1 job).
# <model-dir>/expect.txt lists "<test case> holds" or
# "<test case> violated <text the failure message must contain>".
set -u
dir=$(cd "$1" && pwd); N=${2:-20000}; J=${3:-1}
# P installs into ~/.dotnet/tools; a Homebrew dotnet@8 also needs DOTNET_ROOT
export PATH=$PATH:$HOME/.dotnet/tools
brew_dotnet=/opt/homebrew/opt/dotnet@8
if [ -z "${DOTNET_ROOT:-}" ] && [ -d "$brew_dotnet" ]; then
  export DOTNET_ROOT=$brew_dotnet/libexec PATH=$brew_dotnet/bin:$PATH
fi
export DOTNET_CLI_TELEMETRY_OPTOUT=1 N
cd "$dir"
p compile > PGenerated.compile.log 2>&1 || { tail -20 PGenerated.compile.log; exit 1; }

check() {
  local tc want text out got why mark
  read -r tc want text <<< "$1"
  rm -rf "PCheckerOutput/$tc"
  out=$(p check -tc "$tc" -s "$N" --sch-pct 3 --max-steps 5000 -o "PCheckerOutput/$tc" 2>&1)
  # P can run more than one batch and print a summary for each: any bug
  # in any of them is a violation
  if echo "$out" | grep -qE "Checker found a bug|Found [1-9][0-9]* bugs?"; then got=violated
  elif echo "$out" | grep -q "Found 0 bugs"; then got=holds; else got=error; fi
  why=$(grep -h -m1 "<ErrorLog>" PCheckerOutput/$tc/BugFinding/*.txt 2>/dev/null | sed 's/.*<ErrorLog> //' | cut -c1-140)
  mark=ok
  if [ "$got" != "$want" ]; then mark=UNEXPECTED
  elif [ "$want" = violated ] && [ -n "$text" ] && ! echo "$why" | grep -qF "$text"; then mark=WRONG-FAILURE; fi
  printf "%-34s %-9s %-13s %s\n" "$tc" "$got" "$mark" "$why"
}
export -f check

# one expect.txt line per check, whole, so its text may hold any character
out=$(grep -vE '^[[:space:]]*(#|$)' expect.txt | tr '\n' '\0' | xargs -0 -n 1 -P "$J" bash -c 'check "$0"')
echo "$out"
! echo "$out" | grep -qE " (UNEXPECTED|WRONG-FAILURE) "
