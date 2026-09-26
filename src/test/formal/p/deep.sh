#!/bin/bash
# deep.sh <model-dir> <test case> [schedules]: one test case under random, PCT and POS
set -u
dir=$(cd "$1" && pwd); tc=$2; N=${3:-100000}
# P installs into ~/.dotnet/tools; a Homebrew dotnet@8 also needs DOTNET_ROOT
export PATH=$PATH:$HOME/.dotnet/tools
brew_dotnet=/opt/homebrew/opt/dotnet@8
if [ -z "${DOTNET_ROOT:-}" ] && [ -d "$brew_dotnet" ]; then
  export DOTNET_ROOT=$brew_dotnet/libexec PATH=$brew_dotnet/bin:$PATH
fi
export DOTNET_CLI_TELEMETRY_OPTOUT=1
cd "$dir"
for strat in "--sch-random" "--sch-pct 5" "--sch-pos"; do
  s=$(date +%s)
  out=$(p check -tc "$tc" -s "$N" $strat --max-steps 5000 -o "PCheckerOutput/deep-$tc" 2>&1)
  bugs=$(echo "$out" | grep -oE 'Found [0-9]+ bugs?' | awk '{n+=$2} END {print n+0}')
  sched=$(echo "$out" | grep -oE 'Explored [0-9]+ schedules' | awk '{n+=$2} END {print n+0}')
  echo "$(basename $dir) $tc $strat: $bugs bug(s) in $sched schedules, $(( $(date +%s)-s ))s"
done
