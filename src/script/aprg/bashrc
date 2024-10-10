# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# User specific environment
if ! [[ "$PATH" =~ "$HOME/.local/bin:$HOME/bin:" ]]
then
    PATH="$HOME/.local/bin:$HOME/bin:$PATH"
fi
export PATH

# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=

# User specific aliases and functions
 
alias rm='rm -i'
alias cp='cp -i'
alias mv='mv -i'
alias psf='ps ax --forest'

export LSCOLORS="gxfxbEaEBxxEhEhBaDaCaD"
export HISTSIZE=32768;
COLOFF="\[\033[00m\]"
LGREEN="\[\033[01;32m\]"
LBLUE="\[\033[01;34m\]"
RED="\[\033[31m\]"
ORANGE="\[\033[0;33m\]"$

export sps1="\h@\w$ "
export CMAKE_EXPORT_COMPILE_COMMANDS=ON
function giprompt () { [ -d .git ] && git branch --show-current; }
PS1=$ORANGE'\u@\h'$COLOFF':'$LBLUE'\w\n'$COLOFF$RED'[\t]$'$LGREEN'$(giprompt)'$COLOFF' # '
# git config receive.denyCurrentBranch ignore

[ -f "/ceph/build/vstart_environment.sh" ] && source /ceph/build/vstart_environment.sh

# Auxiliar function to process flamegraphs:
function pfilt () {
  time for x in *perf.out; do y=${x/perf.out/scripted.gz}; perf script -i $x | c++filt | gzip -9 > $y; mv $x /tmp/done/; echo "$x ... done"; done
}
