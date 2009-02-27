
get_val() {
  [ "$2" != "" ] && export $1=$2 || export $1=`$CCONF -c $conf "$3" "$4" "$5"`
}

get_val_bool() {
  if [ "$2" != "" ]; then
	export $1=$2
  else
	tmp=`$CCONF "$3" "$4" "$5"`
	export $1=$5

	[ "$tmp" == "0" ] && export $1=0
	[ "$tmp" == "false" ] && export $1=0
	[ "$tmp" == "1" ] && export $1=1
	[ "$tmp" == "true" ] && export $1=1
  fi
}

get_conf() {
	var=$1
	def=$2
	key=$3
	shift; shift; shift

	tmp=""
	while [ $# -ge 1 ]; do
		tmp=$tmp" -s $1"
		shift
	done
	eval $var=`$CCONF -c $conf $tmp "$key" "$def"`
}

get_conf_bool() {
	get_conf "$@"

	eval "val=$"$1
	[ "$val" == "0" ] && export $1=0
	[ "$val" == "false" ] && export $1=0
	[ "$val" == "1" ] && export $1=1
	[ "$val" == "true" ] && export $1=1
}

