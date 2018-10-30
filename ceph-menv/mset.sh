get_color() {
        s=$1
        sum=1   # just so that 'c1' isn't green that doesn't contrast with the rest of my prompt
        for i in `seq 1 ${#s}`; do
                c=${s:$((i-1)):1};
                o=`printf '%d' "'$c"`
                sum=$((sum+$o))
        done
        echo $sum
}

if [ "$1" == "" ]; then
        unset MRUN_CLUSTER
        unset MRUN_PROMPT
else
        export MRUN_CLUSTER=$1
        export MRUN_PROMPT='['${MRUN_CLUSTER}'] '
        col=$(get_color $1)
        MRUN_PROMPT_COLOR=$((col%7+31))
fi

