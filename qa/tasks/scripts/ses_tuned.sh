set -ex

function check_tuned() {
    local role=$1
    shift
    local minions="$@" # this renders rest of arguments as single space separated list
    local roles=($role_list)
    if [ ${#roles} -gt 1 ] ; then
        for server in $minions ; do
            local role=$(salt $server cmd.run "tuned-adm active | egrep '$role|virtual-guest'" --output=json | jq -r .[] | cut -d : -f 2)
      done
    fi
    salt -L "${minions// /,}" cmd.run "tuned-adm active | egrep '$role|virtual-guest'" 
}


role_list=($(salt '*' pillar.get "roles" | sort -u | grep "\- " | awk '{print $2}' | egrep -v 'admin|master|grafana'))

for role in ${role_list[@]}
do 
    test "$role" == "ceph-osd" && role=storage
    minions=($(salt-run select.minions roles=$role --output=json | jq -r .[] | grep -v $(hostname -f)))
    minion_list="${minions[@]}"  # this converts array to space separated list
    minion_list=${minion// /,}       # this replaces spaces with commas, likely we do not have spaces in hostnames
    if [ -n "$minion_list" ] ; then
        check_tuned $role ${minions[@]}
        salt -L "$minion_list" service.restart tuned.service
        check_tuned $role ${minions[@]}
    fi
done
