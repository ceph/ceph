set -ex

master=$(hostname -f)
declare -a monitor_minions=("$@")

list_monitor_minions=( ${monitor_minions[@]/$master} )

if [ $((${#list_monitor_minions[@]}%2)) -eq 0 ];then
    monitors_max_down=$((${#list_monitor_minions[@]}/2-1))
else
    monitors_max_down=$((${#list_monitor_minions[@]}/2))
fi

for mon2fail in $(seq 1 $monitors_max_down)
do
    mon2fail_fqdn=${list_monitor_minions[$mon2fail]}
    mon2fail=${mon2fail_fqdn%%.*}
    salt $mon2fail_fqdn service.stop ceph-mon@${mon2fail}.service
    salt $mon2fail_fqdn service.status ceph-mon@${mon2fail}.service
    sleep 120
    ceph health detail --format=json | jq -r .checks.MON_DOWN.detail[].message
    stopped_minions+="$mon2fail_fqdn "
done
 
for mon2start_fqdn in $stopped_minions
do
    mon2start=${mon2start_fqdn%%.*}
    salt $mon2start_fqdn service.start ceph-mon@${mon2start}.service
    salt $mon2start_fqdn service.status ceph-mon@${mon2start}.service
    sleep 45
done

for node2down in $(seq 1 $monitors_max_down)
do
    for node2block in $(echo ${list_monitor_minions[@]} | sed "s/${list_monitor_minions[$node2down]}//")
    do
        salt ${list_monitor_minions[$node2down]} cmd.run "iptables -I INPUT -s ${node2block%%.*} -j DROP" || true
        salt ${list_monitor_minions[$node2down]} cmd.run "iptables -I OUTPUT -s ${node2block%%.*} -j DROP" || true
    done
    salt ${list_monitor_minions[$node2down]} cmd.run "iptables -L INPUT" || true
    salt ${list_monitor_minions[$node2down]} cmd.run "iptables -L OUTPUT" || true
    sleep 180
    ceph health detail --format=json | jq -r .checks.MON_DOWN.detail[].message
    nodesdown+="${list_monitor_minions[$node2down]} "
done

for node2up in $nodesdown
do
    salt $node2up cmd.run "iptables -F" || true
done

sleep 90

ceph health | grep HEALTH_OK
