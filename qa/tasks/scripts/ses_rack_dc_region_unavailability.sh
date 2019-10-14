set -ex

master=$(hostname -f)

# split nodes
function split_nodes(){
    number_of_nodes=$1
    first_part=$(($number_of_nodes / 2))
    second_part=$(($number_of_nodes - $first_part))
}

# wait for cluster health OK
function cluster_health(){
    until [ "$(ceph health)" == HEALTH_OK ]
    do
        sleep 30
    done
}

function iptables_drop() {
    salt ${1} cmd.run "iptables -I OUTPUT -d localhost -j ACCEPT"
    salt ${1} cmd.run "iptables -I OUTPUT -d $master -j ACCEPT"
    salt ${1} cmd.run "iptables -I INPUT -s localhost -j ACCEPT"
    salt ${1} cmd.run "iptables -I INPUT -s $master -j ACCEPT"
    salt ${1} cmd.run "iptables -P INPUT DROP"
    salt ${1} cmd.run "iptables -P OUTPUT DROP"
}

function iptables_accept() {
    salt ${1} cmd.run "iptables -P INPUT ACCEPT"
    salt ${1} cmd.run "iptables -P OUTPUT ACCEPT"
    salt ${1} cmd.run "iptables -F"
}

function wait_until_down() {
    until ceph -s | grep ".* ${1}.* down"
    do
        sleep 30
    done
    ceph -s 
    ceph osd tree
}

crushmap_file=crushmap
ceph osd getcrushmap -o ${crushmap_file}.bin
crushtool -d ${crushmap_file}.bin -o ${crushmap_file}.txt

hosts=($(grep ^host ${crushmap_file}.txt | awk '{print $2}' | sort -u))
root_name=$(grep ^root ${crushmap_file}.txt | awk '{print $2}')

# exit 1 if storage nodes are less then 4
if [ ${#hosts[@]} -lt 4 ]
then
    echo "Too few nodes with storage role. Minimum is 4."
    exit 1
fi

### rack failure
for i in rack{1..4}
do
    ceph osd crush add-bucket $i rack
    ceph osd crush move $i root=$root_name
done

### region 1
split_nodes ${#hosts[@]}

# nodes for region1
for region1 in $(seq 0 $(($first_part - 1)))
do
    region1_hosts+=(${hosts[$region1]})
done

# split region1 nodes to racks
split_nodes ${#region1_hosts[@]}

# nodes for rack1 in region1
for rack1 in $(seq 0 $(($first_part - 1)))
do
    rack1_hosts+=(${region1_hosts[$rack1]})
done

# nodes for rack2 in region1
for rack2 in $(seq 1 $second_part)
do
    rack2_hosts+=(${region1_hosts[-$rack2]})
done

# move nodes in crush map to rack1 (region1)
for osd_node in ${rack1_hosts[@]}
do
    ceph osd crush move $osd_node rack=rack1
done
 
# move nodes in crush map to rack2 (region1)
for osd_node in ${rack2_hosts[@]}
do
    ceph osd crush move $osd_node rack=rack2
done
 


# region2
split_nodes ${#hosts[@]}

# nodes for region2
for region2 in $(seq 1 $second_part)
do
    region2_hosts+=(${hosts[-$region2]})
done

# split region2 nodes to racks
split_nodes ${#region2_hosts[@]}

# nodes for rack3 in region2
for rack3 in $(seq 0 $(($first_part - 1)))
do
    rack3_hosts+=(${region2_hosts[$rack3]})
done

# nodes for rack4 in region2
for rack4 in $(seq 1 $second_part)
do
    rack4_hosts+=(${region2_hosts[-$rack4]})
done

for osd_node in ${rack3_hosts[@]}
do
    ceph osd crush move $osd_node rack=rack3
done
 
for osd_node in ${rack4_hosts[@]}
do
    ceph osd crush move $osd_node rack=rack4
done
 
# bring down rack
for node2fail in ${rack4_hosts[@]}
do
    iptables_drop ${node2fail}.teuthology
done

wait_until_down "rack"

# bring rack up
for node2fail in ${rack4_hosts[@]}
do
    iptables_accept ${node2fail}.teuthology
done

cluster_health

### DC failure
ceph osd crush add-bucket dc1 datacenter
ceph osd crush add-bucket dc2 datacenter
ceph osd crush move dc1 root=$root_name
ceph osd crush move dc2 root=$root_name
ceph osd crush move rack1 datacenter=dc1
ceph osd crush move rack2 datacenter=dc1
ceph osd crush move rack3 datacenter=dc2
ceph osd crush move rack4 datacenter=dc2

dc1_nodes=(${rack1_hosts[@]} ${rack2_hosts[@]})
dc2_nodes=(${rack3_hosts[@]} ${rack4_hosts[@]})

# bringing down DC
for node2fail in ${dc1_nodes[@]}
do
    iptables_drop ${node2fail}.teuthology
done

wait_until_down "datacenter"

# bring DC up
for node2fail in ${dc1_nodes[@]}
do
    iptables_accept ${node2fail}.teuthology
done

cluster_health

### region failure
ceph osd crush add-bucket dc3 datacenter
ceph osd crush add-bucket dc4 datacenter
ceph osd crush add-bucket region1 region
ceph osd crush add-bucket region2 region
ceph osd crush move region1 root=$root_name
ceph osd crush move region2 root=$root_name
ceph osd crush move dc1 region=region1
ceph osd crush move dc2 region=region1
ceph osd crush move dc3 region=region2
ceph osd crush move dc4 region=region2
ceph osd crush move rack2 datacenter=dc2
ceph osd crush move rack3 datacenter=dc3
ceph osd crush move rack4 datacenter=dc4

region1_nodes=(${rack1_hosts[@]} ${rack2_hosts[@]})
region2_nodes=(${rack3_hosts[@]} ${rack4_hosts[@]})

# bringing down region
for node2fail in ${region1_nodes[@]}
do
    iptables_drop ${node2fail}.teuthology
done

wait_until_down "region"

# bring region up
for node2fail in ${region1_nodes[@]}
do
    iptables_accept ${node2fail}.teuthology
done

cluster_health

# remove pool
ceph osd pool rm crushmap crushmap --yes-i-really-really-mean-it

# set back default crushmap
ceph osd setcrushmap -i ${crushmap_file}.bin

ceph osd crush tree

cluster_health

rm -f ${crushmap_file}.{txt,bin}
