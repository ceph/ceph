set -ex

declare -a minions=("$@")


stopped_osd=()
for i in ${!storage_minions[@]}
do
    minion=${storage_minions[i]%%.*}
    random_osd=$(ceph osd tree | grep -A 1 $minion | grep -o osd.* | awk '{print $1}')
    salt ${storage_minions[i]} service.stop ceph-osd@${random_osd#*.}
    sleep 5
    echo "Waiting till health is OK."
    until [ "$(ceph health)" == "HEALTH_OK" ]
    do
        let n+=30
        sleep 30
    done
    echo "Total waiting time ${n}s."
    stopped_osd+=("$random_osd")
done

for i in ${!storage_minions[@]}
do
    salt ${storage_minions[i]} service.start ceph-osd@${stopped_osd[i]#*.}
done

ceph osd pool rm stoposddeamon stoposddeamon --yes-i-really-really-mean-it

