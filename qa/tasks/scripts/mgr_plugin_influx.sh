# mgr_plugin_influx.sh
#
# Influx MGR plugin smoke test
#
# args: None

set -ex

influx -version
systemctl start influxdb.service
sleep 5
systemctl status --full --lines=0 influxdb.service
influx -execute 'create database ceph'
influx -database ceph \
    -execute "create user admin with password 'badpassword' with all privileges"
ceph mgr module enable influx
ceph influx config-set hostname $(hostname)
ceph influx config-set port 8086
ceph influx config-set username admin
ceph influx config-set password badpassword
ceph influx config-set database ceph
ceph influx config-set ssl false
ceph influx config-set verify_ssl false
ceph influx send
sleep 5
ceph -s
sleep 5
influx -database ceph -execute 'show series' | head -n1 | grep key
influx -database ceph -execute 'select * from ceph_daemon_stats limit 1' | \
    head -n1 | grep ceph_daemon_stats
echo "OK" >/dev/null
