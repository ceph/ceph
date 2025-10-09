  $ ceph osd pool create xrbddiff1
  pool 'xrbddiff1' created
  $ rbd pool init xrbddiff1
  $ rbd create --thick-provision --size 1M xrbddiff1/xtestdiff1 --no-progress
  $ rbd diff xrbddiff1/xtestdiff1 --format json
  [{"offset":0,"length":1048576,"exists":"true"}]
  $ rbd rm xrbddiff1/xtestdiff1 --no-progress
  $ rbd create --size 1M xrbddiff1/xtestdiff1
  $ rbd diff xrbddiff1/xtestdiff1 --format json
  []
  $ rbd snap create xrbddiff1/xtestdiff1 --snap=allzeroes --no-progress
  $ rbd diff xrbddiff1/xtestdiff1 --format json
  []
  $ rbd diff --from-snap=allzeroes xrbddiff1/xtestdiff1 --format json
  []
  $ rbd bench --io-type write --io-size 1M --io-total 1M xrbddiff1/xtestdiff1 > /dev/null 2>&1
  $ rbd diff xrbddiff1/xtestdiff1 --format json
  [{"offset":0,"length":1048576,"exists":"true"}]
  $ rbd diff --from-snap=allzeroes xrbddiff1/xtestdiff1 --format json
  [{"offset":0,"length":1048576,"exists":"true"}]
  $ rbd snap create xrbddiff1/xtestdiff1 --snap=snap1 --no-progress
  $ rbd snap list xrbddiff1/xtestdiff1 --format json | python3 -mjson.tool --sort-keys | sed 's/,$/, /'
  [
      {
          "id": *,  (glob)
          "name": "allzeroes", 
          "protected": "false", 
          "size": 1048576, 
          "timestamp": * (glob)
      }, 
      {
          "id": *,  (glob)
          "name": "snap1", 
          "protected": "false", 
          "size": 1048576, 
          "timestamp": * (glob)
      }
  ]
  $ rbd diff --from-snap=snap1 xrbddiff1/xtestdiff1 --format json
  []
  $ rbd snap rollback xrbddiff1/xtestdiff1@snap1 --no-progress
  $ rbd diff --from-snap=allzeroes xrbddiff1/xtestdiff1 --format json
  [{"offset":0,"length":1048576,"exists":"true"}]
  $ rbd diff --from-snap=snap1 xrbddiff1/xtestdiff1 --format json
  []
  $ rbd snap rollback xrbddiff1/xtestdiff1@allzeroes --no-progress
  $ rbd diff --from-snap=allzeroes xrbddiff1/xtestdiff1 --format json
  []
  $ rbd diff --from-snap=snap1 xrbddiff1/xtestdiff1 --format json
  [{"offset":0,"length":1048576,"exists":"false"}]
  $ ceph osd pool rm xrbddiff1 xrbddiff1 --yes-i-really-really-mean-it
  pool 'xrbddiff1' removed
