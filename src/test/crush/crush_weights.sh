#!/usr/bin/env bash

source $(dirname $0)/../detect-build-env-vars.sh

if [ `uname` = FreeBSD ]; then
    SED=gsed
else
    SED=sed
fi

read -r -d '' cm <<'EOF'
# devices
device 0 device0
device 1 device1
device 2 device2
device 3 device3
device 4 device4
# types
type 0 osd
type 1 domain
type 2 pool
# buckets
domain root {
    id -1        # do not change unnecessarily
    # weight 5.000
    alg straw2
    hash 0    # rjenkins1
    item device0 weight 10.0
    item device1 weight 10.0
    item device2 weight 10.0
    item device3 weight 10.0
    item device4 weight 1.000
}
# rules
rule data {
    ruleset 0
    type replicated
    min_size 1
    max_size 10
    step take root
    step choose firstn 0 type osd
    step emit
}
EOF

three=($(echo "$cm" | crushtool -c /dev/fd/0 --test --show-utilization \
                              --min-x 1 --max-x 1000000 --num-rep 3 | \
  grep "device \(0\|4\)" | $SED -e 's/^.*stored : \([0-9]\+\).*$/\1/'))

if test $(echo "scale=5; (10 - ${three[0]}/${three[1]}) < .75" | bc) = 1; then
    echo 3 replicas weights better distributed than they should be. 1>&2
    exit 1
fi

one=($(echo "$cm" | crushtool -c /dev/fd/0 --test --show-utilization \
                              --min-x 1 --max-x 1000000 --num-rep 1 | \
  grep "device \(0\|4\)" | $SED -e 's/^.*stored : \([0-9]\+\).*$/\1/'))

if test $(echo "scale=5; (10 - ${one[0]}/${one[1]}) > .1 || (10 - ${one[0]}/${one[1]}) < -.1" | bc) = 1; then
    echo 1 replica not distributed as they should be. 1>&2
    exit 1
fi
