set -ex
ceph osd pool ls | grep rgw | xargs -I {} ceph osd pool rm {} {} --yes-i-really-really-mean-it
