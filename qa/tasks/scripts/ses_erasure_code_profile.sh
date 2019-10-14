set -ex


ceph osd erasure-code-profile set EC-temp-pool
ceph osd erasure-code-profile get EC-temp-pool
ceph osd erasure-code-profile set EC-temp-pool crush-failure-domain=osd k=6 m=2 || true
ceph osd erasure-code-profile set EC-temp-pool crush-failure-domain=osd k=6 m=2 --force
ceph osd erasure-code-profile get EC-temp-pool
rados lspools
rados -p ectemppool ls
ceph osd erasure-code-profile ls
ceph osd pool rm ectemppool ectemppool --yes-i-really-really-mean-it
ceph osd erasure-code-profile rm EC-temp-pool

