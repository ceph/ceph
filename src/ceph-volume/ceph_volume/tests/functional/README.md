# ceph-volume functional test suite

This test suite is based on vagrant and is normally run via Jenkins on github
PRs. With a functioning Vagrant installation these test can also be run locally
(tested with vagrant's libvirt provider).

## Vagrant with libvirt
By default the tests make assumption on the network segments to use (public and
cluster network), as well as the libvirt storage pool and uri. In an unused
vagrant setup these defaults should be fine.
If you prefer to explicitly configure the storage pool and libvirt
uri, create a file
`$ceph_repo/src/ceph-volume/ceph_volume/tests/functional/global_vagrant_variables.yml`
with content as follows:
``` yaml
libvirt_uri: qemu:///system
libvirt_storage_pool: 'vagrant-ceph-nvme'
```
Adjust the values as needed.

After this descend into a test directory (e.g.
`$ceph_repo/src/ceph-volume/ceph_volume/tests/functional/lvm` and run `tox -vre
centos7-bluestore-create -- --provider=libvirt` to execute the tests in
`$ceph_repo/src/ceph-volume/ceph_volume/tests/functional/lvm/centos7/bluestore/create/`
