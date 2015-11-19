SHELL=/bin/bash
D=/tmp/stampsdir
VPATH=${D}

define get_ip
$$(openstack server show -f json $(1) | jq '.[] | select(.Field == "addresses") | .Value' | perl -pe 's/([\da-f]*:){7}[\da-f]*, //; s/.*?=([\d\.]+).*/$$1/')
endef

MY_IP=$(shell hostname -I | cut -f1 -d' ')

${HOME}/.ssh_agent:
	ssh-agent -s > ${HOME}/.ssh_agent
	source ${HOME}/.ssh_agent ; ssh-add ; ssh-add -l
	grep -q ssh_agent ~/.bashrc_teuthology || echo 'source ${HOME}/.ssh_agent' >> ~/.bashrc_teuthology

flock-packages-repository:
	openstack server create --image 'teuthology-ubuntu-14.04' --flavor ${HTTP_FLAVOR} --key-name teuthology --security-group teuthology --property ownedby=${MY_IP} --wait packages-repository ; sleep 30
	ip=$(call get_ip,packages-repository) ; \
	ssh $$ip sudo apt-get update ; \
	ssh $$ip sudo apt-get install -y nginx ; \
	ssh $$ip sudo chown -R ubuntu /usr/share/nginx/html ; \
	perl -pi -e "s/^gitbuilder_host:.*/gitbuilder_host: $$ip/" ~/.teuthology.yaml
	touch ${D}/$@

packages-repository:
	mkdir -p ${D}
	flock --close ${D}/flock-$@.lock ${MAKE} flock-$@
	touch ${D}/$@

ceph-${CEPH_PKG_TYPE}-${CEPH_DIST}-${CEPH_ARCH}-${CEPH_FLAVOR}-${CEPH_SHA1}: packages-repository
	openstack server create --image 'teuthology-${CEPH_OS_TYPE}-${CEPH_OS_VERSION}' --flavor ${BUILD_FLAVOR} --key-name teuthology --security-group teuthology --property ownedby=${MY_IP} --user-data ${CEPH_OS_TYPE}-${CEPH_OS_VERSION}-user-data.txt --wait $@ ; sleep 30
	set -ex ; \
	trap "openstack server delete $@" EXIT ; \
	ip=$(call get_ip,$@) ; \
	for delay in 1 2 4 8 8 8 8 8 8 8 8 8 16 16 16 16 16 32 32 32 64 128 256 512 ; do if ssh -o 'ConnectTimeout=3' $$ip bash -c '"grep -q READYTORUN /var/log/cloud-init*.log"' ; then break ; else sleep $$delay ; fi ; done ; \
	scp make-${CEPH_PKG_TYPE}.sh common.sh ubuntu@$$ip: ; \
	packages_repository=$(call get_ip,${<F}) ; \
	ssh -tt -A ubuntu@$$ip bash ./make-${CEPH_PKG_TYPE}.sh $$packages_repository ${CEPH_DIST} ${CEPH_GIT_URL} ${CEPH_SHA1} ${CEPH_FLAVOR}
	mkdir -p ${D}/${@D} ; touch ${D}/$@

clobber:
	pkill ssh-agent || true
	rm -f ${HOME}/.ssh_agent
	rm -fr ${D}
