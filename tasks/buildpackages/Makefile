SHELL=/bin/bash
D=/tmp/stampsdir
VPATH=${D}

define get_ip
$$(openstack server show -f json $(1) | jq '.[] | select(.Field == "addresses") | .Value' | perl -pe 's/.*?=([\d\.]+).*/$$1/')
endef

MY_IP=$(shell hostname -I | cut -f1 -d' ')

${HOME}/.ssh_agent:
	ssh-agent -s > ${HOME}/.ssh_agent
	source ${HOME}/.ssh_agent ; ssh-add ; ssh-add -l
	grep -q ssh_agent ~/.bashrc_teuthology || echo 'source ${HOME}/.ssh_agent' >> ~/.bashrc_teuthology

packages-repository:
	openstack server create --image 'teuthology-ubuntu-14.04' --flavor 'vps-ssd-1' --key-name teuthology --security-group teuthology --property ownedby=${MY_IP} --wait $@ ; sleep 30
	ip=$(call get_ip,$@) ; \
	ssh $$ip sudo apt-get install -y nginx ; \
	ssh $$ip sudo chown -R ubuntu /usr/share/nginx/html ; \
	perl -pi -e "s/^gitbuilder_host:.*/gitbuilder_host: $$ip/" ~/.teuthology.yaml
	mkdir -p ${D}/${@D} ; touch ${D}/$@

FLAVOR=vps-ssd-1

ceph-deb-${CEPH_DIST}-${CEPH_ARCH}-basic-${CEPH_SHA1}: packages-repository
	openstack server create --image 'teuthology-${CEPH_OS_TYPE}-${CEPH_OS_VERSION}' --flavor ${FLAVOR} --key-name teuthology --security-group teuthology --property ownedby=${MY_IP} --wait $@ ; sleep 30
	set -ex ; \
	ip=$(call get_ip,$@) ; \
	echo 'sudo apt-get update ; sudo apt-get install -y git ; test -d ceph || git clone ${CEPH_GIT_BASE_URL}/ceph ; cd ceph ; git checkout ${CEPH_SHA1}' | ssh $$ip bash ; \
	scp make-debs.sh $$ip: ; \
	packages_repository=$(call get_ip,${<F}) ; \
	echo 'cd ceph ; sudo chown $${USER} /opt ; sudo apt-get install -y reprepro ; git checkout origin/master install-deps.sh ; ./install-deps.sh ; ../make-debs.sh /opt/release '$$packages_repository | ssh -A $$ip bash
	openstack server delete $@
	mkdir -p ${D}/${@D} ; touch ${D}/$@

clobber:
	pkill ssh-agent || true
	rm -f ${HOME}/.ssh_agent
	rm -fr ${D}
