D=/tmp/stampsdir

define get_ip
  $(shell openstack server show -f json ${1} | jq '.[] | select(.Field == "addresses") | .Value' | perl -pe 's/.*?=([\d\.]+).*/$$1/')
endef

MY_IP=$(shell hostname -I | cut -f1 -d' ')

$HOME/.ssh_agent:
	ssh-agent -s > $HOME/.ssh_agent
	source $HOME/.ssh_agent ; ssh-add
	grep -q ssh_agent ~/.bashrc_teuthology || echo 'source $HOME/.ssh_agent' >> ~/.bashrc_teuthology

${D}/packages-repository:
        openstack server create --image 'teuthology-ubuntu-14.04' --flavor 'vps-ssd-1' --key-name teuthology --security-group teuthology --property ownedby=${MY_IP} --wait $@ ; sleep 30
        ip=$(call get_ip,$@)
        ssh $$ip sudo apt-get install -y nginx
        ssh $$ip sudo chown -R ubuntu /usr/share/nginx/html
        perl -pi -e "s/gitbuilder_host.*/gitbuilder_host: $$ip/" ~/.teuthology.yaml
        mkdir -p ${@D} ; $touch $@

FLAVOR=vps-ssd-1

${D}/packages-ceph-deb-${CEPH_DIST}-${CEPH_ARCH}-basic/${CEPH_REF}: ${D}/packages-repository
        openstack server create --image 'teuthology-${CEPH_OS_TYPE}-${CEPH_OS_VERSION}' --flavor ${FLAVOR} --key-name teuthology --security-group teuthology --property ownedby=${MY_IP} --wait $@ ; sleep 30
        ip=$(call get_ip,$@)
        ssh $$ip sudo apt-get install -y git
        scp make-deb.sh $$ip:
        ssh -A $$ip make-debs.sh /opt/release $(call get_ip,$<)
	openstack server delete $@
        mkdir -p ${@D} ; $touch $@
