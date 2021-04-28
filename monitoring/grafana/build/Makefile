
GRAFANA_VERSION := 6.7.4-1
PIECHART_VERSION := "1.4.0"
STATUS_PANEL_VERSION := "1.0.9"
DASHBOARD_DIR := "../dashboards"
DASHBOARD_PROVISIONING := "ceph-dashboard.yml"
IMAGE := "docker.io/centos:8"
VERSION := "${IMAGE: -1}"
PKGMGR := "dnf"
GF_CONFIG := "/etc/grafana/grafana.ini"
ceph_version := "master"

ARCH ?= x86_64
ifeq "$(ARCH)" "arm64"
	override ARCH := aarch64
endif

# Build a grafana instance - preconfigured for use within Ceph's dashboard UI

build :
	echo "Creating base container"
	$(eval CONTAINER := $(shell sudo buildah from ${IMAGE}))
	# Using upstream grafana build
	wget https://dl.grafana.com/oss/release/grafana-${GRAFANA_VERSION}.${ARCH}.rpm
	sudo buildah copy $(CONTAINER) grafana-${GRAFANA_VERSION}.${ARCH}.rpm /tmp/grafana-${GRAFANA_VERSION}.${ARCH}.rpm
	sudo buildah run $(CONTAINER) ${PKGMGR} install -y --setopt install_weak_deps=false --setopt=tsflags=nodocs /tmp/grafana-${GRAFANA_VERSION}.${ARCH}.rpm
	sudo buildah run $(CONTAINER) ${PKGMGR} clean all
	sudo buildah run $(CONTAINER) rm -f /tmp/grafana*.rpm
	sudo buildah run $(CONTAINER) grafana-cli plugins install grafana-piechart-panel ${PIECHART_VERSION}
	sudo buildah run $(CONTAINER) grafana-cli plugins install vonage-status-panel ${STATUS_PANEL_VERSION}
	sudo buildah run $(CONTAINER) mkdir -p /etc/grafana/dashboards/ceph-dashboard
	sudo buildah copy $(CONTAINER) ${DASHBOARD_DIR}/*.json /etc/grafana/dashboards/ceph-dashboard

	@/bin/echo -e "\
apiVersion: 1 \\n\
providers: \\n\
- name: 'Ceph Dashboard' \\n\
  torgId: 1 \\n\
  folder: 'ceph-dashboard' \\n\
  type: file \\n\
  disableDeletion: false \\n\
  updateIntervalSeconds: 3 \\n\
  editable: false \\n\
  options: \\n\
    path: '/etc/grafana/dashboards/ceph-dashboard'" >> ${DASHBOARD_PROVISIONING}


	sudo buildah copy $(CONTAINER) ${DASHBOARD_PROVISIONING} /etc/grafana/provisioning/dashboards/${DASHBOARD_PROVISIONING}

	# expose tcp/3000 for grafana
	sudo buildah config --port 3000 $(CONTAINER)

	# set working dir
	sudo buildah config --workingdir /usr/share/grafana $(CONTAINER)

	# set environment overrides from the default locations in /usr/share
	sudo buildah config --env GF_PATHS_LOGS="/var/log/grafana" $(CONTAINER)
	sudo buildah config --env GF_PATHS_PLUGINS="/var/lib/grafana/plugins" $(CONTAINER)
	sudo buildah config --env GF_PATHS_PROVISIONING="/etc/grafana/provisioning" $(CONTAINER)
	sudo buildah config --env GF_PATHS_DATA="/var/lib/grafana" $(CONTAINER)

	# entrypoint
	sudo buildah config --entrypoint "grafana-server --config=${GF_CONFIG}" $(CONTAINER)

	# finalize
	sudo buildah config --label maintainer="Paul Cuzner <pcuzner@redhat.com>" $(CONTAINER)
	sudo buildah config --label description="Ceph Grafana Container" $(CONTAINER)
	sudo buildah config --label summary="Grafana Container configured for Ceph mgr/dashboard integration" $(CONTAINER)
	sudo buildah commit --format docker --squash $(CONTAINER) ceph-grafana:${ceph_version}
	sudo buildah tag ceph-grafana:${ceph_version} ceph/ceph-grafana:${ceph_version}

clean :
	rm -f grafana-*.rpm*
	rm -f ${DASHBOARD_PROVISIONING}


nautilus : 
	$(MAKE) ceph_version="nautilus" build
octopus : 
	$(MAKE) ceph_version="octopus" build
master : 
	$(MAKE) ceph_version="master" build

all : nautilus octopus master
.PHONY : all 
