
GRAFANA_VERSION ?= 9.4.12-1
PIECHART_VERSION ?= "1.6.2"
STATUS_PANEL_VERSION ?= "1.0.11"
DASHBOARD_DIR := "../../ceph-mixin/dashboards_out"
DASHBOARD_PROVISIONING := "ceph-dashboard.yml"
IMAGE := "docker.io/redhat/ubi8:8.5"
PKGMGR := "dnf"
GF_CONFIG := "/etc/grafana/grafana.ini"
# clip off "-<whatever> from the end of GRAFANA_VERSION
CONTAINER_VERSION := $(shell /bin/echo $(GRAFANA_VERSION) | /bin/sed 's/-.*//')

ARCH ?= x86_64
ifeq "$(ARCH)" "arm64"
	override ARCH := aarch64
endif

LOCALTAG=ceph-grafana:$(CONTAINER_VERSION)-$(ARCH)
TAG=ceph/ceph-grafana:$(CONTAINER_VERSION)-$(ARCH)

# Build a grafana instance - preconfigured for use within Ceph's dashboard UI

build :
	echo "Creating base container"
	$(eval CONTAINER := $(shell sudo buildah from ${IMAGE}))
	# Using upstream grafana build
	curl -fLO https://dl.grafana.com/oss/release/grafana-${GRAFANA_VERSION}.${ARCH}.rpm
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
	sudo buildah commit --format docker --squash $(CONTAINER) $(LOCALTAG)

push:
	sudo podman tag ${LOCALTAG} quay.io/${TAG}
	# sudo podman has issues with auth.json; just override it
	sudo podman login --authfile=auth.json -u $(CONTAINER_REPO_USERNAME) -p $(CONTAINER_REPO_PASSWORD) quay.io
	sudo podman push --authfile=auth.json --format v2s2 quay.io/${TAG}

clean:
	sudo podman rmi ${LOCALTAG} || true
	sudo podman rmi quay.io/${TAG} || true
	rm -f grafana-*.rpm* auth.json
	rm -f ${DASHBOARD_PROVISIONING}
