
GRAFANA_VERSION := 6.6.2-1
PIECHART_VERSION := "1.4.0"
STATUS_PANEL_VERSION := "1.0.9"
DASHBOARD_DIR := "monitoring/grafana/dashboards"
DASHBOARD_PROVISIONING := "ceph-dashboard.yml"
IMAGE := "centos:8"
VERSION := "${IMAGE: -1}"
PKGMGR := "dnf"
# CONTAINER := $(shell buildah from ${IMAGE})
GF_CONFIG := "/etc/grafana/grafana.ini"
ceph_version := "master"

# Build a grafana instance - preconfigured for use within Ceph's dashboard UI

build : fetch_dashboards
	echo "Creating base container"
	$(eval CONTAINER := $(shell buildah from ${IMAGE}))
	# Using upstream grafana build
	wget https://dl.grafana.com/oss/release/grafana-${GRAFANA_VERSION}.x86_64.rpm
	#wget localhost:8000/grafana-${GRAFANA_VERSION}.x86_64.rpm
	#cp grafana-${GRAFANA_VERSION}.x86_64.rpm ${mountpoint}/tmp/.
	buildah copy $(CONTAINER) grafana-${GRAFANA_VERSION}.x86_64.rpm /tmp/grafana-${GRAFANA_VERSION}.x86_64.rpm 
	buildah run $(CONTAINER) ${PKGMGR} install -y --setopt install_weak_deps=false --setopt=tsflags=nodocs /tmp/grafana-${GRAFANA_VERSION}.x86_64.rpm
	buildah run $(CONTAINER) ${PKGMGR} clean all
	buildah run $(CONTAINER) rm -f /tmp/grafana*.rpm
	buildah run $(CONTAINER) grafana-cli plugins install grafana-piechart-panel ${PIECHART_VERSION}
	buildah run $(CONTAINER) grafana-cli plugins install vonage-status-panel ${STATUS_PANEL_VERSION}
	buildah run $(CONTAINER) mkdir -p /etc/grafana/dashboards/ceph-dashboard
	buildah copy $(CONTAINER) jsonfiles/*.json /etc/grafana/dashboards/ceph-dashboard

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


	buildah copy $(CONTAINER) ${DASHBOARD_PROVISIONING} /etc/grafana/provisioning/dashboards/${DASHBOARD_PROVISIONING}

	# expose tcp/3000 for grafana
	buildah config --port 3000 $(CONTAINER)

	# set working dir
	buildah config --workingdir /usr/share/grafana $(CONTAINER)

	# set environment overrides from the default locations in /usr/share
	buildah config --env GF_PATHS_LOGS="/var/log/grafana" $(CONTAINER)
	buildah config --env GF_PATHS_PLUGINS="/var/lib/grafana/plugins" $(CONTAINER)
	buildah config --env GF_PATHS_PROVISIONING="/etc/grafana/provisioning" $(CONTAINER)
	buildah config --env GF_PATHS_DATA="/var/lib/grafana" $(CONTAINER)

	# entrypoint
	buildah config --entrypoint "grafana-server --config=${GF_CONFIG}" $(CONTAINER)

	# finalize
	buildah config --label maintainer="Paul Cuzner <pcuzner@redhat.com>" $(CONTAINER)
	buildah config --label description="Ceph Grafana Container" $(CONTAINER)
	buildah config --label summary="Grafana Container configured for Ceph mgr/dashboard integration" $(CONTAINER)
	buildah commit --format docker --squash $(CONTAINER) ceph-grafana:${ceph_version}
	buildah tag ceph-grafana:${ceph_version} ceph/ceph-grafana:${ceph_version}


fetch_dashboards: clean
	wget -O - https://api.github.com/repos/ceph/ceph/contents/${DASHBOARD_DIR}?ref=${ceph_version} | jq '.[].download_url' > dashboards

	# drop quotes from the list and pick out only json files
	sed -i 's/\"//g' dashboards
	sed -i '/\.json/!d' dashboards
	mkdir jsonfiles 
	while read -r line; do \
		wget "$$line" -P jsonfiles; \
	done < dashboards

clean :
	rm -f dashboards
	rm -fr jsonfiles
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
