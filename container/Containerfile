FROM quay.io/centos/centos:stream8

# The name of the Ceph release.
# e.g., "main", "reef", "qincy"
ARG CEPH_VERSION_NAME="main"

# (optional) Choose a specific Ceph package version to install
# This value is appended directly to ceph packages to select their version
# e.g., "-18.2.0"
ARG CEPH_PACKAGE_VERSION=

# (optional) Choose a specific NFS-Ganesha package version to install
# This value is appended directly to nfs-ganesha packages to select their version
# e.g., "-5.5-1.el8s"
ARG GANESHA_PACKAGE_VERSION=

# (optional) Used as URL "ref" for selecting Shaman packages.
# Will force use of shaman packages if set to something other than CEPH_VERSION_NAME.
# e.g., "main", "reef", "wip-feature"
ARG CEPH_REF="${CEPH_VERSION_NAME}"

# (optional) Set this to select a specific Ceph Shaman SHA1 build directly.
ARG CEPH_SHA1=

# (optional) Set to "crimson" to install crimson packages.
ARG OSD_FLAVOR="default"


# Other labels are set automatically by container/build github action
# See: https://github.com/opencontainers/image-spec/blob/main/annotations.md
LABEL org.opencontainers.image.authors="Guillaume Abrioux <gabrioux@redhat.com>" \
      org.opencontainers.image.documentation="https://docs.ceph.com/"


#===================================================================================================
# Install ceph and dependencies, and clean up
# IMPORTANT: in official builds, use '--squash' build option to keep image as small as possible
#   keeping run steps separate makes local rebuilds quick, but images are big without squash option
#===================================================================================================

# Pre-reqs
RUN dnf install -y --setopt=install_weak_deps=False epel-release jq

# Add NFS-Ganesha repo
RUN \
    echo "[ganesha]" > /etc/yum.repos.d/ganesha.repo && \
    echo "name=ganesha" >> /etc/yum.repos.d/ganesha.repo && \
    echo "baseurl=http://mirror.centos.org/centos/8-stream/storage/\$basearch/nfsganesha-5/" >> /etc/yum.repos.d/ganesha.repo && \
    echo "gpgcheck=0" >> /etc/yum.repos.d/ganesha.repo && \
    echo "enabled=1" >> /etc/yum.repos.d/ganesha.repo

# ISCSI repo
RUN set -x && \
    curl -s -L https://shaman.ceph.com/api/repos/tcmu-runner/main/latest/centos/8/repo?arch=$(arch) -o /etc/yum.repos.d/tcmu-runner.repo && \
    case "${CEPH_VERSION_NAME}" in \
        main) \
            curl -s -L https://shaman.ceph.com/api/repos/ceph-iscsi/main/latest/centos/8/repo -o /etc/yum.repos.d/ceph-iscsi.repo ;\
            ;;\
        quincy|reef) \
            curl -s -L https://download.ceph.com/ceph-iscsi/3/rpm/el8/ceph-iscsi.repo -o /etc/yum.repos.d/ceph-iscsi.repo ;\
            ;;\
    esac

# Ceph repo
RUN set -x && \
    rpm --import 'https://download.ceph.com/keys/release.asc' && \
    ARCH=$(arch); if [ "${ARCH}" == "aarch64" ]; then ARCH="arm64"; fi ;\
    IS_RELEASE=0 ;\
    if [ -n "${CEPH_SHA1}" ]; then \
        REPO_URL=$(curl -s "https://shaman.ceph.com/api/search/?project=ceph&distros=centos/8/${ARCH}&sha1=${CEPH_SHA1}" | jq -r .[0].url ) ;\
    elif [ "${CEPH_VERSION_NAME}" == "main" ] || [ "${CEPH_VERSION_NAME}" != "${CEPH_REF}" ]; then \
        # TODO: this can return different ceph builds (SHA1) for x86 vs. arm runs. is it important to fix?
        REPO_URL=$(curl -s "https://shaman.ceph.com/api/search/?project=ceph&distros=centos/8/${ARCH}&flavor=${OSD_FLAVOR}&ref=${CEPH_REF}&sha1=latest" | jq -r .[0].url) ;\
    else \
        IS_RELEASE=1 ;\
        REPO_URL="http://download.ceph.com/rpm-${CEPH_VERSION_NAME}/el8/" ;\
    fi && \
    rpm -Uvh "$REPO_URL/noarch/ceph-release-1-${IS_RELEASE}.el8.noarch.rpm"

# Copr repos
# scikit for mgr-diskpredition-local, asyncssh for cephadm, but why do we
# ref: https://github.com/ceph/ceph-container/pull/1821
RUN \
    dnf install -y --setopt=install_weak_deps=False dnf-plugins-core && \
    dnf copr enable -y tchaikov/python-scikit-learn && \
    dnf copr enable -y tchaikov/python3-asyncssh

# Update package mgr
RUN dnf update -y --setopt=install_weak_deps=False

# Define and install packages
RUN \
    # General
    PACKAGES="ca-certificates" && \
    # Ceph
    # TODO: remove lua-devel and luarocks once they are present in ceph.spec.in
    #       ref: https://github.com/ceph/ceph/pull/54575#discussion_r1401199635
    PACKAGES="$PACKAGES \
        ceph-common${CEPH_PACKAGE_VERSION} \
        ceph-exporter${CEPH_PACKAGE_VERSION} \
        ceph-grafana-dashboards${CEPH_PACKAGE_VERSION} \
        ceph-immutable-object-cache${CEPH_PACKAGE_VERSION} \
        ceph-mds${CEPH_PACKAGE_VERSION} \
        ceph-mgr-cephadm${CEPH_PACKAGE_VERSION} \
        ceph-mgr-dashboard${CEPH_PACKAGE_VERSION} \
        ceph-mgr-diskprediction-local${CEPH_PACKAGE_VERSION} \
        ceph-mgr-k8sevents${CEPH_PACKAGE_VERSION} \
        ceph-mgr-rook${CEPH_PACKAGE_VERSION} \
        ceph-mgr${CEPH_PACKAGE_VERSION} \
        ceph-mon${CEPH_PACKAGE_VERSION} \
        ceph-osd${CEPH_PACKAGE_VERSION} \
        ceph-radosgw${CEPH_PACKAGE_VERSION} lua-devel luarocks \
        ceph-volume${CEPH_PACKAGE_VERSION} \
        cephfs-mirror${CEPH_PACKAGE_VERSION} \
        cephfs-top${CEPH_PACKAGE_VERSION} \
        kmod \
        libradosstriper1${CEPH_PACKAGE_VERSION} \
        rbd-mirror${CEPH_PACKAGE_VERSION} \
        " && \
    # Optional crimson package(s)
    if [ "${OSD_FLAVOR}" == "crimson" ]; then \
        PACKAGES="$PACKAGES \
        ceph-crimson-osd${CEPH_PACKAGE_VERSION} \
        " ;\
    fi && \
    # Ceph "Recommends"
    PACKAGES="$PACKAGES \
        nvme-cli \
        python3-saml \
        smartmontools \
        " && \
    # NFS-Ganesha
    PACKAGES="$PACKAGES \
        dbus-daemon \
        nfs-ganesha-ceph${GANESHA_PACKAGE_VERSION} \
        nfs-ganesha-rados-grace${GANESHA_PACKAGE_VERSION} \
        nfs-ganesha-rados-urls${GANESHA_PACKAGE_VERSION} \
        nfs-ganesha-rgw${GANESHA_PACKAGE_VERSION} \
        nfs-ganesha${GANESHA_PACKAGE_VERSION} \
        rpcbind \
        sssd-client \
        " && \
    # ISCSI
    PACKAGES="$PACKAGES \
        ceph-iscsi \
        tcmu-runner \
        python3-rtslib \
        " && \
    # Ceph-CSI
    # TODO: coordinate with @Madhu-1 to have Ceph-CSI install these itself if unused by ceph
    #       @adk3798 does cephadm use these?
    PACKAGES="$PACKAGES \
        attr \
        ceph-fuse${CEPH_PACKAGE_VERSION} \
        rbd-nbd${CEPH_PACKAGE_VERSION} \
        " && \
    # Rook (only if packages must be in ceph container image)
    PACKAGES="$PACKAGES \
        systemd-udev \
        " && \
    # Util packages (should be kept to only utils that are truly very useful)
    # 'sgdisk' (from gdisk) is used in docs and scripts for clearing disks (could be a risk? @travisn @guits @ktdreyer ?)
    # 'ps' (from procps-ng) and 'hostname' are very valuable for debugging and CI
    # TODO: remove sg3_utils once they are moved to ceph.spec.in with libstoragemgmt
    #       ref: https://github.com/ceph/ceph-container/pull/2013#issuecomment-1248606472
    PACKAGES="$PACKAGES \
        gdisk \
        hostname \
        procps-ng \
        sg3_utils \
        " && \
    echo ${PACKAGES} > packages.txt && \
    echo "=== PACKAGES TO BE INSTALLED ===" && \
    cat packages.txt && \
    echo "=== INSTALLING ===" && \
    dnf install -y --setopt=install_weak_deps=False --enablerepo=powertools $(cat packages.txt) && \
    mkdir -p /var/run/ganesha

# Disable sync with udev since the container can not contact udev
RUN \
    sed -i -e 's/udev_rules = 1/udev_rules = 0/' \
           -e 's/udev_sync = 1/udev_sync = 0/' \
           -e 's/obtain_device_list_from_udev = 1/obtain_device_list_from_udev = 0/' \
        /etc/lvm/lvm.conf && \
    # validate the sed command worked as expected
    grep -sqo "udev_sync = 0" /etc/lvm/lvm.conf && \
    grep -sqo "udev_rules = 0" /etc/lvm/lvm.conf && \
    grep -sqo "obtain_device_list_from_udev = 0" /etc/lvm/lvm.conf

# CLEAN UP!
RUN set -x && \
    dnf clean all && \
    rm -rf /var/cache/dnf/* && \
    rm -rf /var/lib/dnf/* && \
    rm -f /var/lib/rpm/__db* && \
    # remove unnecessary files with big impact
    rm -rf /etc/selinux /usr/share/{doc,man,selinux} && \
    # don't keep compiled python binaries
    find / -xdev \( -name "*.pyc" -o -name "*.pyo" \) -delete

# Verify that the packages installed haven't been accidentally cleaned, then
# clean the package list and re-clean unnecessary RPM database files
RUN rpm -q $(cat packages.txt) && rm -f /var/lib/rpm/__db* && rm -f *packages.txt

#
# Set some envs in the container for quickly inspecting details about the build at runtime
# Should all be prefaced with CEPH_ unless obviously unrelated (like GANESHA_)
ENV CEPH_VERSION_NAME="${CEPH_VERSION_NAME}" \
    CEPH_PACKAGE_VERSION="${CEPH_PACKAGE_VERSION}" \
    GANESHA_PACKAGE_VERSION="${GANESHA_PACKAGE_VERSION}" \
    CEPH_IS_DEVEL="${IS_DEVEL}" \
    CEPH_REF="${CEPH_REF}" \
    CEPH_OSD_FLAVOR="${CEPH_OSD_FLAVOR}"
