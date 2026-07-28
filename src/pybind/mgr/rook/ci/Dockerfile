FROM quay.io/ceph/daemon-base:latest-main
COPY ./tmp_build/orchestrator /usr/share/ceph/mgr/orchestrator
COPY ./tmp_build/rook /usr/share/ceph/mgr/rook
COPY ./tmp_build/ceph/ /usr/lib/python3.9/site-packages/ceph/
COPY ./tmp_build/*.py /usr/share/ceph/mgr/
COPY ./tmp_build/dashboard/cli.py /usr/share/ceph/mgr/dashboard/cli.py
COPY ./tmp_build/dashboard_plugins/plugin.py /usr/share/ceph/mgr/dashboard/plugins/plugin.py
