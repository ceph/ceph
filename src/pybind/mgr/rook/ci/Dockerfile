FROM quay.ceph.io/ceph-ci/ceph:163b023c76c18fb6af07ce28b1585de014ab9ce0
COPY ./tmp_build/orchestrator /usr/share/ceph/mgr/orchestrator
COPY ./tmp_build/rook /usr/share/ceph/mgr/rook
COPY ./tmp_build/ceph/ /tmp/python-common/ceph/
RUN cp -a /tmp/python-common/ceph/. "$(python3 -c 'import ceph, os; print(os.path.dirname(ceph.__file__))')/" && rm -rf /tmp/python-common
COPY ./tmp_build/*.py /usr/share/ceph/mgr/
COPY ./tmp_build/dashboard/cli.py /usr/share/ceph/mgr/dashboard/cli.py
COPY ./tmp_build/dashboard_plugins/plugin.py /usr/share/ceph/mgr/dashboard/plugins/plugin.py
