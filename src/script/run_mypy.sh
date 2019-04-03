#!/usr/bin/env bash

# needs to be executed from the src directory.
# generates a report at src/mypy_report.txt

python3 -m venv .mypy_venv

. .mypy_venv/bin/activate

pip install $(find * -name requirements.txt | grep -v node_modules | awk '{print "-r  " $0}')
pip install mypy

MYPY_INI=$(cat <<-EOF
[mypy]
strict_optional = True
no_implicit_optional = True
ignore_missing_imports = True
warn_incomplete_stub = True
check_untyped_defs = True
show_error_context = True
EOF
)

export MYPYPATH="$PWD/pybind/rados:$PWD/pybind/rbd:$PWD/pybind/cephfs"

echo "pybind:" > mypy_report.txt
pushd pybind
mypy --config-file=<(echo "$MYPY_INI")  *.py | awk '{print "pybind/" $0}' >> ../mypy_report.txt
popd

echo "MGR Modules:" >> mypy_report.txt
pushd pybind/mgr
mypy --config-file=<(echo "$MYPY_INI")  $(find * -name '*.py' | grep -v -e venv -e tox -e env -e gyp -e node_modules) | awk '{print "pybind/mgr/" $0}' >> ../../mypy_report.txt
popd

echo "ceph-volume:" >> mypy_report.txt
pushd ceph-volume/ceph_volume
mypy --config-file=<(echo "$MYPY_INI")   $(find * -name '*.py' | grep -v -e venv -e tox -e env -e gyp -e node_modules -e tests) | awk '{print "ceph-volume/ceph_volume/" $0}' >> ../../mypy_report.txt
popd

