#!/usr/bin/env bash

# needs to be executed form the src directory.
# generates a report at src/mypy_report.txt

python3 -m venv venv

. venv/bin/activate

pip install $(find * -name requirements.txt | awk '{print "-r  " $0}') mypy

cat <<EOF > ./mypy.ini
[mypy]
strict_optional = True
no_implicit_optional = True
ignore_missing_imports = True
warn_incomplete_stub = True
check_untyped_defs = True
show_error_context = True
EOF


echo "pybind:" > mypy_report.txt
pushd pybind
mypy --config-file=../mypy.ini  *.py | awk '{print "pybind/" $0}' >> ../mypy_report.txt
popd

echo "MGR Modules:" >> mypy_report.txt
pushd pybind/mgr
mypy --config-file=../../mypy.ini  $(find * -name '*.py' | grep -v -e venv -e tox -e env -e gyp -e node_modules) | awk '{print "pybind/mgr/" $0}' >> ../../mypy_report.txt
popd

echo "ceph-volume:" >> mypy_report.txt
pushd ceph-volume/ceph_volume
mypy --config-file=../../mypy.ini   $(find * -name '*.py' | grep -v -e venv -e tox -e env -e gyp -e node_modules -e tests) | awk '{print "ceph-volume/ceph_volume/" $0}' >> ../../mypy_report.txt
popd

