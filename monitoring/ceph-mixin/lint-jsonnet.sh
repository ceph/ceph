#!/bin/sh -e

JSONNETS_FILES=$(find . -name 'vendor' -prune -o \
	-name '*.jsonnet' -print -o -name '*.libsonnet' -print)
for each_jsonnet_file in ${JSONNETS_FILES}; do
	jsonnetfmt "$@" ${each_jsonnet_file} || jfmt_failed_files="$jfmt_failed_files ${each_jsonnet_file}"
done
exit_status=0
# if variable 'jfmt_failed_files' is not empty/null
if [ -n "${jfmt_failed_files}" ]; then
	echo "'jsonnetfmt' check failed on:${jfmt_failed_files}"
	exit_status=1
fi
exit $exit_status
