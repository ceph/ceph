#!/usr/bin/env bash

set -e
TEMPDIR=`mktemp -d`
BASEDIR=$(dirname "$0")

JSONNET_PATH="${GRAFONNET_PATH}" jsonnet -m ${TEMPDIR} $BASEDIR/jsonnet/grafana_dashboards.jsonnet

truncate -s 0 ${TEMPDIR}/json_difference.log
for json_files in $BASEDIR/*.json
do
    JSON_FILE_NAME=$(basename $json_files)
    for generated_files in ${TEMPDIR}/*.json
    do
        GENERATED_FILE_NAME=$(basename $generated_files)
        if [ $JSON_FILE_NAME == $GENERATED_FILE_NAME ]; then
            jsondiff --indent 2 $generated_files $json_files | tee -a ${TEMPDIR}/json_difference.log
        fi
    done
done

if [[ $(wc -l < ${TEMPDIR}/json_difference.log) -eq 0 ]]
then
    rm -rf ${TEMPDIR}
    echo "Congratulations! Grafonnet Check Passed"
else
    rm -rf ${TEMPDIR}
    echo "Grafonnet Check Failed, failed comparing generated file with existing"
    exit 1
fi
