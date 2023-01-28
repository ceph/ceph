#!/bin/sh -e

TEMPDIR=$(mktemp -d)
BASEDIR=$(dirname "$0")

jsonnet -J vendor -m ${TEMPDIR} $BASEDIR/dashboards.jsonnet

truncate -s 0 ${TEMPDIR}/json_difference.log
for file in ${BASEDIR}/dashboards_out/*.json
do
    file_name="$(basename $file)"
    for generated_file in ${TEMPDIR}/*.json
    do
        generated_file_name="$(basename $generated_file)"
        if [ "$file_name" == "$generated_file_name" ]; then
            jsondiff --indent 2 "${generated_file}" "${file}" \
                | tee -a ${TEMPDIR}/json_difference.log
        fi
    done
done

jsonnet -J vendor -S alerts.jsonnet -o ${TEMPDIR}/prometheus_alerts.yml
jsondiff --indent 2 "prometheus_alerts.yml" "${TEMPDIR}/prometheus_alerts.yml" \
    | tee -a ${TEMPDIR}/json_difference.log

err=0
if [ $(wc -l < ${TEMPDIR}/json_difference.log) -eq 0 ]
then
    rm -rf ${TEMPDIR}
    echo "Congratulations! Grafonnet Check Passed"
else
    rm -rf ${TEMPDIR}
    echo "Grafonnet Check Failed, failed comparing generated file with existing"
    exit 1
fi
