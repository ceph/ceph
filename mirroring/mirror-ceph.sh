#!/bin/bash
set -e
#
# Script to mirror Ceph locally
#
# Please, choose a local source and do not sync in a shorter interval than
# 3 hours.
#
SILENT=0

# All available source mirrors
declare -A SOURCES
SOURCES[eu]="eu.ceph.com"
SOURCES[de]="de.ceph.com"
SOURCES[se]="se.ceph.com"
SOURCES[cz]="cz.ceph.com"
SOURCES[au]="au.ceph.com"
SOURCES[us]="download.ceph.com"
SOURCES[hk]="hk.ceph.com"
SOURCES[fr]="fr.ceph.com"
SOURCES[us-east]="us-east.ceph.com"
SOURCES[us-west]="us-west.ceph.com"
SOURCES[global]="download.ceph.com"

function print_usage() {
    echo "$0 [-q ] -s <source mirror> -t <target directory>"
}

while getopts ":qhs:t:" opt; do
    case $opt in
        q)
            SILENT=1
            ;;
        s)
            SOURCE=$OPTARG
            ;;
        t)
            TARGET=$OPTARG
            ;;
        h)
            HELP=1
            ;;
        \?)
            print_usage
            exit 1
            ;;
    esac
done

if [ ! -z "$HELP" ] || [ -z "$TARGET" ] || [ -z "$SOURCE" ]; then
    print_usage
    exit 1
fi

if [ ! -d "$TARGET" ]; then
    echo "$TARGET is not a valid target directory"
    exit 1
fi

for i in "${!SOURCES[@]}"; do
    if [ "$i" == "$SOURCE" ]; then
        SOURCE_HOST=${SOURCES[$i]}
    fi
done

if [ -z "$SOURCE_HOST" ]; then
    echo -n "Please select one of the following sources:"
    for i in "${!SOURCES[@]}"; do
        echo -n " $i"
    done
    echo ""
    exit 1
fi

RSYNC_OPTS="--stats --progress"
if [ $SILENT -eq 1 ]; then
    RSYNC_OPTS="--quiet"
fi

# We start a two-stage sync here for DEB and RPM
# Based on: https://www.debian.org/mirror/ftpmirror
#
# The idea is to prevent temporary situations where metadata points to files
# which do not exist
#

# Exclude all metadata files
rsync ${RSYNC_OPTS} ${SOURCE_HOST}::ceph --recursive --times --links \
                                         --hard-links \
                                         --exclude Packages* \
                                         --exclude Sources* \
                                         --exclude Release* \
                                         --exclude InRelease \
                                         --exclude i18n/* \
                                         --exclude ls-lR* \
                                         --exclude repodata/* \
                                         ${TARGET}

# Now also transfer the metadata and delete afterwards
rsync ${RSYNC_OPTS} ${SOURCE_HOST}::ceph --recursive --times --links \
                                         --hard-links --delete-after \
                                         ${TARGET}
