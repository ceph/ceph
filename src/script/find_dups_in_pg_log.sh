#!/bin/sh

# pipe output of grep for objectname in osd logs to me
#
# e.g.,
#
#  zgrep smithi01817880-936 remote/*/log/*osd* | ~/src/ceph/src/script/find_dups_in_pg_log.sh
#
# or
#
#  zcat remote/*/log/*osd* | ~/src/ceph/src/script/find_dups_in_pg_log.sh
#
# output will be any requests that appear in the pg log >1 time (along with
# their count)

#grep append_log | sort -k 2 | sed 's/.*append_log//' | awk '{print $3 " " $8}' | sort | uniq | awk '{print $2}' | sort | uniq -c | grep -v ' 1 '

grep append_log | grep ' by ' | \
        perl -pe 's/(.*) \[([^ ]*) (.*) by ([^ ]+) (.*)/$2 $4/' | \
        sort | uniq | \
        awk '{print $2}' | \
        sort | uniq -c | grep -v ' 1 '
