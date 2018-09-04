#!/bin/bash
PROFILES="bs_dedicated_db_crypt
bs_dedicated_db_sizes_crypt
bs_dedicated_db_sizes_mixed_crypt
bs_dedicated_db_sizes_mixed
bs_dedicated_db_sizes
bs_dedicated_db
bs_dedicated_wal_crypt
bs_dedicated_wal_db_crypt
bs_dedicated_wal_db_sizes_all_crypt
bs_dedicated_wal_db_sizes_all
bs_dedicated_wal_db_sizes_mixed_crypt
bs_dedicated_wal_db_sizes_mixed
bs_dedicated_wal_db
bs_dedicated_wal_sizes_crypt
bs_dedicated_wal_sizes_mixed_crypt
bs_dedicated_wal_sizes_mixed
bs_dedicated_wal_sizes
bs_dedicated_wal
fs_dedicated_journal_crypt
fs_dedicated_journal
"

for p in $PROFILES ; do
    cat << EOF > ${p}.yaml
overrides:
  deepsea:
    exec:
    - suites/basic/health-ok.sh --cli --profile=$p
EOF
done
