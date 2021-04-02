#!/bin/bash -ex

# The main point of these tests beyond ceph_test_libcephsqlite is to:
#
# - Ensure you can load the Ceph VFS via the dynamic load extension mechanism
#   in SQLite.
# - Check the behavior of a dead application, that it does not hold locks
#   indefinitely.

pool="$1"
ns="$(basename $0)"

function sqlite {
  background="$1"
  if [ "$background" = b ]; then
    shift
  fi
  a=$(cat)
  printf "%s" "$a" >&2
  # We're doing job control gymnastics here to make sure that sqlite3 is the
  # main process (i.e. the process group leader) in the background, not a bash
  # function or job pipeline.
  sqlite3 -cmd '.output /dev/null' -cmd '.load libcephsqlite.so' -cmd 'pragma journal_mode = PERSIST' -cmd ".open file:///$pool:$ns/baz.db?vfs=ceph" -cmd '.output stdout' <<<"$a" &
  if [ "$background" != b ]; then
    wait
  fi
}

function striper {
  rados --pool=$pool --namespace="$ns" --striper "$@"
}

function repeat {
  n=$1
  shift
  for ((i = 0; i < "$n"; ++i)); do
    echo "$*"
  done
}

striper rm baz.db || true

time sqlite <<EOF
create table if not exists foo (a INT);
insert into foo (a) values (RANDOM());
drop table foo;
EOF

striper stat baz.db
striper rm baz.db

time sqlite <<EOF
CREATE TABLE IF NOT EXISTS rand(text BLOB NOT NULL);
$(repeat 10 'INSERT INTO rand (text) VALUES (RANDOMBLOB(4096));')
SELECT LENGTH(text) FROM rand;
DROP TABLE rand;
EOF

time sqlite <<EOF
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS rand(text BLOB NOT NULL);
$(repeat 100 'INSERT INTO rand (text) VALUES (RANDOMBLOB(4096));')
COMMIT;
SELECT LENGTH(text) FROM rand;
DROP TABLE rand;
EOF

# Connection death drops the lock:

striper rm baz.db
date
sqlite b <<EOF
CREATE TABLE foo (a BLOB);
INSERT INTO foo VALUES ("start");
WITH RECURSIVE c(x) AS
  (
   VALUES(1)
  UNION ALL
   SELECT x+1
   FROM c
  )
INSERT INTO foo (a)
  SELECT RANDOMBLOB(1<<20)
  FROM c
  LIMIT (1<<20);
EOF

# Let it chew on that INSERT for a while so it writes data, it will not finish as it's trying to write 2^40 bytes...
sleep 10
echo done

jobs -l
kill -KILL -- $(jobs -p)
date
wait
date

n=$(sqlite <<<"SELECT COUNT(*) FROM foo;")
[ "$n" -eq 1 ]

# Connection "hang" loses the lock and cannot reacquire it:

striper rm baz.db
date
sqlite b <<EOF
CREATE TABLE foo (a BLOB);
INSERT INTO foo VALUES ("start");
WITH RECURSIVE c(x) AS
  (
   VALUES(1)
  UNION ALL
   SELECT x+1
   FROM c
  )
INSERT INTO foo (a)
  SELECT RANDOMBLOB(1<<20)
  FROM c
  LIMIT (1<<20);
EOF

# Same thing, let it chew on the INSERT for a while...
sleep 20
jobs -l
kill -STOP -- $(jobs -p)
# cephsqlite_lock_renewal_timeout is 30s
sleep 45
date
kill -CONT -- $(jobs -p)
sleep 10
date
# it should exit with an error as it lost the lock
wait
date

n=$(sqlite <<<"SELECT COUNT(*) FROM foo;")
[ "$n" -eq 1 ]
