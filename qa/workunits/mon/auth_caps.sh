#!/bin/bash

set -e
set -x
declare -A keymap

combinations="r w x rw rx wx rwx"

for i in ${combinations}; do
  k="foo_$i"
  k=`ceph auth get-or-create-key client.$i mon "allow $i"` || exit 1
  keymap["$i"]=$k
done

# add special caps
# force blank cap with '--force'
keymap["blank"]=`ceph auth get-or-create-key client.blank mon 'allow' --force` || exit 1
keymap["all"]=`ceph auth get-or-create-key client.all mon 'allow *'` || exit 1

tmp=`mktemp`
ceph auth export > $tmp

trap "rm $tmp" INT ERR EXIT QUIT 0

expect() {

  set +e

  local expected_ret=$1
  local ret

  shift
  cmd=$@

  eval $cmd
  ret=$?

  set -e

  if [[ $ret -ne $expected_ret ]]; then
    echo "ERROR: running \'$cmd\': expected $expected_ret got $ret"
    return 1
  fi

  return 0
}

read_ops() {
  local caps=$1
  local has_read=1 has_exec=1
  local ret
  local args

  ( echo $caps | grep 'r' ) || has_read=0
  ( echo $caps | grep 'x' ) || has_exec=0
  
  if [[ "$caps" == "all" ]]; then
    has_read=1
    has_exec=1
  fi

  ret=13
  if [[ $has_read -gt 0 && $has_exec -gt 0 ]]; then
    ret=0
  fi

  args="--id $caps --key ${keymap[$caps]}"
 
  expect $ret ceph auth get client.admin $args
  expect $ret ceph auth get-key client.admin $args
  expect $ret ceph auth export $args
  expect $ret ceph auth export client.admin $args
  expect $ret ceph auth list $args
  expect $ret ceph auth print-key client.admin $args
  expect $ret ceph auth print_key client.admin $args
}

write_ops() {

  local caps=$1
  local has_read=1 has_write=1 has_exec=1
  local ret
  local err
  local args

  ( echo $caps | grep 'r' ) || has_read=0
  ( echo $caps | grep 'w' ) || has_write=0
  ( echo $caps | grep 'x' ) || has_exec=0

  if [[ "$caps" == "all" ]]; then
    has_read=1
    has_write=1
    has_exec=1
  fi

  ret=13
  if [[ $has_read -gt 0 && $has_write -gt 0 && $has_exec -gt 0 ]]; then
    ret=0
  fi

  args="--id $caps --key ${keymap[$caps]}"

  expect $ret ceph auth add client.foo $args
  expect $ret "ceph auth caps client.foo mon 'allow *' $args"
  expect $ret ceph auth get-or-create client.admin $args
  echo "wtf -- before: err=$err ret=$ret"
  err=$ret
  [[ $ret -eq 0 ]] && err=22 # EINVAL
  expect $err "ceph auth get-or-create client.bar mon 'allow' $args"
  echo "wtf -- after: err=$err ret=$ret"
  expect $ret "ceph auth get-or-create client.bar mon 'allow' --force $args"
  expect $ret ceph auth get-or-create-key client.admin $args
  expect $ret ceph auth get-or-create-key client.baz $args
  expect $ret ceph auth del client.bar $args
  expect $ret ceph auth del client.baz $args
  expect $ret ceph auth del client.foo $args
  expect $ret ceph auth import -i $tmp $args
}

echo "running combinations: ${!keymap[@]}"

subcmd=$1

for i in ${!keymap[@]}; do
  echo "caps: $i"
  if [[ -z "$subcmd" || "$subcmd" == "read" || "$subcmd" == "all" ]]; then
    read_ops $i
  fi

  if [[ -z "$subcmd" || "$subcmd" == "write" || "$subcmd" == "all" ]]; then
    write_ops $i
  fi
done

# cleanup
for i in ${combinations} blank all; do
  ceph auth del client.$i || exit 1
done

echo "OK"
