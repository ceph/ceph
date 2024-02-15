#!/usr/bin/env bash

set -ex

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

function make_files()
{
  set +x
  temp_dir=`mktemp -d mkfile_test_XXXXXX`
  for i in $(seq 1 $1)
  do
    echo -n | dd of="${temp_dir}/file_$i" conv=fsync || return 1
    sync "${temp_dir}" || return 1
  done
  set -x
  return 0
}

function make_dirs()
{
  set +x
  temp_dir=`mktemp -d mkdir_test_XXXXXX`
  for i in $(seq 1 $1)
  do
    mkdir -p ${temp_dir}/dir_${i} || return 1
    sync "${temp_dir}" || return 1
  done
  set -x
  return 0
}

function make_nodes()
{
  set +x
  temp_dir=`mktemp -d mknod_test_XXXXXX`
  for i in $(seq 1 $1)
  do
    mknod ${temp_dir}/fifo_${i} p || return 1
    sync "${temp_dir}" || return 1
  done
  set -x
  return 0
}

function rename_files()
{
  set +x
  temp_dir=`mktemp -d rename_test_XXXXXX`
  mkdir -p ${temp_dir}/rename

  for i in $(seq 1 $1)
  do
    touch ${temp_dir}/file_${i} || return 1

    mv ${temp_dir}/file_${i} ${temp_dir}/rename/ || return 1
    sync "${temp_dir}" || return 1
  done
  set -x
  return 0
}

function make_symlinks()
{
  set +x
  temp_dir=`mktemp -d symlink_test_XXXXXX`
  mkdir -p ${temp_dir}/symlink 

  touch ${temp_dir}/file

  for i in $(seq 1 $1)
  do
    ln -s ../file ${temp_dir}/symlink/sym_${i} || return 1
    sync "${temp_dir}" || return 1
  done
  set -x
  return 0
}

function make_links()
{
  set +x
  temp_dir=`mktemp -d link_test_XXXXXX`
  mkdir -p ${temp_dir}/link 

  touch ${temp_dir}/file

  for i in $(seq 1 $1)
  do
    ln ${temp_dir}/file ${temp_dir}/link/link_${i} || return 1
    sync "${temp_dir}" || return 1
  done
  set -x
  return 0
}

function cleanup()
{
  rm -rf *
}

test_dir="max_entries"
mkdir -p $test_dir
pushd $test_dir

dir_max_entries=100
ceph config set mds mds_dir_max_entries $dir_max_entries

ok_dir_max_entries=$dir_max_entries
fail_dir_max_entries=$((dir_max_entries+1))

# make files test
make_files $ok_dir_max_entries
expect_false make_files $fail_dir_max_entries

# make dirs test
make_dirs $ok_dir_max_entries
expect_false make_dirs $fail_dir_max_entries

# make nodes test
make_nodes $ok_dir_max_entries
expect_false make_nodes $fail_dir_max_entries

# rename files test
rename_files $ok_dir_max_entries
expect_false rename_files $fail_dir_max_entries

# symlink files test
make_symlinks $ok_dir_max_entries
expect_false make_symlinks $fail_dir_max_entries

# link files test
make_links $ok_dir_max_entries
expect_false make_links $fail_dir_max_entries

# no limit (e.g., default value)
dir_max_entries=0
ceph config set mds mds_dir_max_entries $dir_max_entries

make_files 500
make_dirs 500
make_nodes 500
rename_files 500
make_symlinks 500
make_links 500

cleanup

popd # $test_dir

echo OK
