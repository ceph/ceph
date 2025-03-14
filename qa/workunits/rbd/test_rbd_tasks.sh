#!/usr/bin/env bash
set -ex

POOL=rbd_tasks
POOL_NS=ns1

setup() {
  trap 'cleanup' INT TERM EXIT

  ceph osd pool create ${POOL} 128
  rbd pool init ${POOL}
  rbd namespace create ${POOL}/${POOL_NS}

  TEMPDIR=`mktemp -d`
}

cleanup() {
  ceph osd pool rm ${POOL} ${POOL} --yes-i-really-really-mean-it

  rm -rf ${TEMPDIR}
}

wait_for() {
  local TEST_FN=$1
  shift 1
  local TEST_FN_ARGS=("$@")

  for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
    sleep ${s}

    ${TEST_FN} "${TEST_FN_ARGS[@]}" || continue
    return 0
  done
  return 1
}

task_exists() {
  local TASK_ID=$1
  [[ -z "${TASK_ID}" ]] && exit 1

  ceph rbd task list ${TASK_ID} || return 1
  return 0
}

task_dne() {
  local TASK_ID=$1
  [[ -z "${TASK_ID}" ]] && exit 1

  ceph rbd task list ${TASK_ID} || return 0
  return 1
}

task_in_progress() {
  local TASK_ID=$1
  [[ -z "${TASK_ID}" ]] && exit 1

  [[ $(ceph rbd task list ${TASK_ID} | jq '.in_progress') == 'true' ]]
}

test_remove() {
  echo "test_remove"

  local IMAGE=`uuidgen`
  rbd create --size 1 --image-shared ${POOL}/${IMAGE}

  # MGR might require some time to discover the OSD map w/ new pool
  wait_for ceph rbd task add remove ${POOL}/${IMAGE}
}

test_flatten() {
  echo "test_flatten"

  local PARENT_IMAGE=`uuidgen`
  local CHILD_IMAGE=`uuidgen`

  rbd create --size 1 --image-shared ${POOL}/${PARENT_IMAGE}
  rbd snap create ${POOL}/${PARENT_IMAGE}@snap
  rbd clone ${POOL}/${PARENT_IMAGE}@snap ${POOL}/${POOL_NS}/${CHILD_IMAGE} --rbd-default-clone-format=2
  [[ "$(rbd info --format json ${POOL}/${POOL_NS}/${CHILD_IMAGE} | jq 'has("parent")')" == "true" ]]

  local TASK_ID=`ceph rbd task add flatten ${POOL}/${POOL_NS}/${CHILD_IMAGE} | jq --raw-output ".id"`
  wait_for task_dne ${TASK_ID}

  [[ "$(rbd info --format json ${POOL}/${POOL_NS}/${CHILD_IMAGE} | jq 'has("parent")')" == "false" ]]
}

test_trash_remove() {
  echo "test_trash_remove"

  local IMAGE=`uuidgen`
  rbd create --size 1 --image-shared ${POOL}/${IMAGE}
  local IMAGE_ID=`rbd info --format json ${POOL}/${IMAGE} | jq --raw-output ".id"`
  rbd trash mv ${POOL}/${IMAGE}
  [[ -n "$(rbd trash list ${POOL})" ]] || exit 1

  local TASK_ID=`ceph rbd task add trash remove ${POOL}/${IMAGE_ID} | jq --raw-output ".id"`
  wait_for task_dne ${TASK_ID}

  [[ -z "$(rbd trash list ${POOL})" ]] || exit 1
}

test_migration_execute() {
  echo "test_migration_execute"

  local SOURCE_IMAGE=`uuidgen`
  local TARGET_IMAGE=`uuidgen`
  rbd create --size 1 --image-shared ${POOL}/${SOURCE_IMAGE}
  rbd migration prepare ${POOL}/${SOURCE_IMAGE} ${POOL}/${TARGET_IMAGE}
  [[ "$(rbd status --format json ${POOL}/${TARGET_IMAGE} | jq --raw-output '.migration.state')" == "prepared" ]]

  local TASK_ID=`ceph rbd task add migration execute ${POOL}/${TARGET_IMAGE} | jq --raw-output ".id"`
  wait_for task_dne ${TASK_ID}

  [[ "$(rbd status --format json ${POOL}/${TARGET_IMAGE} | jq --raw-output '.migration.state')" == "executed" ]]
}

test_migration_commit() {
  echo "test_migration_commit"

  local SOURCE_IMAGE=`uuidgen`
  local TARGET_IMAGE=`uuidgen`
  rbd create --size 1 --image-shared ${POOL}/${SOURCE_IMAGE}
  rbd migration prepare ${POOL}/${SOURCE_IMAGE} ${POOL}/${TARGET_IMAGE}
  [[ "$(rbd status --format json ${POOL}/${TARGET_IMAGE} | jq --raw-output '.migration.state')" == "prepared" ]]

  local TASK_ID=`ceph rbd task add migration execute ${POOL}/${TARGET_IMAGE} | jq --raw-output ".id"`
  wait_for task_dne ${TASK_ID}

  TASK_ID=`ceph rbd task add migration commit ${POOL}/${TARGET_IMAGE} | jq --raw-output ".id"`
  wait_for task_dne ${TASK_ID}

  [[ "$(rbd status --format json ${POOL}/${TARGET_IMAGE} | jq 'has("migration")')" == "false" ]]
  (rbd info ${POOL}/${SOURCE_IMAGE} && return 1) || true
  rbd info ${POOL}/${TARGET_IMAGE}
}

test_migration_abort() {
  echo "test_migration_abort"

  local SOURCE_IMAGE=`uuidgen`
  local TARGET_IMAGE=`uuidgen`
  rbd create --size 1 --image-shared ${POOL}/${SOURCE_IMAGE}
  rbd migration prepare ${POOL}/${SOURCE_IMAGE} ${POOL}/${TARGET_IMAGE}
  [[ "$(rbd status --format json ${POOL}/${TARGET_IMAGE} | jq --raw-output '.migration.state')" == "prepared" ]]

  local TASK_ID=`ceph rbd task add migration execute ${POOL}/${TARGET_IMAGE} | jq --raw-output ".id"`
  wait_for task_dne ${TASK_ID}

  TASK_ID=`ceph rbd task add migration abort ${POOL}/${TARGET_IMAGE} | jq --raw-output ".id"`
  wait_for task_dne ${TASK_ID}

  [[ "$(rbd status --format json ${POOL}/${SOURCE_IMAGE} | jq 'has("migration")')" == "false" ]]
  rbd info ${POOL}/${SOURCE_IMAGE}
  (rbd info ${POOL}/${TARGET_IMAGE} && return 1) || true
}

test_list() {
  echo "test_list"

  local IMAGE_1=`uuidgen`
  local IMAGE_2=`uuidgen`

  rbd create --size 1T --image-shared ${POOL}/${IMAGE_1}
  rbd create --size 1T --image-shared ${POOL}/${IMAGE_2}

  local TASK_ID_1=`ceph rbd task add remove ${POOL}/${IMAGE_1} | jq --raw-output ".id"`
  local TASK_ID_2=`ceph rbd task add remove ${POOL}/${IMAGE_2} | jq --raw-output ".id"`

  local LIST_FILE="${TEMPDIR}/list_file"
  ceph rbd task list > ${LIST_FILE}
  cat ${LIST_FILE}

  [[ $(jq "[.[] | .id] | contains([\"${TASK_ID_1}\", \"${TASK_ID_2}\"])" ${LIST_FILE}) == "true" ]]

  ceph rbd task cancel ${TASK_ID_1}
  ceph rbd task cancel ${TASK_ID_2}
}

test_cancel() {
  echo "test_cancel"

  local IMAGE=`uuidgen`
  rbd create --size 1T --image-shared ${POOL}/${IMAGE}
  local TASK_ID=`ceph rbd task add remove ${POOL}/${IMAGE} | jq --raw-output ".id"`

  wait_for task_exists ${TASK_ID}

  ceph rbd task cancel ${TASK_ID}
  wait_for task_dne ${TASK_ID}
}

test_duplicate_task() {
  echo "test_duplicate_task"

  local IMAGE=`uuidgen`
  rbd create --size 1T --image-shared ${POOL}/${IMAGE}
  local IMAGE_ID=`rbd info --format json ${POOL}/${IMAGE} | jq --raw-output ".id"`
  rbd trash mv ${POOL}/${IMAGE}

  local TASK_ID_1=`ceph rbd task add trash remove ${POOL}/${IMAGE_ID} | jq --raw-output ".id"`
  local TASK_ID_2=`ceph rbd task add trash remove ${POOL}/${IMAGE_ID} | jq --raw-output ".id"`

  [[ "${TASK_ID_1}" == "${TASK_ID_2}" ]]

  ceph rbd task cancel ${TASK_ID_1}
}

test_duplicate_name() {
  echo "test_duplicate_name"

  local IMAGE=`uuidgen`
  rbd create --size 1G --image-shared ${POOL}/${IMAGE}
  local TASK_ID_1=`ceph rbd task add remove ${POOL}/${IMAGE} | jq --raw-output ".id"`

  wait_for task_dne ${TASK_ID_1}

  rbd create --size 1G --image-shared ${POOL}/${IMAGE}
  local TASK_ID_2=`ceph rbd task add remove ${POOL}/${IMAGE} | jq --raw-output ".id"`

  [[ "${TASK_ID_1}" != "${TASK_ID_2}" ]]
  wait_for task_dne ${TASK_ID_2}

  local TASK_ID_3=`ceph rbd task add remove ${POOL}/${IMAGE} | jq --raw-output ".id"`

  [[ "${TASK_ID_2}" == "${TASK_ID_3}" ]]
}

test_progress() {
  echo "test_progress"

  local IMAGE_1=`uuidgen`
  local IMAGE_2=`uuidgen`

  rbd create --size 1 --image-shared ${POOL}/${IMAGE_1}
  local TASK_ID_1=`ceph rbd task add remove ${POOL}/${IMAGE_1} | jq --raw-output ".id"`

  wait_for task_dne ${TASK_ID_1}

  local PROGRESS_FILE="${TEMPDIR}/progress_file"
  ceph progress json > ${PROGRESS_FILE}
  cat ${PROGRESS_FILE}

  [[ $(jq "[.completed | .[].id] | contains([\"${TASK_ID_1}\"])" ${PROGRESS_FILE}) == "true" ]]

  rbd create --size 1T --image-shared ${POOL}/${IMAGE_2}
  local TASK_ID_2=`ceph rbd task add remove ${POOL}/${IMAGE_2} | jq --raw-output ".id"`

  wait_for task_in_progress ${TASK_ID_2}
  ceph progress json > ${PROGRESS_FILE}
  cat ${PROGRESS_FILE}

  [[ $(jq "[.events | .[].id] | contains([\"${TASK_ID_2}\"])" ${PROGRESS_FILE}) == "true" ]]

  ceph rbd task cancel ${TASK_ID_2}
  wait_for task_dne ${TASK_ID_2}

  ceph progress json > ${PROGRESS_FILE}
  cat ${PROGRESS_FILE}

  [[ $(jq "[.completed | map(select(.failed)) | .[].id] | contains([\"${TASK_ID_2}\"])" ${PROGRESS_FILE}) == "true" ]]
}

setup
test_remove
test_flatten
test_trash_remove
test_migration_execute
test_migration_commit
test_migration_abort
test_list
test_cancel
test_duplicate_task
test_duplicate_name
test_progress

echo OK
