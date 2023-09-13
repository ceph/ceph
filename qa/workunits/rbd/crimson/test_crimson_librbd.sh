#!/bin/sh -e

if [ -n "${VALGRIND}" ]; then
  valgrind ${VALGRIND} --suppressions=${TESTDIR}/valgrind.supp \
    --error-exitcode=1 ceph_test_librbd
else
  # Run test cases indivually to allow better selection
  # of ongoing Crimson development.
  # Disabled test groups are tracked here:
  # https://tracker.ceph.com/issues/58791
  ceph_test_librbd --gtest_filter='TestLibRBD.*'
  ceph_test_librbd --gtest_filter='EncryptedFlattenTest/0.*'
  ceph_test_librbd --gtest_filter='EncryptedFlattenTest/1.*'
  ceph_test_librbd --gtest_filter='EncryptedFlattenTest/2.*'
  ceph_test_librbd --gtest_filter='EncryptedFlattenTest/3.*'
  ceph_test_librbd --gtest_filter='EncryptedFlattenTest/4.*'
  ceph_test_librbd --gtest_filter='EncryptedFlattenTest/5.*'
  ceph_test_librbd --gtest_filter='EncryptedFlattenTest/6.*'
  ceph_test_librbd --gtest_filter='EncryptedFlattenTest/7.*'
  # ceph_test_librbd --gtest_filter='DiffIterateTest/0.*'
  # ceph_test_librbd --gtest_filter='DiffIterateTest/1.*'
  ceph_test_librbd --gtest_filter='TestImageWatcher.*'
  ceph_test_librbd --gtest_filter='TestInternal.*'
  ceph_test_librbd --gtest_filter='TestMirroring.*'
  # ceph_test_librbd --gtest_filter='TestDeepCopy.*'
  ceph_test_librbd --gtest_filter='TestGroup.*'
  # ceph_test_librbd --gtest_filter='TestMigration.*'
  ceph_test_librbd --gtest_filter='TestMirroringWatcher.*'
  ceph_test_librbd --gtest_filter='TestObjectMap.*'
  ceph_test_librbd --gtest_filter='TestOperations.*'
  ceph_test_librbd --gtest_filter='TestTrash.*'
  ceph_test_librbd --gtest_filter='TestJournalEntries.*'
  ceph_test_librbd --gtest_filter='TestJournalReplay.*'
fi
exit 0
