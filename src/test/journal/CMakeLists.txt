set(journal_test
  mock/MockJournaler.cc
  )
add_library(journal_test STATIC ${journal_test})
set_target_properties(journal_test PROPERTIES COMPILE_FLAGS
  ${UNITTEST_CXX_FLAGS})

# unittest_journal
set(unittest_journal_srcs
  test_main.cc
  test_Entry.cc
  test_FutureImpl.cc
  test_Journaler.cc
  test_JournalMetadata.cc
  test_JournalPlayer.cc
  test_JournalRecorder.cc
  test_JournalTrimmer.cc
  test_ObjectPlayer.cc
  test_ObjectRecorder.cc
  RadosTestFixture.cc
  )

add_executable(unittest_journal EXCLUDE_FROM_ALL
  ${unittest_journal_srcs} 
  )
add_ceph_unittest(unittest_journal ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/unittest_journal)
target_link_libraries(unittest_journal 
  journal
  cls_journal
  cls_journal_client
  rados_test_stub
  librados
  radostest
  global 
  )
