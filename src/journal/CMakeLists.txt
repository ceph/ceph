set(journal_srcs
  Entry.cc
  Future.cc
  FutureImpl.cc
  Journaler.cc
  JournalMetadata.cc
  JournalPlayer.cc
  JournalRecorder.cc
  JournalTrimmer.cc
  ObjectPlayer.cc
  ObjectRecorder.cc
  Utils.cc)
add_library(journal STATIC ${journal_srcs})
target_link_libraries(journal cls_journal_client)
