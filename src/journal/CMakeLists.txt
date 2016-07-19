set(journal_srcs
  AsyncOpTracker.cc
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
