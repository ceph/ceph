if ENABLE_CLIENT
if WITH_RADOS

libjournal_la_SOURCES = \
	journal/AsyncOpTracker.cc \
	journal/Entry.cc \
	journal/Future.cc \
	journal/FutureImpl.cc \
	journal/Journaler.cc \
	journal/JournalMetadata.cc \
	journal/JournalPlayer.cc \
	journal/JournalRecorder.cc \
	journal/JournalTrimmer.cc \
	journal/ObjectPlayer.cc \
	journal/ObjectRecorder.cc \
	journal/Utils.cc

noinst_LTLIBRARIES += libjournal.la
noinst_HEADERS += \
	journal/AsyncOpTracker.h \
	journal/Entry.h \
	journal/Future.h \
	journal/FutureImpl.h \
	journal/Journaler.h \
	journal/JournalMetadata.h \
	journal/JournalMetadataListener.h \
	journal/JournalPlayer.h \
	journal/JournalRecorder.h \
	journal/JournalTrimmer.h \
	journal/ObjectPlayer.h \
	journal/ObjectRecorder.h \
	journal/ReplayEntry.h \
	journal/ReplayHandler.h \
	journal/Utils.h
DENCODER_DEPS += libjournal.la

endif # WITH_RADOS
endif # ENABLE_CLIENT
