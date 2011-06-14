AUTOMAKE_OPTIONS = gnu

SUBDIRS = 
bin_PROGRAMS =

AM_CFLAGS = -Wall

direct_io_test_SOURCES = direct_io_test.c
test_sync_io_SOURCES = test_sync_io.c
test_short_dio_read_SOURCES = test_short_dio_read.c

if WITH_DEBUG
bin_PROGRAMS += direct_io_test test_sync_io test_short_dio_read
endif
