# zlib plugin

MACHINE = $(shell uname -p)

.asm.s:
	@echo "  MKTMP   " $@;
	@cp $< $@

noinst_HEADERS += \
  compressor/zlib/ZlibCompressor.h \
  isa-l/include/igzip_lib.h \
  isa-l/include/types.h

zlib_sources = \
  common/buffer.cc \
  compressor/Compressor.cc \
  compressor/zlib/CompressionPluginZlib.cc \
  compressor/zlib/ZlibCompressor.cc

if INTEL
zlib_sources += \
  isa-l/igzip/igzip.c \
  isa-l/igzip/hufftables_c.c \
  isa-l/igzip/crc_utils_01.asm \
  isa-l/igzip/crc_utils_04.asm \
  isa-l/igzip/igzip_body_01.asm \
  isa-l/igzip/igzip_body_04.asm \
  isa-l/igzip/igzip_finish.asm \
  isa-l/igzip/igzip_stateless_01.asm \
  isa-l/igzip/igzip_stateless_04.asm \
  isa-l/igzip/crc_data.asm \
  isa-l/igzip/crc32_gzip.asm \
  isa-l/igzip/detect_repeated_char.asm \
  isa-l/igzip/igzip_multibinary.asm \
  isa-l/igzip/igzip_stateless_base.c \
  isa-l/igzip/igzip_base.c
endif

compressor/zlib/CompressionPluginZlib.cc: ./ceph_ver.h

libceph_zlib_la_SOURCES = ${zlib_sources}
libceph_zlib_la_CFLAGS = ${AM_CFLAGS} \
    -I $(srcdir)/isa-l/include/

if INTEL
libceph_zlib_la_CCASFLAGS = ${AM_CCASFLAGS} \
    -I $(srcdir)/isa-l/include/
endif

libceph_zlib_la_CXXFLAGS= ${AM_CXXFLAGS}
libceph_zlib_la_LIBADD = $(LIBCRUSH) $(PTHREAD_LIBS) $(EXTRALIBS)
libceph_zlib_la_LDFLAGS = ${AM_LDFLAGS} -lz -version-info 2:0:0
libceph_zlib_la_LIBTOOLFLAGS = --tag=CC
if LINUX
libceph_zlib_la_LDFLAGS += -export-symbols-regex '.*__compressor_.*'
endif

compressorlib_LTLIBRARIES += libceph_zlib.la
