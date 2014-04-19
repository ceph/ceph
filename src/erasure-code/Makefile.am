## erasure code plugins
erasure_codelibdir = $(pkglibdir)/erasure-code
erasure_codelib_LTLIBRARIES =  

include erasure-code/jerasure/Makefile.am

liberasure_code_la_SOURCES = \
	erasure-code/ErasureCodePlugin.cc
if LINUX
liberasure_code_la_LIBADD = -ldl
endif # LINUX
noinst_LTLIBRARIES += liberasure_code.la

noinst_HEADERS += \
	erasure-code/ErasureCodeInterface.h \
	erasure-code/ErasureCodePlugin.h
