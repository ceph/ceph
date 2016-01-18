#!/bin/sh -xv
~
	# sudo ./install-deps.sh
	. ./autogen_freebsd.sh
	./autogen.sh \
	&& ./configure ${CONFIGURE_FLAGS} \
	&& ( cd src/gmock/gtest; patch < /usr/ports/devel/googletest/files/patch-bsd-defines ) \
	&& gmake -j8 ENABLE_GIT_VERSION=OFF | tee gmake.log \
	&& gmake check ENABLE_GIT_VERSION=OFF CEPH_BUFFER_NO_BENCH=yes | tee gmake_check.log


