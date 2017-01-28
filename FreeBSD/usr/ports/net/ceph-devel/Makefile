# Created by: Willem Jan Withagen <wjw@digiware.nl>
# $FreeBSD: head/net/ceph-devel/Makefile 437431 2017-04-01 11:59:18Z antoine $

PORTNAME=	ceph
DISTVERSION=	wip.v2017-2-14
CATEGORIES=	net
PKGNAMESUFFIX=	-devel

MAINTAINER=	wjw@digiware.nl
COMMENT=Ceph delivers object, block, and file storage in a unified system

LICENSE=	GPLv2 LGPL3
LICENSE_COMB=	dual

BUILD_DEPENDS=	\
	yasm:devel/yasm \
	pkgconf:devel/pkgconf \
	cython>=0.25.2:lang/cython \
	sphinx-build:textproc/py-sphinx \
	${LOCALBASE}/lib/libatomic_ops.a:devel/libatomic_ops
LIB_DEPENDS=	\
	libboost_python.so:devel/boost-python-libs \
	libboost_thread.so:devel/boost-libs \
	libleveldb.so:databases/leveldb \
	libldap.so:net/openldap24-client \
	libnss3.so:security/nss \
	libcryptopp.so:security/cryptopp \
	libsnappy.so:archivers/snappy \
	libcurl.so:ftp/curl \
	libxml2.so:textproc/libxml2 \
	libexpat.so:textproc/expat2 \
	liblz4.so:archivers/liblz4 \
	libfcgi.so:www/fcgi \
	libplds4.so:devel/nspr \
	libtcmalloc.so:devel/google-perftools
RUN_DEPENDS=	\
	bash:shells/bash \
	${LOCALBASE}/bin/getopt:misc/getopt \
	xml:textproc/xmlstarlet \
	jq:textproc/jq \
	flock:sysutils/flock \
	virtualenv:devel/py-virtualenv

USE_GITHUB=	yes
GH_ACCOUNT=	wjwithagen:DEFAULT
GH_TUPLE+=	facebook:rocksdb:a0deec9:rocksdb/src/rocksdb
GH_TUPLE+=	ceph:ceph-erasure-code-corpus:b5c8634:cepherasurecodecorpus/ceph-erasure-code-corpus
GH_TUPLE+=	ceph:ceph-object-corpus:master:cephobjectcorpus/ceph-object-corpus
GH_TUPLE+=	ceph:civetweb:v1.5-1809-g7f9f5d1:civetweb/src/civetweb
GH_TUPLE+=	ceph:jerasure:v2-ceph:jerasure/src/erasure-code/jerasure/jerasure
GH_TUPLE+=	ceph:gf-complete:v3-ceph:gfcomplete/src/erasure-code/jerasure/gf-complete
GH_TUPLE+=	ceph:googletest:ceph-release-1.7.x:googletest/src/googletest
GH_TUPLE+=	ceph:spdk:v1.2.0-39-g9322c25:spdk/src/spdk
GH_TUPLE+=	ceph:xxHash:v0.5.1-2-g1f40c65:xxHash/src/xxHash
GH_TUPLE+=	ceph:isa-l:v2.16.0:isal/src/isa-l
GH_TUPLE+=	ceph:lua:lua-5.3-ceph:lua/src/lua
GH_TUPLE+=	ceph:Beast:999e2fa:Beast/src/Beast
GH_TUPLE+=	boostorg:boost:boost-1.61.0-275-g1790aff:boost/src/boost
GH_TUPLE+=	ceph:dpdk:a38e5ec:dpd/src/dpd
GH_TUPLE+=	facebook:zstd:v1.1.2:zstd/src/zstd

CMAKE_BUILD_TYPE=	Release
CMAKE_ARGS=	\
	-D ENABLE_GIT_VERSION:BOOL=OFF \
	-D WITH_SYSTEM_BOOST:BOOL=ON \
	-D WITH_LTTNG:BOOL=OFF \
	-D WITH_FUSE:BOOL=OFF \
	-D WITH_KRBD:BOOL=OFF \
	-D WITH_XFS:BOOL=OFF \
	-D WITH_KVS:BOOL=OFF \
	-D WITH_LIBCEPHFS:BOOL=OFF \
	-D WITH_CEPHFS:BOOL=OFF \
	-D WITH_EMBEDDED:BOOL=OFF

USES=	gmake cmake:outsource python:2.7 gettext-runtime shebangfix
SHEBANG_FILES=src/rbdmap src/ceph-post-file.in src/rbd-replay-many

# even though i386 will build, it is not really advised.
ONLY_FOR_ARCHS=	amd64

USE_RC_SUBR=	ceph
USE_LDCONFIG=	yes

USERS=		ceph
GROUPS=		ceph

pre-configure:
	${LN} -s ${CONFIGURE_WRKSRC} ${WRKSRC}/build

post-install:
	${MKDIR} ${STAGEDIR}${PREFIX}/etc/ceph
	${MKDIR} ${STAGEDIR}/var/lib/ceph/bootstrap-mds
	${MKDIR} ${STAGEDIR}/var/lib/ceph/bootstrap-osd
	${MKDIR} ${STAGEDIR}/var/lib/ceph/mds
	${MKDIR} ${STAGEDIR}/var/lib/ceph/mon
	${MKDIR} ${STAGEDIR}/var/lib/ceph/osd
	${MKDIR} ${STAGEDIR}/var/lib/ceph/tmp
	${MKDIR} ${STAGEDIR}/var/log/ceph
	# Use the Ceph init.d script as the interface to regular
	# starting and stopping deamons. rc.d work is done thru
	# a separate rc.d/ceph script calling init-ceph again.
	${MV} ${STAGEDIR}${PREFIX}/etc/init.d/ceph ${STAGEDIR}${PREFIX}/bin/init-ceph
	# remove files not packaged
	${RM} ${STAGEDIR}${DOCSDIR}/sample.fetch_config
	${RM} ${STAGEDIR}${DOCSDIR}/sample.ceph.conf
	${RMDIR} ${STAGEDIR}${DOCSDIR}
	${RM} ${STAGEDIR}${PREFIX}/bin/ceph_bench_log
	${RM} ${STAGEDIR}${PREFIX}/bin/ceph_multi_stress_watch
	${RM} ${STAGEDIR}${PREFIX}/bin/ceph_perf*
	${RM} ${STAGEDIR}${PREFIX}/bin/ceph_psim
	${RM} ${STAGEDIR}${PREFIX}/bin/ceph_scratchtoolpp
	${RM} ${STAGEDIR}${PREFIX}/bin/ceph_smalliobench*
	${RM} ${STAGEDIR}${PREFIX}/bin/ceph_test*
	${RM} ${STAGEDIR}${PREFIX}/bin/ceph_tpbench
	${RM} ${STAGEDIR}${PREFIX}/bin/ceph_xattr_bench
	${RM} ${STAGEDIR}${PREFIX}/sbin/ceph-disk-udev
	${RMDIR} ${STAGEDIR}${PREFIX}/etc/init.d
	${INSTALL_DATA} ${WRKSRC}/src/sample.ceph.conf ${STAGEDIR}${PREFIX}/etc/ceph/ceph.conf.sample

.include <bsd.port.mk>
