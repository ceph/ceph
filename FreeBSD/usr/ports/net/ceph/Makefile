PORTNAME=	ceph
PORTREVISION=	11.0.2
PORTVERSION=	wip.FreeBSD
CATEGORIES=     net
MASTER_SITE=    http://github.com/

MAINTAINER=     wjw@digiware.nl
COMMENT=        Ceph uniquely delivers object, block, and file storage in one unified system.

LICENSE=        GPLv2 LGPL3
LICENSE_COMB=   dual

# even though i386 is nor really advised.
ONLY_FOR_ARCHS= i386 amd64

BUILD_DEPENDS= \
        git:devel/git \
        gmake:devel/gmake \
        cmake:devel/cmake \
        yasm:devel/yasm \
        pkgconf:devel/pkgconf \
        cython:lang/cython \
        python:lang/python27 \

RUN_DEPENDS= \
        getopt:misc/getopt \
        xml:textproc/xmlstarlet \
        jq:textproc/jq \
        flock:sysutils/flock \
        virtualenv:devel/py-virtualenv \

LIB_DEPENDS= \
        libboost_system.so:devel/boost-all \
        libboost_python.so:devel/boost-python-libs \
        libleveldb.so:databases/leveldb \
        libldap.so:net/openldap24-client \
        libnss3.so:security/nss \
        libcryptopp.so:security/cryptopp \
        libsnappy.so:archivers/snappy \
        libcurl.so:ftp/curl \
        libxml2.so:textproc/libxml2 \
        libexpat.so:textproc/expat2 \

OTHER_DEPENDS= \
        libatomic_ops.a:devel/libatomic_ops \
        libedit.so:devel/libedit \

TEST_DEPENDS= \
        java/junit \
        devel/py-argparse \
        devel/py-nose \
        www/py-flask \
        www/fcgi \


USE_GITHUB=	yes
GH_ACCOUNT=	wjwithagen:DEFAULT
# GH_TUPLE allows putting GH_ACCOUNT, GH_PROJECT, GH_TAGNAME, and GH_SUBDIR into a single variable. 
# The format is account:project:tagname:group/subdir. The /subdir part is optional. It is helpful i
# when there is more than one GitHub project from which to fetch.
GH_TUPLE=	wjwithagen:ceph:${PORTVERSION}:ceph
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

# USERS=	ceph
# GROUPS=	ceph

USES= 	gmake cmake:outsource

CMAKE_SOURCE_PATH=	..
CMAKE_VERBOSE=		Y
CMAKE_BUILD_TYPE=	RelWithDebInfo
CMAKE_ARGS=	\
        -D ENABLE_GIT_VERSION=OFF \
        -D WITH_SYSTEM_BOOST=ON \
        -D WITH_LTTNG=OFF \
        -D WITH_FUSE=OFF \
        -D WITH_KRBD=OFF \
        -D WITH_XFS=OFF \
        -D WITH_KVS=OFF \
        -D WITH_LIBCEPHFS=OFF \
        -D WITH_CEPHFS=OFF \
        -D WITH_RADOSGW=ON \
	-D WITH_EMBEDDED=OFF \
	..
BUILD_WRKSRC= ${WRKSRC}/build

pre-configure:
	mkdir -p ${WRKSRC}/build
	ln -s ${WRKSRC}/build ${WRKDIR}/.build

do-configure:
	( cd ${WRKSRC}/build ;\
	  cmake ${CMAKE_ARGS} .. ;\
	)

pre-build:
	echo Checking cython version to be atleast 0.25.2
	pkg info -x 'cython-0.25.[2-9]'
	
do-build:
	echo Building Ceph
	( cd ${WRKSRC}/build; \
	  gmake -j8 V=1 VERBOSE=1 ;\
	)

post-stage:
	${MKDIR} ${STAGEDIR}/etc/ceph
	${MKDIR} ${STAGEDIR}/var/lib/ceph
	${MKDIR} ${STAGEDIR}/var/lib/ceph/bootstrap-mds
	${MKDIR} ${STAGEDIR}/var/lib/ceph/bootstrap-osd
	${MKDIR} ${STAGEDIR}/var/lib/ceph/mds
	${MKDIR} ${STAGEDIR}/var/lib/ceph/mon
	${MKDIR} ${STAGEDIR}/var/lib/ceph/osd
	${MKDIR} ${STAGEDIR}/var/lib/ceph/tmp
	${MKDIR} ${STAGEDIR}/var/log/ceph
	${MKDIR} ${STAGEDIR}/usr/local/etc/rc.d
	${CP}    ${WRKSRC}/build/bin/init-ceph ${STAGEDIR}/usr/local/etc/rc.d/ceph



.include <bsd.port.mk>
