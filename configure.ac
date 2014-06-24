#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# Autoconf
AC_PREREQ(2.59)

# NOTE: This version is _only_ used for naming the tarball.  The
# VERSION define is not used by the code.  It gets a version string
# from 'git describe'; see src/ceph_ver.[ch]

AC_INIT([ceph], [0.81], [ceph-devel@vger.kernel.org])

# Create release string.  Used with VERSION for RPMs.
RPM_RELEASE=0
AC_SUBST(RPM_RELEASE)
if test -d ".git" ; then
  AC_CHECK_PROG(GIT_CHECK, git, yes)
  if test x"$GIT_CHECK" = x"yes"; then
    RPM_RELEASE=`if expr index $(git describe --always) '-' > /dev/null ; then git describe --always | cut -d- -f2- | tr '-' '.' ; else echo "0"; fi`
  fi
fi
AC_MSG_NOTICE([RPM_RELEASE='$RPM_RELEASE'])

AC_CONFIG_MACRO_DIR([m4])

AC_CONFIG_SUBDIRS([src/gtest])

# Environment
AC_CANONICAL_HOST
AC_CANONICAL_TARGET

# Fix automake problems in 1.12
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])

# Automake
AM_INIT_AUTOMAKE
AM_PROG_CC_C_O
AM_PROG_LIBTOOL
AM_PROG_AS


# enable make V=0 (if automake >1.11)
AM_INIT_AUTOMAKE([foreign])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

# Platform
case "${target_os}" in
darwin*)
	AC_DEFINE([DARWIN], [1], [Define if darwin/osx])
	;;
linux*)
	linux="yes"
	;;
freebsd*)
	freebsd="yes"
	;;
esac
AM_CONDITIONAL(LINUX, test x"$linux" = x"yes")
AM_CONDITIONAL(FREEBSD, test x"$freebsd" = x"yes")

# Checks for programs.
AC_PROG_CXX
if test "$CXX" = no || test "$CXX:$GXX" = "g++:"; then
  AC_MSG_ERROR([no C++ compiler found])
fi

AC_MSG_CHECKING([if compiler is clang])
AC_COMPILE_IFELSE([AC_LANG_PROGRAM([], [[
#ifndef __clang__
#error "Not Clang"
#endif
return 0;
]])],
[CLANG=yes], [CLANG=no])
AC_MSG_RESULT([$CLANG])
AM_CONDITIONAL(CLANG, test "$CLANG" = "yes")

#AC_PROG_CC
AC_PROG_MAKE_SET
AC_PROG_LIBTOOL

# Compiler flags

AC_SUBST(AM_CXXFLAGS)
AM_CXXFLAGS="${AM_CXXFLAGS}"

# Check for yasm
if yasm -f elf64 src/common/crc32c_intel_fast_asm.S -o /dev/null; then
   echo 'we have a modern and working yasm'
   if test `arch` = "x86_64"; then
      echo 'we are x86_64'
      AC_DEFINE([HAVE_GOOD_YASM_ELF64], [1], [we have a recent yasm and are x86_64])
      with_good_yasm=yes
   fi
else
   echo 'we do not have a modern/working yasm'
fi
AM_CONDITIONAL(WITH_GOOD_YASM_ELF64, test "$with_good_yasm" = "yes")

# Checks for compiler warning types

# AC_CHECK_CC_FLAG(FLAG_TO_TEST, VARIABLE_TO_SET_IF_SUPPORTED)
# ---------
AC_DEFUN([AC_CHECK_CC_FLAG],
[{
	AC_LANG_PUSH([C])
	my_cflags_save="$CFLAGS"
	CFLAGS="$my_cflags_save $1"
	AC_MSG_CHECKING([whether $CC accepts $1])
	AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[ ]], [[ ]])],
			  [AC_MSG_RESULT([yes]); AC_SUBST([$2], ["$1"])],
			  [AC_MSG_RESULT([no])]
			 )
	CFLAGS="$my_cflags_save"
	AC_LANG_POP([C])
}])

AC_CHECK_CC_FLAG([-Wtype-limits], [WARN_TYPE_LIMITS])
AC_CHECK_CC_FLAG([-Wignored-qualifiers], [WARN_IGNORED_QUALIFIERS])

# Checks for architecture stuff
AM_CONDITIONAL([ENABLE_FPU_NEON], [case $target_cpu in arm*) true;; *) false;; esac])

# Check for compiler VTA support
AX_CHECK_COMPILE_FLAG([-fvar-tracking-assignments], [HAS_VTA_SUPPORT=1], [HAS_VTA_SUPPORT=0])
AM_CONDITIONAL(COMPILER_HAS_VTA, [test "$HAS_VTA_SUPPORT" = 1])

AX_CXX_STATIC_CAST
AX_C_VAR_FUNC
AX_C_PRETTY_FUNC

# Checks for libraries.
ACX_PTHREAD

AC_CHECK_LIB([uuid], [uuid_parse], [true], AC_MSG_FAILURE([libuuid not found]))

# rbd {map,unmap,showmapped} dependencies, Linux only
if test x"$linux" = x"yes"; then
  # libblkid
  AC_CHECK_HEADER([blkid/blkid.h], [],
    AC_MSG_ERROR([blkid/blkid.h not found (libblkid-dev, libblkid-devel)]))
  AC_CHECK_LIB([blkid], [blkid_devno_to_wholedisk], [],
    AC_MSG_FAILURE([libblkid not found]))

  # libudev
  AC_CHECK_HEADER([libudev.h], [],
    AC_MSG_ERROR([libudev.h not found (libudev-dev, libudev-devel)]))
  AC_CHECK_LIB([udev], [udev_monitor_receive_device], [],
    AC_MSG_FAILURE([libudev not found]))
fi

#
# Check for res_nquery in libresolv. There are several variations. On OSX
# res_nquery is a macro defined in resolv.h, so the typical AC_CHECK_LIB
# doesn't work. On FreeBSD res_nquery can be found in libc. The required
# library for linking (if any) is defined RESOLV_LIBS.
#
AC_CHECK_HEADER([resolv.h], [], [], [#include <netinet/in.h>])

AC_DEFUN([CHECK_RESOLV_LIBS], [{
  AC_MSG_CHECKING([if res_nquery will link (LIBS=$1)])
  saved_LIBS="${LIBS}"
  LIBS="$1"
  AC_LINK_IFELSE([AC_LANG_PROGRAM([[
        #include <netinet/in.h>
        #include <resolv.h>
      ]], [[res_nquery(0, 0, 0, 0, 0, 0);]])],
      [AC_MSG_RESULT([yes])
      [$2]],
      AC_MSG_RESULT([no]))
  LIBS="${saved_LIBS}"
}])

RESOLV_LIBS=""
CHECK_RESOLV_LIBS([$RESOLV_LIBS], [resolv_libs="ok"])
if test x"$resolv_libs" != "xok"; then
  RESOLV_LIBS="-lresolv"
  CHECK_RESOLV_LIBS([$RESOLV_LIBS], [resolv_libs="ok"])
  if test x"$resolv_libs" != "xok"; then
    AC_MSG_FAILURE([no resolv library found])
  fi
fi
AC_SUBST([RESOLV_LIBS])

dnl check for libkeyutils on linux
KEYUTILS_LIB=""
AS_IF([test x"$linux" = x"yes"], [
  AC_CHECK_LIB([keyutils], [add_key], [KEYUTILS_LIB="-lkeyutils"], [
    AC_MSG_FAILURE([libkeyutils not found])])])
AC_SUBST(KEYUTILS_LIB)

AC_CHECK_LIB([m], [pow], [true], AC_MSG_FAILURE([libm not found]))
AC_CHECK_FUNCS([syncfs], AC_DEFINE([HAVE_SYS_SYNCFS], [1], [we have syncfs]), [])

# Find some crypto library for us to use, while letting user to decide which one to use.
AC_ARG_WITH([cryptopp],
	[AS_HELP_STRING([--with-cryptopp], [Use cryptographic functions from cryptopp])],
	[],
	[with_cryptopp=check])
have_cryptopp=no
# this looks clumsy but it's just if A then { success } else { if B then success }
AS_IF([test "x$with_cryptopp" != "xno"],
    [PKG_CHECK_MODULES([CRYPTOPP],
	    [libcrypto++],
	    [have_cryptopp=yes],
	    [
		   AC_LANG_PUSH([C++])
		   SAVED_CXXFLAGS="${CXXFLAGS}"
		   SAVED_LIBS="${LIBS}"
		   LIBS="${LIBS} ${PTHREAD_LIBS}"
		   CXXFLAGS="${CXXFLAGS} ${PTHREAD_CFLAGS}"
		   AC_SEARCH_LIBS([_ZTIN8CryptoPP14CBC_EncryptionE], [crypto++ cryptopp],
			  [have_cryptopp=yes],
			  [true], [])
		   CRYPTOPP_LIBS="${ac_cv_search__ZTIN8CryptoPP14CBC_EncryptionE}"
		   LIBS="${SAVED_LIBS}"
		   CXXFLAGS="${SAVED_CXXFLAGS}"
		   AC_LANG_POP([C++])
	   ])])
# bail out if given explicit --with-cryptopp
if test "x$have_cryptopp" = "xno" -a "x$with_cryptopp" != "xcheck" -a "x$with_cryptopp" != "xno"; then
    AC_MSG_FAILURE([--with-cryptopp was given, but library was not found])
fi

AC_ARG_WITH([nss],
	[AS_HELP_STRING([--with-nss], [Use cryptographic functions from nss])],
	[],
	[with_nss=check])
have_nss=no
AS_IF([test "x$with_nss" != "xno"],
    [PKG_CHECK_MODULES([NSS], [nss], [have_nss=yes], [true])])
# bail out if given explicit --with-nss
if test "x$have_nss" = "xno" -a "x$with_nss" != "xcheck" -a "x$with_nss" != "xno"; then
    AC_MSG_FAILURE([--with-nss was given, but library was not found])
fi

# now decide which crypto library to really use
if test "x$have_cryptopp" = "xyes"; then
    AC_MSG_NOTICE([using cryptopp for cryptography])
    AC_DEFINE([USE_CRYPTOPP], [1], [Define if using CryptoPP.])
    AC_SUBST([CRYPTO_CFLAGS], [$CRYPTOPP_CFLAGS])
    #AC_SUBST([CRYPTO_CXXFLAGS], [$CRYPTOPP_CXXFLAGS])
    AM_CXXFLAGS="${AM_CXXFLAGS} ${CRYPTOPP_CXXFLAGS}"
    AC_SUBST([CRYPTO_LIBS], [$CRYPTOPP_LIBS])
elif test "x$have_nss" = "xyes"; then
    AC_MSG_NOTICE([using nss for cryptography])
    AC_DEFINE([USE_NSS], [1], [Define if using NSS.])
    AC_SUBST([CRYPTO_CFLAGS], [$NSS_CFLAGS])
    # this needs CFLAGS too in practise to get the includes right. ugly.
    #AC_SUBST([CRYPTO_CXXFLAGS], [$NSS_CFLAGS $NSS_CXXFLAGS])
    AM_CXXFLAGS="${AM_CXXFLAGS} ${NSS_CFLAGS} ${NSS_CXXFLAGS}"
    AC_SUBST([CRYPTO_LIBS], [$NSS_LIBS])
else
    AC_MSG_FAILURE([no suitable crypto library found])
fi

# profiler?
AC_ARG_WITH([profiler],
            [AS_HELP_STRING([--with-profiler], [build extra profiler binaries])],
	    [case "${withval}" in
	    	  yes) with_profiler=yes ;;
		  no)  with_profiler=no ;;
		  *) AC_MSG_ERROR([bad value ${withval} for --with-profiler]) ;;
	     esac],
            [with_profiler=no])
AS_IF([test "x$with_profiler" = xyes],
	    [AC_CHECK_LIB([profiler], [ProfilerFlush], [],
	        	  [AC_MSG_FAILURE([--with-profiler was given but libprofiler (libgoogle-perftools-dev on debian) not found])])],
	    [])
AM_CONDITIONAL(WITH_PROFILER, test "$with_profiler" = "yes")
AS_IF([test "$with_profiler" = "yes"],
      [AC_DEFINE([HAVE_PROFILER], [1], [Define if you have perftools profiler enabled])],
      [])

# debug crap?
AC_ARG_WITH([debug],
            [AS_HELP_STRING([--with-debug], [build extra debug binaries])],
            [case "${withval}" in
		  yes) with_debug=yes ;;
		  no)  with_debug=no ;;
		  *) AC_MSG_ERROR([bad value ${withval} for --with-debug]) ;;
	     esac],
            [with_debug=no])
AM_CONDITIONAL(WITH_DEBUG, test "$with_debug" = "yes")

AC_DEFINE([DEBUG_GATHER], [1], [Define if you want C_Gather debugging])

# code coverage?
AC_ARG_ENABLE([coverage],
            [AS_HELP_STRING([--enable-coverage], [enable code coverage tracking])],
            [],
            [enable_coverage=no])
AM_CONDITIONAL(ENABLE_COVERAGE, test "x$enable_coverage" != xno)
if test "x$enable_coverage" != xno; then
   AC_DEFINE([ENABLE_COVERAGE], [1], [Define if enabling coverage.])
fi
AC_SUBST(GCOV_PREFIX_STRIP, `echo $(pwd)/src | tr -dc / | wc -c`)

# radosgw?
AC_ARG_WITH([radosgw],
            [AS_HELP_STRING([--with-radosgw], [build RADOS gateway])],
            [],
            [with_radosgw=check])
RADOSGW=0
AS_IF([test "x$with_radosgw" != xno],
	    [AC_CHECK_LIB([fcgi], [FCGX_Init],
             [AC_CHECK_LIB([expat], [XML_Parse],
              [AC_CHECK_LIB([curl], [curl_easy_init],
               [RADOSGW=1
	        AC_CHECK_HEADER([fastcgi/fcgiapp.h],
		 [AC_DEFINE([FASTCGI_INCLUDE_DIR], [1], [FastCGI headers are in /usr/include/fastcgi])])
	       ],
	       [if test "x$with_radosgw" != "xcheck"; then
		    AC_MSG_FAILURE([--with-radosgw was given but libcurl (libcurl-dev on debian) not found])
	       fi])
              ],
	      [if test "x$with_radosgw" != "xcheck"; then
		   AC_MSG_FAILURE([--with-radosgw was given but libexpat (libexpat1-dev on debian) not found])
	      fi])
             ],
	     [if test "x$with_radosgw" != "xcheck"; then
		AC_MSG_FAILURE([--with-radosgw was given but libfcgi (libfcgi-dev on debian) not found])
	     fi])])
AM_CONDITIONAL(WITH_RADOSGW, test "$RADOSGW" = "1")

AS_IF([test "$RADOSGW" = "1"], [AC_DEFINE([WITH_RADOSGW], [1], [define if radosgw enabled])])

AS_IF([test "$RADOSGW" = "1"], 
              [AC_CHECK_LIB([curl], [curl_multi_wait],
                            AC_DEFINE([HAVE_CURL_MULTI_WAIT], [1], [Define if have curl_multi_wait()]))
              ])

# fuse?
AC_ARG_WITH([fuse],
            [AS_HELP_STRING([--without-fuse], [disable FUSE userspace client])],
            [],
            [with_fuse=yes])
LIBFUSE=
AS_IF([test "x$with_fuse" != xno],
	    [AC_CHECK_LIB([fuse], [fuse_main],
             [AC_SUBST([LIBFUSE], ["-lfuse"])
               AC_DEFINE([HAVE_LIBFUSE], [1],
                         [Define if you have fuse])
               HAVE_LIBFUSE=1
	       # look for fuse_getgroups and define FUSE_GETGROUPS if found
           LIBS_saved="$LIBS"
           LIBS="$LIBS -lfuse"
	       AC_CHECK_FUNCS([fuse_getgroups])
           LIBS="$LIBS_saved"
              ],
             [AC_MSG_FAILURE(
                   [no FUSE found (use --without-fuse to disable)])])])
AM_CONDITIONAL(WITH_FUSE, [test "$HAVE_LIBFUSE" = "1"])

# tcmalloc?
AC_ARG_WITH([tcmalloc],
	    [AS_HELP_STRING([--without-tcmalloc], [disable tcmalloc for memory allocations])],
	    [],
	    [with_tcmalloc=yes])
TCMALLOC=
AS_IF([test "x$with_tcmalloc" != xno],
	    [AC_CHECK_LIB([tcmalloc], [malloc],
	     [AC_SUBST([LIBTCMALLOC], ["-ltcmalloc"])
	       AC_DEFINE([HAVE_LIBTCMALLOC], [1],
	       		 [Define if you have tcmalloc])
	       HAVE_LIBTCMALLOC=1
	     ],
	    [AC_MSG_FAILURE(
		  [no tcmalloc found (use --without-tcmalloc to disable)])])])
AM_CONDITIONAL(WITH_TCMALLOC, [test "$HAVE_LIBTCMALLOC" = "1"])

#set pg ref debugging?
AC_ARG_ENABLE([pgrefdebugging],
	    [AS_HELP_STRING([--enable-pgrefdebugging], [enable pg ref debugging])],
			[AC_DEFINE([PG_DEBUG_REFS], [1], [Defined if you want pg ref debugging])],
			[])

#
# Java is painful
#   - adapted from OMPI wrappers package
#   - this might become bigger. maybe should be own m4 file
#
AC_ARG_ENABLE(cephfs-java,
    [AC_HELP_STRING([--enable-cephfs-java], [build libcephfs Java bindings])],
    [], [enable_cephfs_java=no])

AM_CONDITIONAL(ENABLE_CEPHFS_JAVA, [test "x$enable_cephfs_java" = "xyes"])

AC_ARG_WITH(jdk-dir,
    AC_HELP_STRING([--with-jdk-dir(=DIR)], [Path to JDK directory]))

if test "x$enable_cephfs_java" = "xyes"; then

	# setup bin/include dirs from --with-jdk-dir (search for jni.h, javac)
	AS_IF([test -n "$with_jdk_dir"], [
		   javac_prog=`find $with_jdk_dir/ -name javac | head -n 1`
		   AS_IF([test -x "$javac_prog"], [
				  EXTRA_JDK_BIN_DIR=`dirname $javac_prog`])
		   jnih=`find $with_jdk_dir/ -name jni.h | head -n 1`
		   AS_IF([test -r "$jnih"], [
				  EXTRA_JDK_INC_DIR=`dirname $jnih`])])

	# setup defaults for Debian default-jdk package (without --with-jdk-dir)
	AS_IF([test -z "$with_jdk_dir"], [
		   # This works with Debian's and CentOS' default-jdk package
       for dir in '/usr/lib/jvm/default-java/' '/usr/lib/jvm/java/' '/usr/lib/jvm/java-gcj/'; do
          # only test if a suitable path has not yet been found
          AS_IF([test "$EXTRA_JDK_BIN_DIR" == ""], [
		          AS_IF([test -x "$javac_prog"], [
				          EXTRA_JDK_BIN_DIR=`dirname $javac_prog`])
		          jnih=`find $dir -name jni.h | head -n 1`
		          AS_IF([test -r "$jnih"], [
				          EXTRA_JDK_INC_DIR=`dirname $jnih`])
              ])
       done
       ])

        # cephfs_java_test only makes sense if java is already turned on
	# setup CLASSPATH for Debian default junit4.jar package 
        #
        # Configuring --with-debug and --enable-cephfs-java will throw an error if
        # JUnit4 cannot be found. While currently this works for users who have
        # installed via the package manager on Ubuntu, we need to expand this
        # check to 1 support other distrubtions and 2 allow users to influence
        # the search path.
        AS_IF([test "x$with_debug" = "xyes"], [
        	dir='/usr/share/java'
	        junit4_jar=`find $dir -name junit4.jar | head -n 1`
		AS_IF([test -r "$junit4_jar"], [
		      EXTRA_CLASSPATH_JAR=`dirname $junit4_jar`/junit4.jar
		      AC_SUBST(EXTRA_CLASSPATH_JAR)
		      [have_junit4=1]], [
		      AC_MSG_NOTICE([Cannot find junit4.jar (apt-get install junit4)])
		      [have_junit4=0]])])

        AC_CHECK_CLASSPATH
        AC_PROG_JAVAC
        AC_PROG_JAVAH
        AC_PROG_JAR

        CLASSPATH=$CLASSPATH:$EXTRA_CLASSPATH_JAR
        export CLASSPATH
        AC_MSG_NOTICE([classpath - $CLASSPATH])

        # Check for jni.h
	CPPFLAGS_save=$CPPFLAGS

	AS_IF([test -n "$EXTRA_JDK_INC_DIR"],
		  [JDK_CPPFLAGS="-I$EXTRA_JDK_INC_DIR"
		   AS_IF([test -d "$EXTRA_JDK_INC_DIR/linux"],
				 [JDK_CPPFLAGS="$JDK_CPPFLAGS -I$EXTRA_JDK_INC_DIR/linux"])
		   CPPFLAGS="$CPPFLAGS $JDK_CPPFLAGS"])

	AC_CHECK_HEADER([jni.h], [], AC_MSG_ERROR([Cannot find header 'jni.h'. Try setting --with-jdk-dir]))

	CPPFLAGS=$CPPFLAGS_save

	# Setup output var
	AC_SUBST(JDK_CPPFLAGS)
fi
AM_CONDITIONAL(HAVE_JUNIT4, [test "$have_junit4" = "1"])

#
# FreeBSD has it in base.
#
if test x"$freebsd" != x"yes"; then
PKG_CHECK_MODULES([LIBEDIT], [libedit >= 2.11],
                [], AC_MSG_FAILURE([No usable version of libedit found.]))
else
	LIBEDIT_LIBS="-ledit"
fi

#libatomic-ops? You want it!
AC_ARG_WITH([libatomic-ops],
            [AS_HELP_STRING([--without-libatomic-ops],
	    [disable libatomic-ops for the atomic_t type])],
            [],
            [with_libatomic_ops=yes])
AS_IF([test "x$with_libatomic_ops" != xno],
	    [AC_CHECK_HEADER([atomic_ops.h],
             [HAVE_ATOMIC_OPS=1],
             [AC_MSG_FAILURE(
                   [no libatomic-ops found (use --without-libatomic-ops to disable)])
              ])])
AS_IF([test "$HAVE_ATOMIC_OPS" = "1"],
	[
         AC_CHECK_SIZEOF(AO_t, [], [
                                #include <atomic_ops.h>
                                ])
         ],
	[AC_DEFINE([NO_ATOMIC_OPS], [1], [Defined if you do not have atomic_ops])])


AM_CONDITIONAL(WITH_LIBATOMIC, [test "$HAVE_ATOMIC_OPS" = "1"])

# newsyn?  requires mpi.
#AC_ARG_WITH([newsyn],
#            [AS_HELP_STRING([--with-newsyn], [build newsyn target requires mpi])],
#            [],
#            [with_newsyn=no])

AC_ARG_WITH([ocf],
            [AS_HELP_STRING([--with-ocf], [build OCF-compliant cluster resource agent])],
            ,
            [with_ocf=no])
AM_CONDITIONAL(WITH_OCF, [ test "$with_ocf" = "yes" ])

# check is snappy-devel is installed, needed by leveldb
AC_CHECK_LIB([snappy], [snappy_compress], [], [AC_MSG_FAILURE([libsnappy not found])])
# use system leveldb
AC_CHECK_LIB([leveldb], [leveldb_open], [], [AC_MSG_FAILURE([libleveldb not found])], [-lsnappy -lpthread])
# see if we can use bloom filters with leveldb
AC_LANG_PUSH([C++])
AC_CHECK_HEADER([leveldb/filter_policy.h], [AC_DEFINE([HAVE_LEVELDB_FILTER_POLICY], [1], [Defined if LevelDB supports bloom filters ])])
AC_LANG_POP([C++])

# Find supported SIMD / SSE extensions supported by the compiler
AX_INTEL_FEATURES()

# use rocksdb
AC_ARG_WITH([librocksdb],
            [AS_HELP_STRING([--with-librocksdb], [build rocksdb support])],
            [],
            [with_librocksdb=no])
AS_IF([test "x$with_librocksdb" = "xyes"],
            [PKG_CHECK_MODULES([LIBROCKSDB], [rocksdb], [], [true])])
AS_IF([test "x$with_librocksdb" = "xyes"],
            [AC_DEFINE([HAVE_LIBROCKSDB], [1], [Defined if you have librocksdb enabled])])
AM_CONDITIONAL(WITH_LIBROCKSDB, [ test "$with_librocksdb" = "yes" ])

#add rocksdb support
# check libz
AS_IF([test "x$with_librocksdb" = "xyes"],
            [AC_CHECK_LIB([z], [gzread], [], [AC_MSG_FAILURE([libz not found])])])
# check libbz2
AS_IF([test "x$with_librocksdb" = "xyes"],
            [AC_CHECK_LIB([bz2], [BZ2_bzCompressInit], [], [AC_MSG_FAILURE([libbz2 not found])])])
# check librt
AS_IF([test "x$with_librocksdb" = "xyes"],
            [AC_CHECK_LIB([rt], [clock_gettime], [], [AC_MSG_FAILURE([librt not found])])])
# use system rocksdb
#AC_CHECK_LIB([rocksdb], [rocksdb_open], [], [AC_MSG_FAILURE([librocks not found])],[-lsnappy -lpthread -lrt -lbz2 -lz])
#AC_CHECK_LIB([rocksdb], [open], [], [AC_MSG_FAILURE([librocks not found])],[-lsnappy -lpthread -lrt -lbz2 -lz])

# use system libs3?
AC_ARG_WITH([system-libs3],
	[AS_HELP_STRING([--with-system-libs3], [use system libs3])],
	,
	[with_system_libs3=no])
AS_IF([test "x$with_system_libs3" = xyes],
            [AC_CHECK_LIB([s3], [S3_initialize], [true], [AC_MSG_FAILURE([libs3 not found])], [-lpthread])])
AS_IF([test "x$with_system_libs3" = xcheck],
            [AC_SEARCH_LIBS([S3_initialize], [s3], [with_system_libs3=yes], [true], [-lpthread])])
AM_CONDITIONAL(WITH_SYSTEM_LIBS3, [ test "$with_system_libs3" = "yes" ])

# rest-bench?
AC_ARG_WITH([rest-bench],
	    [AS_HELP_STRING([--with-rest-bench], [enables rest-bench])],
	    [],
	    [with_rest_bench=no])
AM_CONDITIONAL(WITH_REST_BENCH, [ test "$with_rest_bench" = "yes" ])

# use libaio?
AC_ARG_WITH([libaio],
            [AS_HELP_STRING([--without-libaio], [disable libaio use by journal])],
            ,
            [with_libaio=yes])
AS_IF([test "x$with_libaio" != xno],
	    [AC_CHECK_LIB([aio], [io_submit], [true], AC_MSG_FAILURE([libaio not found]))])
AS_IF([test "x$with_libaio" != xno],
	    [AC_CHECK_HEADER([libaio.h])])	
AS_IF([test "$with_libaio" = "yes"],
	    [AC_DEFINE([HAVE_LIBAIO], [1], [Defined if you don't have atomic_ops])])
AM_CONDITIONAL(WITH_LIBAIO, [ test "$with_libaio" = "yes" ])

# use libxfs?
AC_ARG_WITH([libxfs],
  [AS_HELP_STRING([--without-libxfs], [disable libxfs use by FileStore])],
  [],
  [with_libxfs=yes])
AS_IF([test "x$with_libxfs" != "xno"], [
  # xfs/xfs.h presence and XFS_XFLAG_EXTSIZE define
  AC_CHECK_HEADER([xfs/xfs.h], [], AC_MSG_ERROR(
    [xfs/xfs.h not found (--without-libxfs to disable)]))
  AC_MSG_CHECKING([for XFS_XFLAG_EXTSIZE in xfs/xfs.h])
  AC_EGREP_CPP([yes_have_xfs_xflag_extsize], [
    #include <xfs/xfs.h>
    #ifdef XFS_XFLAG_EXTSIZE
    yes_have_xfs_xflag_extsize
    #endif
  ], [
    AC_MSG_RESULT([yes])
    AC_DEFINE([HAVE_LIBXFS], [1], [Define to 1 if you have libxfs])
  ], [
    AC_MSG_RESULT([no])
    AC_MSG_ERROR([XFS_XFLAG_EXTSIZE not found (--without-libxfs to disable)])
  ])
])
AM_CONDITIONAL(WITH_LIBXFS, [test "x$with_libxfs" != "xno"])

# use libzfs
AC_ARG_WITH([libzfs],
	    [AS_HELP_STRING([--with-libzfs], [build ZFS support])],
	    ,
	    [with_libzfs=no])
AS_IF([test "x$with_libzfs" = xyes],
	    [PKG_CHECK_MODULES([LIBZFS], [zfs], [], [true])])
AS_IF([test "x$with_libzfs" = xyes],
	    [AC_DEFINE([HAVE_LIBZFS], [1], [Defined if you have libzfs enabled])])
AM_CONDITIONAL(WITH_LIBZFS, [ test "$with_libzfs" = "yes" ])

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_HEADER_SYS_WAIT


# spirit?
AC_LANG([C++])

AC_CHECK_HEADER([boost/spirit/include/classic_core.hpp], [],
  [AC_CHECK_HEADER([boost/spirit.hpp], [use_bspirit_old_hdr=yes],
    AC_MSG_FAILURE(["Can't find boost spirit headers"]))])
AM_CONDITIONAL(USE_BOOST_SPIRIT_OLD_HDR, [test "$use_bspirit_old_hdr" = "yes"])

AC_CHECK_HEADER([boost/random/discrete_distribution.hpp],
	[AC_DEFINE([HAVE_BOOST_RANDOM_DISCRETE_DISTRIBUTION], [], [have boost::random::discrete_distribution])],
	[])

AC_CHECK_HEADER([boost/statechart/state.hpp], [],
    AC_MSG_FAILURE(["Can't find boost statechart headers; need 1.34 or later"]))
AC_CHECK_HEADER([boost/program_options/option.hpp], [],
    AC_MSG_FAILURE(["Can't find boost program_options headers"]))

# If we have the boost system library installed, then we may want to link
# with it.
AC_CHECK_LIB(boost_system-mt, main, [],
    [AC_CHECK_LIB(boost_system, main, [],
        AC_MSG_NOTICE(["Boost system library not found."]))])

# Find the right boost_thread library.
AC_CHECK_LIB(boost_thread-mt, main, [],
    [AC_CHECK_LIB(boost_thread, main, [],
        AC_MSG_FAILURE(["Boost thread library not found."]))])

#
# Check for boost_program_options library (defines BOOST_PROGRAM_OPTIONS_LIBS).
#
BOOST_PROGRAM_OPTIONS_LIBS=""
saved_LIBS="${LIBS}"
LIBS=""
AC_CHECK_LIB(boost_program_options-mt, main, [],
    [AC_CHECK_LIB(boost_program_options, main, [],
        AC_MSG_FAILURE(["Boost program options library not found."]))])
BOOST_PROGRAM_OPTIONS_LIBS="${LIBS}"
LIBS="${saved_LIBS}"
AC_SUBST(BOOST_PROGRAM_OPTIONS_LIBS)

AC_LANG([C])

AC_CHECK_MEMBER([struct fiemap_extent.fe_logical],
	[AC_DEFINE([HAVE_FIEMAP_H], [], [linux/fiemap.h was found, fiemap ioctl will be used])],
	[AC_MSG_NOTICE([linux/fiemap.h was not found or not usable; using local Ceph copy])],
	[[#include <linux/fiemap.h>]])

AC_CHECK_HEADERS([ \
	arpa/inet.h \
	arpa/nameser_compat.h \
	linux/version.h \
	netdb.h \
	netinet/in.h \
	sys/file.h \
	sys/ioctl.h \
	sys/mount.h \
	sys/param.h \
	sys/socket.h \
	sys/statvfs.h \
	sys/time.h \
	sys/vfs.h \
	sys/xattr.h \
	syslog.h \
	utime.h \
])

# sync_file_range
AC_CHECK_FUNC([sync_file_range],
	[AC_DEFINE([HAVE_SYNC_FILE_RANGE], [], [sync_file_range(2) is supported])],
	[])

# fallocate
AC_CHECK_FUNC([fallocate],
	[AC_DEFINE([CEPH_HAVE_FALLOCATE], [], [fallocate(2) is supported])],
	[])

#
# Test for time-related `struct stat` members.
#

AC_CHECK_MEMBER([struct stat.st_mtim.tv_nsec],
  [AC_DEFINE(HAVE_STAT_ST_MTIM_TV_NSEC, 1,
    [Define if you have struct stat.st_mtim.tv_nsec])])

AC_CHECK_MEMBER([struct stat.st_mtimespec.tv_nsec],
  [AC_DEFINE(HAVE_STAT_ST_MTIMESPEC_TV_NSEC, 1,
    [Define if you have struct stat.st_mtimespec.tv_nsec])])

# splice/tee
AC_CHECK_FUNC([splice],
	[AC_DEFINE([CEPH_HAVE_SPLICE], [], [splice(2) is supported])],
	[])

# F_SETPIPE_SZ in fcntl.h
AC_MSG_CHECKING([for F_SETPIPE_SZ in fcntl.h])
AC_EGREP_CPP([yes_have_f_setpipe_sz], [
  #define _GNU_SOURCE
  #include <fcntl.h>
  #ifdef F_SETPIPE_SZ
  yes_have_f_setpipe_sz
  #endif
], [
  AC_MSG_RESULT([yes])
  AC_DEFINE([CEPH_HAVE_SETPIPE_SZ], [], [F_SETPIPE_SZ is supported])
], [
  AC_MSG_RESULT([no])
  AC_MSG_NOTICE([F_SETPIPE_SZ not found, zero-copy may be less efficent])
])

AC_CHECK_FUNCS([posix_fallocate])
AC_CHECK_HEADERS([sys/prctl.h])
AC_CHECK_FUNCS([prctl])
AC_CHECK_FUNCS([pipe2])
AC_CHECK_FUNCS([posix_fadvise])

AC_MSG_CHECKING([for fdatasync])
AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <unistd.h>
]], [[
#if defined(_POSIX_SYNCHRONIZED_IO) && _POSIX_SYNCHRONIZED_IO > 0
return fdatasync(0);
#else
#error Not supported
#endif
]])], [
AC_MSG_RESULT([yes])
AC_DEFINE([HAVE_FDATASYNC], 1, [Define to 1 if you have fdatasync.])
], [
AC_MSG_RESULT([no])
])

#
# Check for pthread spinlock (depends on ACX_PTHREAD)
#
saved_LIBS="$LIBS"
saved_CFLAGS="$CFLAGS"
LIBS="$PTHREAD_LIBS $LIBS"
CFLAGS="$PTHREAD_CFLAGS $CFLAGS"
AC_CHECK_FUNC([pthread_spin_init],
  [AC_DEFINE(HAVE_PTHREAD_SPINLOCK, 1, [Define if you have pthread_spin_init])])
LIBS="$saved_LIBS"
CFLAGS="$saved_CFLAGS"

AC_CHECK_TYPES([int8_t, uint8_t, int16_t, uint16_t, int32_t, uint32_t,
  int64_t, uint64_t])

dnl check for Linux types
AC_CHECK_HEADERS([linux/types.h])
AC_CHECK_TYPES([__u8, __s8, __u16, __s16, __u32, __s32, __u64, __s64, __le16,
  __be16, __le32, __be32, __le64, __be64], [], [], [[#include <linux/types.h>]])

# Checks for typedefs, structures, and compiler characteristics.
#AC_HEADER_STDBOOL
#AC_C_CONST
#AC_TYPE_UID_T
#AC_C_INLINE
#AC_TYPE_INT16_T
#AC_TYPE_INT32_T
#AC_TYPE_INT64_T
#AC_TYPE_INT8_T
#AC_TYPE_MODE_T
#AC_TYPE_OFF_T
#AC_TYPE_PID_T
#AC_TYPE_SIZE_T
#AC_TYPE_SSIZE_T
#AC_CHECK_MEMBERS([struct stat.st_blksize])
#AC_STRUCT_ST_BLOCKS
#AC_CHECK_MEMBERS([struct stat.st_rdev])
#AC_HEADER_TIME
#AC_STRUCT_TM
#AC_TYPE_UINT16_T
#AC_TYPE_UINT32_T
#AC_TYPE_UINT64_T
#AC_TYPE_UINT8_T

# Checks for library functions.
#AC_FUNC_CHOWN
#AC_FUNC_CLOSEDIR_VOID
#AC_FUNC_ERROR_AT_LINE
#AC_FUNC_FORK
#AC_PROG_GCC_TRADITIONAL
#AC_FUNC_LSTAT
#AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
#AC_FUNC_MALLOC   # this causes annoying rpl_malloc error on some machines; skip it
#AC_FUNC_MEMCMP
#AC_FUNC_MMAP
#AC_FUNC_REALLOC
#AC_FUNC_SELECT_ARGTYPES
#AC_TYPE_SIGNAL
#AC_FUNC_STAT
#AC_FUNC_UTIME_NULL
#AC_CHECK_FUNCS([bzero fchdir fdatasync floor ftruncate getcwd gethostbyname gethostname gettimeofday inet_ntoa localtime_r memmove memset mkdir munmap pow rmdir select socket sqrt strcasecmp strchr strerror strstr utime])

# check for return type (and presence) if strerror_r in C++ mode
AC_LANG_PUSH([C++])
AC_FUNC_STRERROR_R
AC_LANG_POP([C++])

AM_CONDITIONAL(WITH_BUILD_TESTS, test "$WITH_BUILD_TESTS" = "1")

AM_PATH_PYTHON([2.4],
	[], [AC_MSG_FAILURE([Failed to find Python 2.4 or newer])])

AC_CONFIG_HEADERS([src/acconfig.h])
AC_CONFIG_FILES([Makefile
	src/Makefile
	src/ocf/Makefile
	src/ocf/ceph
	src/ocf/rbd
	src/java/Makefile
	man/Makefile
	ceph.spec])
AC_OUTPUT
