#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# Autoconf
AC_PREREQ(2.59)

# NOTE: This version is _only_ used for naming the tarball.  The
# VERSION define is not used by the code.  It gets a version string
# from 'git describe'; see src/ceph_ver.[ch]

AC_INIT([ceph], [10.0.5], [ceph-devel@vger.kernel.org])

AX_CXX_COMPILE_STDCXX_11(, mandatory)

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

AC_ARG_WITH([man-pages],
    [AS_HELP_STRING([--with-man-pages], [build man pages])],
    [],
    [with_man_pages=check])
AS_IF([test "x$with_man_pages" != "xno"],
  [AC_CHECK_PROGS(SPHINX_BUILD, sphinx-1.0-build sphinx-build)
   AS_IF([test -z "$SPHINX_BUILD" && \
          test "x$with_man_pages" = "xyes"],
     [AC_MSG_ERROR([sphinx-build not found (python-sphinx)])])])
AM_CONDITIONAL(WITH_MAN_PAGES, test -n "$SPHINX_BUILD")

AC_CONFIG_MACRO_DIR([m4])

AC_CONFIG_SUBDIRS([src/gmock])

# Environment
AC_CANONICAL_HOST
AC_CANONICAL_TARGET

# Fix automake problems in 1.12
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])

# Automake
AM_PROG_CC_C_O
AM_PROG_LIBTOOL
AM_PROG_AS


AM_INIT_AUTOMAKE([foreign parallel-tests tar-ustar])
# enable make V=0 (if automake >1.11)
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

# Platform
case "${target_os}" in
darwin*)
	AC_DEFINE([DARWIN], [1], [Define if darwin/osx])
    darwin="yes"
	;;
linux*)
	linux="yes"
	;;
freebsd*)
	freebsd="yes"
	;;
solaris*)
	solaris="yes"
	;;
aix*)
	aix="yes"
	;;
esac
AM_CONDITIONAL(LINUX, test x"$linux" = x"yes")
AM_CONDITIONAL(FREEBSD, test x"$freebsd" = x"yes")
AM_CONDITIONAL(DARWIN, test x"$darwin" = x"yes")
AM_CONDITIONAL(SOLARIS, test x"$solaris" = x"yes")
AM_CONDITIONAL(AIX, test x"$aix" = x"yes")

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

# Find out what to build (default is most of these)

# rados?
AC_ARG_WITH([rados],
	[AS_HELP_STRING([--with-rados], [build with librados support])],
	[],
	[with_rados=yes])
AM_CONDITIONAL(WITH_RADOS, test "$with_rados" = "yes")
#AS_IF([test "$with_rados" = "yes"], [AC_DEFINE([WITH_RADOS])])

# rbd?
# rbd requires rados
AC_ARG_WITH([rbd],
	[AS_HELP_STRING([--with-rbd], [build rbd files])],
	[],
	[with_rbd=yes])
AM_CONDITIONAL(WITH_RBD, test "$with_rbd" = "yes")
#AS_IF([test "$with_rbd" = "yes"], [AC_DEFINE([WITH_RADOS, WITH_RBD])])

AC_ARG_WITH([cython],
	[AS_HELP_STRING([--with-cython], [build python bindings for libraries])],
	[],
	[with_cython=yes])
AM_CONDITIONAL(WITH_CYTHON, test "$with_cython" = "yes")

# cephfs?
# cephfs requires rados
AC_ARG_WITH([cephfs],
	[AS_HELP_STRING([--with-cephfs], [build cephfs files])],
	[],
	[with_cephfs=yes])
AM_CONDITIONAL(WITH_CEPHFS, test "$with_cephfs" = "yes")
#AS_IF([test "$with_cephfs" = "yes"], [AC_DEFINE([WITH_RADOS, WITH_CEPHFS])])

# radosgw?
# radosgw requires rados
AC_ARG_WITH([radosgw],
	[AS_HELP_STRING([--with-radosgw], [build RADOS gateway])],
	[],
	[with_radosgw=check])
# AM_CONDITIONAL is defined later -- we need to check whether we can enable radosgw if no option is present
#AS_IF([test "$with_radosgw" = "yes"], [AC_DEFINE([WITH_RADOS, WITH_RADOSGW])])

AC_ARG_WITH([selinux],
	[AS_HELP_STRING([--with-selinux], [build SELinux policy])],
	[],
	[with_selinux=no])
AM_CONDITIONAL(WITH_SELINUX, test "$with_selinux" = "yes")
if test "x$with_selinux" = x"yes"; then
	AC_CHECK_FILE([/usr/share/selinux/devel/policyhelp], [true], [AC_MSG_FAILURE([No SELinux found])])
	AC_CHECK_FILE([/usr/share/selinux/devel/include/Makefile], [true],  [AC_MSG_FAILURE([No SELinux Makefile found])])
fi

# radosstriper?
AC_ARG_WITH([radosstriper],
	[AS_HELP_STRING([--with-radosstriper], [build radosstriper files])],
	[],
	[with_radosstriper=yes])
AM_CONDITIONAL(WITH_RADOSSTRIPER, test "$with_radosstriper" = "yes")
#AS_IF([test "$with_radostriper" = "yes"], [AC_DEFINE([WITH_RADOS, WITH_RADOSSTRIPER])])

# mon?
AC_ARG_WITH([mon],
	[AS_HELP_STRING([--with-mon], [build Ceph monitor software files])],
	[],
	[with_mon=yes])
AM_CONDITIONAL(WITH_MON, test "$with_mon" = "yes")
#AS_IF([test "$with_mon" = "yes"], [AC_DEFINE([WITH_MON])])

# osd?
AC_ARG_WITH([osd],
	[AS_HELP_STRING([--with-osd], [build object store daemon files])],
	[],
	[with_osd=yes])
AM_CONDITIONAL(WITH_OSD, test "$with_osd" = "yes")
#AS_IF([test "$with_osd" = "yes"], [AC_DEFINE([WITH_OSD])])

# mds?
AC_ARG_WITH([mds],
	[AS_HELP_STRING([--with-mds], [build mds files])],
	[],
	[with_mds=yes])
AM_CONDITIONAL(WITH_MDS, test "$with_mds" = "yes")
#AS_IF([test "$with_mds" = "yes"], [AC_DEFINE([WITH_MDS])])

# client?
AC_ARG_ENABLE([client],
	[AS_HELP_STRING([--enable-client], [enable client-side build])],
	[],
	[enable_client=yes])
AM_CONDITIONAL([ENABLE_CLIENT], test "$enable_client" = "yes")
#AS_IF([test "$enable_client" = "yes"], [AC_DEFINE([WITH_RADOS, WITH_RBD, ENABLE_CLIENT])])

# server?
AC_ARG_ENABLE([server],
	[AS_HELP_STRING([--enable-server], [enable server-side build ])],
	[],
	[enable_server=yes])
AM_CONDITIONAL(ENABLE_SERVER, test "$enable_server" = "yes")
#AS_IF([test "$enable_server" = "yes"], [AC_DEFINE([WITH_MON, WITH_OSD, WITH_MDS, ENABLE_SERVER])])

# cython is required to build python bindings for libraries
if test x"$with_cython" = xyes; then
    AC_CHECK_PROG(CYTHON_CHECK, cython, yes)
    if test x"$CYTHON_CHECK" != xyes; then
        AC_MSG_FAILURE([cython not found])
    fi
    AC_CHECK_PROG(PYTHON_CONFIG_CHECK, python-config, yes)
    if test x"$PYTHON_CONFIG_CHECK" != xyes; then
        AC_MSG_FAILURE([python-config not found])
    fi
    PYTHON_CFLAGS=`python-config --cflags`
    AC_SUBST(PYTHON_CFLAGS)
    PYTHON_LDFLAGS=`python-config --ldflags`
    AC_SUBST(PYTHON_LDFLAGS)
fi

# cond-check if snappy-devel is installed, needed by leveldb that is need by server parts of the project
AS_IF([test "$enable_server" = "yes" -a \( "$with_osd" = "yes" -o "$with_mon" = "yes" \)],
	[AC_CHECK_LIB([snappy], [snappy_compress], [true], [AC_MSG_FAILURE([libsnappy not found])])])

# cond-check leveldb, necessary if server, osd or mon enabled
AS_IF([test "$enable_server" = "yes" -a \( "$with_osd" = "yes" -o "$with_mon" = "yes" \)],
	[AC_CHECK_LIB([leveldb], [leveldb_open], [true], [AC_MSG_FAILURE([libleveldb not found])], [-lsnappy -lpthread])])

# Check for yasm
AC_CHECK_PROG(YASM_CHECK, yasm, yes)
if test x"$YASM_CHECK" = x"yes"; then
  if yasm -f elf64 src/common/crc32c_intel_fast_asm.S -o /dev/null; then
     echo 'we have a modern and working yasm'
     if test `arch` = "x86_64" ; then
        echo 'we are x86_64'
        arch_x32=0
        AC_COMPILE_IFELSE([AC_LANG_PROGRAM([], [[
          #if defined(__x86_64__) && defined(__ILP32__)
          #error x32
          #endif]])], [], [arch_x32=1])
        if test $arch_x32 -eq 0 ; then
           echo 'we are not x32'
           AC_DEFINE([HAVE_GOOD_YASM_ELF64], [1], [we have a recent yasm and are x86_64])
           with_good_yasm=yes
  
           if yasm -f elf64 -i src/erasure-code/isa/isa-l/include/ src/erasure-code/isa/isa-l/erasure_code/gf_vect_dot_prod_avx2.asm.s -o /dev/null 2> /dev/null ; then
              echo 'yasm can also build the isa-l stuff'
              AC_DEFINE([HAVE_BETTER_YASM_ELF64], [1], [yasm can also build the isa-l])
  	    with_better_yasm=yes
           else
              echo "yasm doesn't build the isa-l stuff"
  	 fi
        else
           echo 'we are x32; no yasm for you'
        fi
     else
        echo 'we are not x86_64 && !x32'
     fi
  else
     echo 'we do not have a modern/working yasm'
  fi
fi
AM_CONDITIONAL(WITH_GOOD_YASM_ELF64, test "$with_good_yasm" = "yes")
AM_CONDITIONAL(WITH_BETTER_YASM_ELF64, test "$with_better_yasm" = "yes")

# check for better yasm

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
AC_CHECK_CC_FLAG([-Werror=format-security], [WARN_ERROR_FORMAT_SECURITY])

AC_CHECK_CC_FLAG([-rdynamic], [RDYNAMIC_FLAG])

# Check for compiler VTA support
AX_CHECK_COMPILE_FLAG([-fvar-tracking-assignments], [HAS_VTA_SUPPORT=1], [HAS_VTA_SUPPORT=0])
AM_CONDITIONAL(COMPILER_HAS_VTA, [test "$HAS_VTA_SUPPORT" = 1])

AX_CXX_STATIC_CAST
AX_C_VAR_FUNC
AX_C_PRETTY_FUNC

# Checks for libraries.
ACX_PTHREAD


#Linux only dependencies
if test x"$linux" = x"yes"; then

  # rbd {map,unmap,showmapped} dependencies, Linux only
  if test x"$with_rbd" = x"yes"; then

    # libblkid
    AC_CHECK_HEADER([blkid/blkid.h], [],
      AC_MSG_ERROR([blkid/blkid.h not found (libblkid-dev, libblkid-devel)]))
    AC_CHECK_LIB([blkid], [blkid_get_cache], [true],
      AC_MSG_FAILURE([libblkid not found]))
    AC_CHECK_LIB([blkid], [blkid_find_dev_with_tag], [true],
      AC_MSG_FAILURE([libblkid not found]))
    AC_CHECK_LIB([blkid], [blkid_dev_devname], [true],
      AC_MSG_FAILURE([libblkid not found]))
    AC_CHECK_LIB([blkid], [blkid_devno_to_wholedisk], [true],
      AC_MSG_FAILURE([libblkid not found]))

    # libudev
    AC_CHECK_HEADER([libudev.h], [],
      AC_MSG_ERROR([libudev.h not found (libudev-dev, libudev-devel)]))
    AC_CHECK_LIB([udev], [udev_monitor_receive_device], [true],
      AC_MSG_FAILURE([libudev not found]))

    # libexpat
    AC_CHECK_HEADER([expat.h], [],
      AC_MSG_ERROR([expat.h not found (libexpat-devel)]))
    AC_CHECK_LIB([expat], [XML_Parse], [true],
      AC_MSG_FAILURE([libexpat not found]))
  fi
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
AC_DEFINE(HAVE_RES_NQUERY, 1, [Define if you have res_nquery])

dnl check for libkeyutils on linux
KEYUTILS_LIB=""
AS_IF([test x"$linux" = x"yes"], [
  AC_CHECK_LIB([keyutils], [add_key], [KEYUTILS_LIB="-lkeyutils"], [
    AC_MSG_FAILURE([libkeyutils not found (libkeyutils-dev, keyutils-libs-devel)])])])
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

AC_ARG_ENABLE(gitversion,
    [AC_HELP_STRING([--enable-gitversion], [build Ceph with git version string])],
    [], [enable_gitversion=yes])

AM_CONDITIONAL(NO_GIT_VERSION, [test "x$enable_gitversion" = "xno"])

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
	        	  [AC_MSG_FAILURE([--with-profiler was given but libprofiler (libgoogle-perftools-dev on debian) not found])])
            ],
	    [])
AM_CONDITIONAL(WITH_PROFILER, test "$with_profiler" = "yes")
AS_IF([test "$with_profiler" = "yes"],
      [AC_DEFINE([HAVE_PROFILER], [1], [Define if you have perftools profiler enabled])],
      [])

# debug crap?
AC_ARG_WITH([debug],
            [AS_HELP_STRING([--with-debug], [build extra debug binaries and tests])],
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

# is radosgw available?
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
AS_IF([test "x$with_fuse" != xno], [
  PKG_CHECK_MODULES([LIBFUSE], [fuse],
    [HAVE_LIBFUSE=1
     AC_DEFINE([HAVE_LIBFUSE], [1], [Define if you have fuse])],
    [AC_MSG_FAILURE([no FUSE found (use --without-fuse to disable)])])
])
AM_CONDITIONAL(WITH_FUSE, [test "$HAVE_LIBFUSE" = "1"])

# jemalloc?
AC_ARG_WITH([jemalloc],
	    [AS_HELP_STRING([--with-jemalloc], [enable jemalloc for memory allocations])],
	    [],
	    [with_jemalloc=no])
JEMALLOC=
AS_IF([test "x$with_jemalloc" = xyes],
	    [AC_CHECK_LIB([jemalloc], [malloc],
	     [AC_SUBST([LIBJEMALLOC], ["-ljemalloc"])
	       AC_DEFINE([HAVE_LIBJEMALLOC], [1],
                         [Define if you have jemalloc])
	       HAVE_LIBJEMALLOC=1
	     ],
	    [AC_MSG_FAILURE(
		  [no jemalloc found (do not use --with-jemalloc)])])])
AM_CONDITIONAL(WITH_JEMALLOC, [test "$HAVE_LIBJEMALLOC" = "1"])

# tcmalloc-minimal?
AC_ARG_WITH([tcmalloc-minimal],
	    [AS_HELP_STRING([--with-tcmalloc-minimal], [enable minimal tcmalloc support for memory allocations])],
	    [],
	    [with_tcmalloc_minimal=no])

AS_IF([test "x$with_jemalloc" = "xyes"],[with_tcmalloc_minimal=no],[])

TCMALLOC_MINIMAL=
AS_IF([test "x$with_tcmalloc_minimal" != xno],
        [AC_CHECK_LIB([tcmalloc_minimal], [malloc],
         [AC_SUBST([LIBTCMALLOC], ["-ltcmalloc_minimal"])
	       AC_DEFINE([HAVE_LIBTCMALLOC_MINIMAL], [1],
	       		 [Define if you have tcmalloc])
	       HAVE_LIBTCMALLOC_MINIMAL=1
	     ],
	    [AC_MSG_FAILURE(
		  [no tcmalloc found (do not use --with-tcmalloc-minimal)])])])
AM_CONDITIONAL(WITH_TCMALLOC_MINIMAL, [test "$HAVE_LIBTCMALLOC_MINIMAL" = "1"])

# tcmalloc?
AC_ARG_WITH([tcmalloc],
	    [AS_HELP_STRING([--without-tcmalloc], [disable tcmalloc for memory allocations])],
	    [],
	    [with_tcmalloc=yes])

AS_IF([test "x$with_jemalloc" = "xyes"],[with_tcmalloc=no],[])
AS_IF([test "x$with_tcmalloc_minimal" = "xyes"],[with_tcmalloc=no],[])

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

AS_IF([test "$with_profiler" = yes -o \
            "$HAVE_LIBTCMALLOC" = "1" -o \
            "$HAVE_LIBTCMALLOC_MINIMAL" = "1"],
      [AC_LANG_PUSH([C++])
       AC_CHECK_HEADERS([gperftools/heap-profiler.h \
                         gperftools/malloc_extension.h \
                         gperftools/profiler.h])
       AC_LANG_POP([C++])])

# error out if --with-jemalloc and ! --without-tcmalloc
if test "x$with_jemalloc" = "xyes"; then
	if test "x$with_tcmalloc" != "xno"; then
		AC_MSG_FAILURE([--with-jemalloc called without --without-tcmalloc])
	fi
fi

#set pg ref debugging?
AC_ARG_ENABLE([pgrefdebugging],
	    [AS_HELP_STRING([--enable-pgrefdebugging], [enable pg ref debugging])],
			[], [enable_pgrefdebugging=no])
AS_IF([test "x$enable_pgrefdebugging" = "xyes"],
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
	        junit4_jar=`( find $dir -name junit4.jar;find $dir -name junit.jar ) | head -n 1`
		AS_IF([test -r "$junit4_jar"], [
		      EXTRA_CLASSPATH_JAR="$junit4_jar"
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
# Accelio and OFED
#
AC_ARG_ENABLE(xio,
    [AC_HELP_STRING([--enable-xio], [build Ceph Accelio transport])],
    [], [enable_xio=no])

AM_CONDITIONAL(ENABLE_XIO, [test "x$enable_xio" = "xyes"])

if test "x$enable_xio" = x"yes"; then
   AC_CHECK_HEADER([libxio.h], [], AC_MSG_ERROR([Cannot find header 'libxio.h'.]))
   AC_CHECK_LIB([xio], [xio_init], [], AC_MSG_FAILURE([Accelio libxio not found]))
   AC_CHECK_LIB([ibverbs], [ibv_query_device], [], AC_MSG_FAILURE([libibverbs not found]))
   AC_CHECK_LIB([rdmacm], [rdma_connect], [], AC_MSG_FAILURE([librdmacm not found]))

   # Also require boost-regex, used in address_helper
   AC_CHECK_LIB(boost_regex, main, [],
      AC_MSG_FAILURE(["Boost regex library not found."]))

   AC_DEFINE([HAVE_XIO], [1], [Accelio conditional compilation])

   XIO_LIBS="-lxio -libverbs -lrdmacm"
   AC_SUBST(XIO_LIBS)
fi

#
# SPDK
#
AC_ARG_WITH(spdk,
    [AC_HELP_STRING([--with-spdk], [build Ceph SPDK Support])], [], [with_spdk=no])

AM_CONDITIONAL(WITH_SPDK, [test "x$with_spdk" != "xno"])

if test "x$with_spdk" != x"no"; then
   PKG_CHECK_MODULES([LIBPCIACCESS], [pciaccess])
   AC_CHECK_HEADER([dpdk/rte_config.h], [],
     AC_MSG_ERROR([dpdk/rte_config.h not found (libdpdk-dev, libdpdk-devel)]))
   AC_CHECK_LIB([rte_eal], [rte_eal_init], [true],
     AC_MSG_FAILURE([DPDK rte_eal_init not found]),
                [-lrte_mempool -lrte_ring -lpthread -ldl])
   AC_SUBST([LIBDPDK_CFLAGS], [-I/usr/include/dpdk])
   AC_SUBST([LIBDPDK_LIBS], ['-lrte_eal -lrte_mempool -lrte_ring -lpthread'])
   AC_SUBST([LIBPCIACCESS_LIBS])
   AC_DEFINE([HAVE_SPDK], [1], [SPDK conditional compilation])
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

# cond-check snappy-devel and leveldb, necessary if server + osd or mon enabled
if test "$enable_server" = "yes" -a \( "$with_osd" = "yes" -o "$with_mon" = "yes" \); then
	AC_CHECK_LIB([snappy], [snappy_compress], [true], [AC_MSG_FAILURE([libsnappy not found])])
	AC_CHECK_LIB([leveldb], [leveldb_open], [true], [AC_MSG_FAILURE([libleveldb not found])], [-lsnappy -lpthread])

	# see if we can use bloom filters with leveldb
	AC_LANG_PUSH([C++])
	AC_CHECK_HEADER([leveldb/filter_policy.h], [AC_DEFINE([HAVE_LEVELDB_FILTER_POLICY], [1], [Defined if LevelDB supports bloom filters ])])
	AC_LANG_POP([C++])
fi

# Find supported SIMD / NEON / SSE extensions supported by the compiler
AX_ARM_FEATURES()
AM_CONDITIONAL(HAVE_NEON, [ test "x$ax_cv_support_neon_ext" = "xyes"])
AM_CONDITIONAL(HAVE_ARMV8_CRC, [ test "x$ax_cv_support_crc_ext" = "xyes"])
AX_INTEL_FEATURES()
AM_CONDITIONAL(HAVE_SSSE3, [ test "x$ax_cv_support_ssse3_ext" = "xyes"])
AM_CONDITIONAL(HAVE_SSE4_PCLMUL, [ test "x$ax_cv_support_pclmuldq_ext" = "xyes"])

# kinetic osd backend?
AC_ARG_WITH([kinetic],
            [AS_HELP_STRING([--with-kinetic], [build kinetic support])],
            [],
            [with_kinetic=no])
# no pkg-config support yet
#AS_IF([test "x$with_kinetic" = "xyes"],
#            [PKG_CHECK_MODULES([KINETIC], [kinetic_client], [], [true])])
AS_IF([test "x$with_kinetic" = "xyes"],
            [AC_DEFINE([HAVE_KINETIC], [1], [Defined if you have kinetic enabled])])
AM_CONDITIONAL(WITH_KINETIC, [ test "$with_kinetic" = "yes" ])

# check for c++11 (but do not enable it)
old_cxxflags="$CXXFLAGS"
AX_CXX_COMPILE_STDCXX_11([], [optional])
CXXFLAGS="$old_cxxflags"

# use rocksdb
AC_ARG_WITH([librocksdb],
            [AS_HELP_STRING([--with-librocksdb], [build rocksdb support])],
            [],
            [with_librocksdb=no])
AS_IF([test "x$with_librocksdb" = "xyes"],
            [PKG_CHECK_MODULES([LIBROCKSDB], [rocksdb], [], [true])])
AS_IF([test "x$with_librocksdb" = "xyes"],
            [AC_CHECK_LIB([rocksdb], [open], [], [AC_MSG_FAILURE([librocksdb not found])])])
AM_CONDITIONAL(WITH_DLIBROCKSDB, [ test "$with_librocksdb" = "yes" ])

AC_ARG_WITH([librocksdb-static],
            [AS_HELP_STRING([--with-librocksdb-static], [build rocksdb support])],
            [],
            [with_librocksdb_static=check])
AS_IF([test "x$with_librocksdb_static" = "xcheck" -a "x$HAVE_CXX11" = "x1" ],
            [with_librocksdb_static="yes"])
AS_IF([test "x$with_librocksdb_static" = "xyes"],
            [AC_DEFINE([HAVE_LIBROCKSDB], [1], [Defined if you have librocksdb enabled])])
AM_CONDITIONAL(WITH_SLIBROCKSDB, [ test "x$with_librocksdb_static" = "xyes" ])
AM_CONDITIONAL(WITH_LIBROCKSDB, [ test "x$with_librocksdb_static" = "xyes" -o "x$with_librocksdb" = "xyes" ])

# rocksdb detects bzlib and lz4 in its Makefile, which forces us to do the same.
AS_IF([test "x$with_librocksdb_static" = "xyes"], [
  AC_CHECK_HEADER([bzlib.h], [have_bzlib=yes])
  AC_CHECK_HEADER([lz4.h], [have_lz4=yes])])
AM_CONDITIONAL(HAVE_BZLIB, [test "x$have_bzlib" = "xyes"])
AM_CONDITIONAL(HAVE_LZ4, [test "x$have_lz4" = "xyes"])

# needs libcurl and libxml2
if test "x$with_rest_bench" = xyes && test "x$with_system_libs3" = xno; then
   AC_CHECK_LIB([curl], [curl_easy_init], [], AC_MSG_ERROR([libcurl not found]))
   AC_CHECK_LIB([xml2], [xmlParseChunk], [], AC_MSG_ERROR([libxml2 not found]))
fi

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

AC_CHECK_HEADER([boost/asio/coroutine.hpp],
	[AC_DEFINE([HAVE_BOOST_ASIO_COROUTINE], [], [have boost::asio::coroutine])],
	[])
AC_CHECK_HEADER([boost/statechart/state.hpp], [],
    AC_MSG_FAILURE(["Can't find boost statechart headers; need 1.34 or later"]))
AC_CHECK_HEADER([boost/regex.hpp], [],
    AC_MSG_FAILURE(["Can't find boost regex headers"]))
AC_CHECK_HEADER([boost/program_options/option.hpp], [],
    AC_MSG_FAILURE(["Can't find boost program_options headers"]))

# If we have the boost system library installed, then we may want to link
# with it.
AC_CHECK_LIB(boost_system-mt, main, [],
    [AC_CHECK_LIB(boost_system, main, [],
        AC_MSG_NOTICE(["Boost system library not found."]))])

AC_CHECK_LIB(boost_iostreams-mt, main, [],
    [AC_CHECK_LIB(boost_iostreams, main, [],
        AC_MSG_FAILURE(["Boost iostreams library not found."]))])

# Find the right boost_thread library.
BOOST_THREAD_LIBS=""
saved_LIBS="${LIBS}"
LIBS=""
AC_CHECK_LIB(boost_thread-mt, main, [],
    [AC_CHECK_LIB(boost_thread, main, [],
        AC_MSG_FAILURE(["Boost thread library not found."]))])
BOOST_THREAD_LIBS="${LIBS}"
LIBS="${saved_LIBS}"
AC_SUBST(BOOST_THREAD_LIBS)

# boost-random
BOOST_RANDOM_LIBS=""
saved_LIBS="${LIBS}"
LIBS=""
AC_CHECK_LIB(boost_random-mt, main, [],
    [AC_CHECK_LIB(boost_random, main, [],
        AC_MSG_FAILURE(["Boost random library not found."]))])
BOOST_RANDOM_LIBS="${LIBS}"
LIBS="${saved_LIBS}"
AC_SUBST(BOOST_RANDOM_LIBS)

# boost-regex
BOOST_REGEX_LIBS=""
saved_LIBS="${LIBS}"
LIBS=""
AC_CHECK_LIB(boost_regex-mt, main, [],
    [AC_CHECK_LIB(boost_regex, main, [],
        AC_MSG_FAILURE(["Boost regex library not found."]))])
BOOST_REGEX_LIBS="${LIBS}"
LIBS="${saved_LIBS}"
AC_SUBST(BOOST_REGEX_LIBS)

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
	sys/cdefs.h \
	syslog.h \
	utime.h \
	execinfo.h
])

# name_to_handle_at
AC_CHECK_FUNC([name_to_handle_at],
	[AC_DEFINE([HAVE_NAME_TO_HANDLE_AT], [], [name_to_handle_at exists])],
	[])

# sync_file_range
AC_CHECK_FUNC([sync_file_range],
	[AC_DEFINE([HAVE_SYNC_FILE_RANGE], [], [sync_file_range(2) is supported])],
	[])

# fallocate
AC_CHECK_FUNC([fallocate],
	[AC_DEFINE([CEPH_HAVE_FALLOCATE], [], [fallocate(2) is supported])],
	[])

# mallinfo
AC_CHECK_FUNC([mallinfo],
  [AC_DEFINE(HAVE_MALLINFO, 1, [Define if you have mallinfo])])

# getgrouplist
AC_CHECK_FUNCS([getgrouplist])

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
case "${target_os}" in
aix*)
	# AIX splice() is something else
	;;
*)
	AC_CHECK_FUNC([splice],
		[AC_DEFINE([CEPH_HAVE_SPLICE], [], [splice(2) is supported])],
		[])
	;;
esac

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
AC_CHECK_FUNCS([pwritev], AC_DEFINE([HAVE_PWRITEV], [1], [we have pwritev]))

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

AC_MSG_CHECKING([for sched.h])
AC_LANG_PUSH([C++])
AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#define _GNU_SOURCE
#include <sched.h>
]], [[
cpu_set_t cpuset;
CPU_ZERO(&cpuset);
CPU_SET(sched_getcpu(), &cpuset);
sched_setaffinity(0, sizeof(cpuset), &cpuset);
sched_yield();
return 0;
]])], [
AC_MSG_RESULT([yes])
AC_DEFINE([HAVE_SCHED], 1, [Define to 1 if you have sched.h.])
], [
AC_MSG_RESULT([no])
])
AC_LANG_POP([C++])


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


dnl Old versions of lttng-gen-tp leave out includes, and they break our stuff.
AC_MSG_CHECKING([if lttng-gen-tp is sane])
lttng_gen_tp_dir=`mktemp -d`
echo "#include <foo-inc.h>" > "$lttng_gen_tp_dir/foo.tp"
if ( ( cd "$lttng_gen_tp_dir" && lttng-gen-tp foo.tp -o foo.h && grep "#include <foo-inc.h>" foo.h ) > /dev/null 2>&1 ) ; then
  have_good_lttng_gen_tp=yes
else
  have_good_lttng_gen_tp=no
fi
rm -rf "$lttng_gen_tp_dir"
AC_MSG_RESULT([$have_good_lttng_gen_tp])

AC_ARG_WITH([lttng],
	[AS_HELP_STRING([--with-lttng], [Trace with LTTng])])
AS_IF([test "x$with_lttng" = "xno"], [use_lttng=no],
      [test "x$with_lttng" = "xyes"], [use_lttng=yes],
      [test "x$have_good_lttng_gen_tp" = "xyes"], [use_lttng=yes; AC_MSG_NOTICE([lttng auto-enabled])],
      [use_lttng=no; AC_MSG_NOTICE([lttng auto-disabled])])
AM_CONDITIONAL([WITH_LTTNG], test x"$use_lttng" = x"yes")
AM_COND_IF([WITH_LTTNG], [
  AC_DEFINE([WITH_LTTNG], [1], [Define if you want to use LTTng])

  AC_CHECK_HEADER([lttng/tracepoint.h], [],
    AC_MSG_ERROR([lttng/tracepoint.h not found (liblttng-ust-dev, lttng-ust-devel)]))

  AC_CHECK_PROG([LTTNG_GEN_TP_CHECK], [lttng-gen-tp], [yes])
  if test x"$LTTNG_GEN_TP_CHECK" != "xyes"; then
      AC_MSG_FAILURE([lttng-gen-tp not found])
  fi
  AC_SUBST([LTTNG_GEN_TP_PROG], [lttng-gen-tp])

  if test x"$have_good_lttng_gen_tp" != "xyes"; then
      AC_MSG_FAILURE([lttng-gen-tp does not behave properly])
  fi

  AC_MSG_CHECKING([if time_t is an integer])
  AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[#include <time.h>]], [[
      struct {
          unsigned int time_t_is_integer: ((time_t) 1.5 == 1) ? 1 : -1;
      } x;
      return 0;
  ]])], [
     AC_MSG_RESULT([yes])
  ], [
     AC_MSG_RESULT([no])
     AC_MSG_FAILURE([time_t is not an integer.  We assume this for tracing.])
  ])

  AC_MSG_CHECKING([if time_t fits in uint64_t])
  AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[#include <time.h>
  #include <inttypes.h>]], [[
      struct {
          unsigned int time_t_fits_in_uin64_t: (sizeof(time_t) <= sizeof(uint64_t)) ? 1 : -1;
      } x;
      return 0;
  ]])], [
     AC_MSG_RESULT([yes])
  ], [
     AC_MSG_RESULT([no])
     AC_MSG_FAILURE([time_t is larger than uint64_t.  We assume it can be cast to uint64_t, for tracing.])
  ])
], [
  AC_DEFINE([tracepoint(...)], [], [LTTng is disabled, so define this macro to be nothing.])
])


AC_CHECK_HEADERS([babeltrace/ctf/events.h babeltrace/babeltrace.h])
AC_CHECK_DECL([BT_CLOCK_REAL],
              [have_good_babeltrace=yes],
              [have_good_babeltrace=no],
              [[#include <babeltrace/babeltrace.h>]])
AC_ARG_WITH([babeltrace],
	[AS_HELP_STRING([--with-babeltrace], [Enable Babeltrace])])
AS_IF([test "x$with_babeltrace" = "xno"], [use_babeltrace=no],
      [test "x$with_babeltrace" = "xyes"], [use_babeltrace=yes],
      [test "x$ac_cv_header_babeltrace_ctf_events_h$ac_cv_header_babeltrace_babeltrace_h$have_good_babeltrace" = "xyesyesyes"], [use_babeltrace=yes; AC_MSG_NOTICE([babeltrace auto-enabled])],
      [use_babeltrace=no; AC_MSG_NOTICE([babeltrace auto-disabled])])
AM_CONDITIONAL([WITH_BABELTRACE], test x"$use_babeltrace" = x"yes")
AM_COND_IF([WITH_BABELTRACE], [
  AC_DEFINE([WITH_BABELTRACE], [1], [Define if you want to use Babeltrace])

  AC_CHECK_HEADER([babeltrace/babeltrace.h], [],
    AC_MSG_ERROR([babeltrace/babeltrac.h not found (libbabeltrace-dev, libbabeltrace-devel)]))

  AC_CHECK_HEADER([babeltrace/ctf/events.h], [],
    AC_MSG_ERROR([babeltrace/ctf/events.h not found (libbabeltrace-ctf-dev, libbabeltrace-devel)]))
])

dnl check for valgrind
AC_ARG_ENABLE([valgrind],
              [AS_HELP_STRING([--enable-valgrind], [enable valgrind unit tests])],
              [enable_valgrind=$enableval], [enable_valgrind=])
AC_CHECK_PROG(HAVE_VALGRIND, valgrind, yes)
AS_IF(
  [test "x$HAVE_VALGRIND" = "x"], AS_IF([test "x$enable_valgrind" = "xyes"], [AC_MSG_ERROR([valgrind not found])]),
  [test "x$enable_valgrind" = "x"], [enable_valgrind=yes])

AM_CONDITIONAL([VALGRIND_ENABLED], [test "x$enable_valgrind" = "xyes"])
if test "x$enable_valgrind" = "xyes"; then
  AC_CHECK_HEADERS([valgrind/helgrind.h])
fi

# STS?
AC_ARG_ENABLE([sts],
	[AS_HELP_STRING([--enable-sts], [enable radoswgw sts])],
	[],
	[enable_sts=no])
AM_CONDITIONAL(ENABLE_STS, test "$enable_sts" = "yes")
#AS_IF([test "$enable_sts" = "yes"], [AC_DEFINE([WITH_RADOS, ENABLE_STS])])

AC_SUBST(systemd_unit_dir)
AC_ARG_WITH(
    systemd-unit-dir,
    AS_HELP_STRING(
	    [--with-systemdsystemunitdir=DIR],
	    [systemd unit directory @<:@SYSTEMD_UNIT_DIR@:>@
        Defaults to the correct value for debian /etc/systemd/system/]
    ),
    [
	    systemd_unit_dir="$withval"
    ],
    [
        # default to the systemd admin unit directory
        which pkg-config
        pkg_config_exists=$?
        if test x"$pkg_config_exists" = x"0"; then
            systemd_unit_dir=`pkg-config systemd --variable=systemdsystemunitdir`
        else
            systemd_unit_dir="/etc/systemd/system/"
        fi
    ]
)

# Force not to use eventfd
AC_ARG_WITH([eventfd],
            [AS_HELP_STRING([--without-eventfd], [disable eventfd [default=no]])],
            ,
            [with_eventfd=yes])
AS_IF([test "x$with_eventfd" != xno],
    [AC_CHECK_HEADERS(sys/eventfd.h,
                     [AC_DEFINE(HAVE_EVENTFD, 1, [Have eventfd extension.])])])
AM_CONDITIONAL(WITH_EVENTFD, [ test "$with_eventfd" = "yes" ])

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
	systemd/Makefile
	man/Makefile
	doc/Makefile
	selinux/Makefile
	ceph.spec])
AC_OUTPUT
