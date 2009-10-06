#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# Autoconf
AC_PREREQ(2.59)
AC_INIT([Ceph distributed file system], [.1], 
	      [Sage Weil <sage@newdream.net>],
	      [ceph])

# Environment
AC_CANONICAL_HOST
AC_CANONICAL_TARGET

# Automake
AM_INIT_AUTOMAKE(ceph, 0.16.1)
AM_PROG_CC_C_O
AM_PROG_LIBTOOL

# Platform
case "${target_os}" in
darwin*)
	AC_DEFINE([DARWIN], [1], [Define if darwin/osx])
esac

# Checks for programs.
AC_PROG_CXX
#AC_PROG_CC
#AC_PROG_RANLIB
AC_PROG_MAKE_SET
AC_PROG_LIBTOOL

# Checks for libraries.
AC_CHECK_LIB([m], [pow])
AC_CHECK_LIB([pthread], [pthread_create])

# debug crap?
AC_ARG_WITH([debug],
            [AS_HELP_STRING([--with-debug], [build extra debug binaries])],
            [with_debug=yes],
            [with_debug=no])
AM_CONDITIONAL(WITH_DEBUG, test "$with_debug" = "yes")

# radosgw?
AC_ARG_WITH([radosgw],
            [AS_HELP_STRING([--with-radosgw], [build RADOS gateway])],
            [],
            [with_radosgw=check])
RADOSGW=1
AS_IF([test "x$with_radosgw" != xno],
	    [AC_CHECK_LIB([fcgi], [FCGX_Init],
             [true],
	     [RADOSGW=0
	     if test "x$with_radosgw" != "xcheck"; then
		AC_MSG_FAILURE([--with-radosgw was given but libfcgi (libfcgi-dev on debian) not found])
	     fi])
	     AC_CHECK_LIB([expat], [XML_Parse],
             [true],
	     [RADOSGW=0
	     if test "x$with_radosgw" != "xcheck"; then
		AC_MSG_FAILURE([--with-radosgw was given but libexpat (libexpat1-dev on debian) not found])
	     fi])])
AM_CONDITIONAL(WITH_RADOSGW, test "$RADOSGW" = "1")

# fuse?
AC_ARG_WITH([fuse],
            [AS_HELP_STRING([--with-fuse], [use FUSE library for client])],
            [],
            [with_fuse=check])
LIBFUSE=
AS_IF([test "x$with_fuse" != xno],
	    [AC_CHECK_LIB([fuse], [fuse_main],
             [AC_SUBST([LIBFUSE], ["-lfuse"])
               AC_DEFINE([HAVE_LIBFUSE], [1],
                         [Define if you have fuse])
               HAVE_LIBFUSE=1
              ],
             [if test "x$with_fuse" != xcheck; then
                 AC_MSG_FAILURE(
                   [--with-fuse was given but test failed])
               fi
             ])])
AM_CONDITIONAL(WITH_FUSE, [test "$HAVE_LIBFUSE" = "1"])

# jni?
AC_ARG_WITH([hadoop],
            [AS_HELP_STRING([--with-hadoop], [build hadoop client])],
            [],
            [with_hadoop=check])
AS_IF([test "x$with_hadoop" != xno],
	    [AC_CHECK_HEADER([jni.h],
             [HAVE_JNI=1],
             [if test "x$with_hadoop" != xcheck; then
                 AC_MSG_FAILURE(
                   [--with-hadoop was given but jni.h not found])
               fi
             ])])
AM_CONDITIONAL(WITH_HADOOPCLIENT, [test "$HAVE_JNI" = "1"])

# CommonC++?
AC_ARG_WITH([ccgnu],
            [AS_HELP_STRING([--with-ccgnu],
              [use CommonC++ library for fast thread-safe reference counting])],
            [],
            [with_ccgnu=check])
LIBCCGNU2=
AS_IF([test "x$with_ccgnu" == xyes],
	    [AC_CHECK_LIB([ccgnu2], [main],
             [AC_SUBST([LIBCCGNU2], ["-lccgnu2"])
              AC_DEFINE([WITH_CCGNU], [1],
                        [Define if you have ccgnu])
	      LIBS="-lccgnu2 -ldl $LIBS"
             ],
             [if test "x$with_ccgnu" != xcheck; then
                 AC_MSG_FAILURE(
                   [--with-ccgnu was given but test failed])
               fi
             ], [-ldl -lpthread])])
AM_CONDITIONAL(WITH_CCGNU, test "WITH_CCGNU" = "1")

# newsyn?  requires mpi.
#AC_ARG_WITH([newsyn],
#            [AS_HELP_STRING([--with-newsyn], [build newsyn target requires mpi])],
#            [],
#            [with_newsyn=no])

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_HEADER_SYS_WAIT

# spirit?
AC_LANG([C++])
AC_DEFINE([HAVE_BOOST_SPIRIT_CORE_HPP], [], [Description])
AC_CHECK_HEADER([boost/spirit.hpp],
                [AC_DEFINE([HAVE_BOOST_SPIRIT_CORE_HPP])],
                [AC_MSG_ERROR([Sorry you need to install the Boost spirit parser library (libboost-dev on debian)])])

AC_LANG([C])

AC_CHECK_HEADER([histedit.h],
	[],
	[AC_MSG_ERROR([Sorry you need histedit.h (libedit-dev on debian)])])

AC_CHECK_HEADER([openssl/md5.h],
	[],
	[AC_MSG_ERROR([Sorry you need openssl dev files (libssl-dev on debian)])])

AC_CHECK_HEADERS([sys/xattr.h arpa/inet.h netdb.h netinet/in.h sys/file.h sys/ioctl.h sys/mount.h sys/param.h sys/socket.h sys/statvfs.h sys/time.h sys/vfs.h syslog.h utime.h])

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

AC_CONFIG_HEADERS([src/acconfig.h])
AC_CONFIG_FILES([Makefile
	src/Makefile
	man/Makefile
	ceph.spec])
AC_OUTPUT

AM_INIT_AUTOMAKE
