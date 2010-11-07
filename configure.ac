#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# Autoconf
AC_PREREQ(2.59)
AC_INIT([Ceph distributed file system], [0.24~rc], 
	      [Sage Weil <sage@newdream.net>],
	      [ceph])

# Environment
AC_CANONICAL_HOST
AC_CANONICAL_TARGET

# Automake
AM_INIT_AUTOMAKE(ceph, 0.24~rc)
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
AC_CHECK_LIB([m], [pow], [true], AC_MSG_FAILURE([libm not found]))
AC_CHECK_LIB([pthread], [pthread_create], [true], AC_MSG_FAILURE([libpthread not found]))

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
RADOSGW=0
AS_IF([test "x$with_radosgw" != xno],
	    [AC_CHECK_LIB([fcgi], [FCGX_Init],
             [AC_CHECK_LIB([expat], [XML_Parse],
              [RADOSGW=1],
	      [if test "x$with_radosgw" != "xcheck"; then
		   AC_MSG_FAILURE([--with-radosgw was given but libexpat (libexpat1-dev on debian) not found])
	      fi])
             ],
	     [if test "x$with_radosgw" != "xcheck"; then
		AC_MSG_FAILURE([--with-radosgw was given but libfcgi (libfcgi-dev on debian) not found])
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

# tcmalloc?
AC_ARG_WITH([tcmalloc],
	    [AS_HELP_STRING([--with--tcmalloc], [use tcmalloc instead of ptmallc])],
	    [],
	    [with_tcmalloc=check])
TCMALLOC=
AS_IF([test "x$with_tcmalloc" != xno],
	    [AC_CHECK_LIB([tcmalloc], [malloc],
	     [AC_SUBST([LIBTCMALLOC], ["-ltcmalloc"])
	       AC_DEFINE([HAVE_LIBTCMALLOC], [1],
	       		 [Define if you have tcmalloc])
	       HAVE_LIBTCMALLOC=1
	     ],
	    [if test "x$with_tcmalloc" != xcheck; then
	        AC_MSG_FAILURE(
		  [--with-tcmalloc was given but test failed])
	      fi
	    ])])
AM_CONDITIONAL(WITH_TCMALLOC, [test "$HAVE_LIBTCMALLOC" = "1"])

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

#libatomic-ops? You want it!
AC_ARG_WITH([libatomic-ops],
            [AS_HELP_STRING([--with-libatomic-ops],
	    [use libatomic-ops to build Ceph's atomic_t type])],
            [],
            [with_libatomic_ops=check])
AS_IF([test "x$with_libatomic_ops" != xno],
	    [AC_CHECK_HEADER([atomic_ops.h],
             [HAVE_ATOMIC_OPS=1],
             [if test "x$with_libatomic_ops" != xcheck; then
                 AC_MSG_FAILURE(
                   [--with-libatomic-ops was given but atomic_ops.h not found])
               fi
             ])])
AS_IF([test "$HAVE_ATOMIC_OPS" = "1"],
	[],
	AC_DEFINE([NO_ATOMIC_OPS], [1], [Defined if you don't have atomic_ops]))
AM_CONDITIONAL(WITH_LIBATOMIC, [test "$HAVE_ATOMIC_OPS" = "1"])

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

# sync_file_range
AC_CHECK_FUNC([sync_file_range],
	[AC_DEFINE([HAVE_SYNC_FILE_RANGE], [], [sync_file_range(2) is supported])],
	[])

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

AC_ARG_WITH([gtk2],
        [AS_HELP_STRING([--with-gtk2], [Build the graphical tools.  Default=yes.])],
        [try_with_gtk2=$withval],
        [])
AS_IF([test "x$try_with_gtk2" != "xno"],
        [PKG_CHECK_MODULES(GTKMM, [gtkmm-2.4 >= 1.0.0],
                [AC_DEFINE([HAVE_GTK2], [1], [we have gtk2])],
                [try_with_gtk2=no
                 AC_MSG_RESULT([Sorry, a usable version of gtkmm was not found. We will build without it.])])])
AM_CONDITIONAL(WITH_GTK2, [test "x$try_with_gtk2" != "xno"])

AC_CONFIG_HEADERS([src/acconfig.h])
AC_CONFIG_FILES([Makefile
	src/Makefile
	man/Makefile
	ceph.spec])
AC_OUTPUT
