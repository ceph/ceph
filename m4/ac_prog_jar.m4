dnl @synopsis AC_PROG_JAR
dnl
dnl AC_PROG_JAR tests for an existing jar program. It uses the
dnl environment variable JAR then tests in sequence various common jar
dnl programs.
dnl
dnl If you want to force a specific compiler:
dnl
dnl - at the configure.in level, set JAR=yourcompiler before calling
dnl AC_PROG_JAR
dnl
dnl - at the configure level, setenv JAR
dnl
dnl You can use the JAR variable in your Makefile.in, with @JAR@.
dnl
dnl Note: This macro depends on the autoconf M4 macros for Java
dnl programs. It is VERY IMPORTANT that you download that whole set,
dnl some macros depend on other. Unfortunately, the autoconf archive
dnl does not support the concept of set of macros, so I had to break it
dnl for submission.
dnl
dnl The general documentation of those macros, as well as the sample
dnl configure.in, is included in the AC_PROG_JAVA macro.
dnl
dnl @category Java
dnl @author Egon Willighagen <e.willighagen@science.ru.nl>
dnl @version 2000-07-19
dnl @license AllPermissive

AC_DEFUN([AC_PROG_JAR],[
AC_REQUIRE([AC_EXEEXT])dnl
if test "x$JAVAPREFIX" = x; then
        test "x$JAR" = x && AC_CHECK_PROGS(JAR, jar$EXEEXT)
else
        test "x$JAR" = x && AC_CHECK_PROGS(JAR, jar, $JAVAPREFIX)
fi
test "x$JAR" = x && AC_MSG_ERROR([no acceptable jar program found in \$PATH])
AC_PROVIDE([$0])dnl
])
