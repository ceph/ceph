#
# Test for C compiler support of __PRETTY_FUNCTION__
#
#  - Adapted from ax_c_var_func (Noah Watkins)
#

AU_ALIAS([AC_C_PRETTY_FUNC], [AX_C_PRETTY_FUNC])
AC_DEFUN([AX_C_PRETTY_FUNC],
[AC_REQUIRE([AC_PROG_CC])
AC_CACHE_CHECK(whether $CC recognizes __PRETTY_FUNCTION__, ac_cv_c_pretty_func,
AC_TRY_COMPILE(,
[
char *s = __PRETTY_FUNCTION__;
],
AC_DEFINE(HAVE_PRETTY_FUNC,,
[Define if the C complier supports __PRETTY_FUNCTION__]) ac_cv_c_pretty_func=yes,
ac_cv_c_pretty_func=no) )
])dnl
