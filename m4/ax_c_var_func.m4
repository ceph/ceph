# ===========================================================================
#       http://www.gnu.org/software/autoconf-archive/ax_c_var_func.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_C_VAR_FUNC
#
# DESCRIPTION
#
#   This macro tests if the C complier supports the C9X standard __func__
#   indentifier.
#
#   The new C9X standard for the C language stipulates that the identifier
#   __func__ shall be implictly declared by the compiler as if, immediately
#   following the opening brace of each function definition, the declaration
#
#     static const char __func__[] = "function-name";
#
#   appeared, where function-name is the name of the function where the
#   __func__ identifier is used.
#
# LICENSE
#
#   Copyright (c) 2008 Christopher Currie <christopher@currie.com>
#
#   This program is free software; you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the
#   Free Software Foundation; either version 2 of the License, or (at your
#   option) any later version.
#
#   This program is distributed in the hope that it will be useful, but
#   WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
#   Public License for more details.
#
#   You should have received a copy of the GNU General Public License along
#   with this program. If not, see <http://www.gnu.org/licenses/>.
#
#   As a special exception, the respective Autoconf Macro's copyright owner
#   gives unlimited permission to copy, distribute and modify the configure
#   scripts that are the output of Autoconf when processing the Macro. You
#   need not follow the terms of the GNU General Public License when using
#   or distributing such scripts, even though portions of the text of the
#   Macro appear in them. The GNU General Public License (GPL) does govern
#   all other use of the material that constitutes the Autoconf Macro.
#
#   This special exception to the GPL applies to versions of the Autoconf
#   Macro released by the Autoconf Archive. When you make and distribute a
#   modified version of the Autoconf Macro, you may extend this special
#   exception to the GPL to apply to your modified version as well.

#serial 5

AU_ALIAS([AC_C_VAR_FUNC], [AX_C_VAR_FUNC])
AC_DEFUN([AX_C_VAR_FUNC],
[AC_REQUIRE([AC_PROG_CC])
AC_CACHE_CHECK(whether $CC recognizes __func__, ac_cv_c_var_func,
AC_TRY_COMPILE(,
[
char *s = __func__;
],
AC_DEFINE(HAVE_FUNC,,
[Define if the C complier supports __func__]) ac_cv_c_var_func=yes,
ac_cv_c_var_func=no) )
])dnl
