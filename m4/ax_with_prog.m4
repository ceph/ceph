##### http://autoconf-archive.cryp.to/ax_with_prog.html
#
# SYNOPSIS
#
#   AX_PROG_WITH([VARIABLE],[program],[PATH],[VALUE-IF-NOT-FOUND])
#
# DESCRIPTION
#
#   Locates an installed program binary, placing the result in the
#   precious variable VARIABLE. Accepts a present VARIABLE, then
#   --with-program, and failing that searches for program in the given
#   path (which defaults to the system path). If program is found,
#   VARIABLE is set to the full path of the binary; if it is not found,
#   VARIABLE is set to VALUE-IF-NOT-FOUND, which defaults to 'program'.
#
#   A typical example could be the following one:
#
#         AX_WITH_PROG(PERL,perl)
#
#   NOTE: This macro is based upon the original AX_WITH_PYTHON macro
#   from Dustin J. Mitchell <dustin@cs.uchicago.edu>.
#
# LAST MODIFICATION
#
#   2008-01-29
#
# COPYLEFT
#
#   Copyright (c) 2008 Francesco Salvestrini <salvestrini@users.sourceforge.net>
#   Copyright (c) 2008 Dustin J. Mitchell <dustin@cs.uchicago.edu>
#
#   Copying and distribution of this file, with or without
#   modification, are permitted in any medium without royalty provided
#   the copyright notice and this notice are preserved.

AC_DEFUN([AX_WITH_PROG],[
    AC_PREREQ([2.61])

    pushdef([VARIABLE],$1)
    pushdef([EXECUTABLE],$2)
    pushdef([PATH_PROG],$3)
    pushdef([VALUE_IF_NOT_FOUND],$4)

    AC_ARG_VAR(VARIABLE,Absolute path to EXECUTABLE executable)

    AS_IF(test -z "$VARIABLE",[
    	AC_MSG_CHECKING(whether EXECUTABLE executable path has been provided)
        AC_ARG_WITH(EXECUTABLE,AS_HELP_STRING([--with-EXECUTABLE=[[[[PATH]]]]],absolute path to EXECUTABLE executable), [
	    AS_IF([test "$withval" != "yes"],[
	        VARIABLE="$withval"
		AC_MSG_RESULT($VARIABLE)
	    ],[
		VARIABLE=""
	        AC_MSG_RESULT([no])
	    ])
	],[
	    AC_MSG_RESULT([no])
	])

        AS_IF(test -z "$VARIABLE",[
	    AC_PATH_PROG([]VARIABLE[],[]EXECUTABLE[],[]VALUE_IF_NOT_FOUND[],[]PATH_PROG[])
        ])
    ])

    popdef([VALUE_IF_NOT_FOUND])
    popdef([PATH_PROG])
    popdef([EXECUTABLE])
    popdef([VARIABLE])
])
