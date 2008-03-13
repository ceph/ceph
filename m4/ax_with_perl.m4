##### http://autoconf-archive.cryp.to/ax_with_perl.html
#
# SYNOPSIS
#
#   AX_WITH_PERL([VALUE-IF-NOT-FOUND],[PATH])
#
# DESCRIPTION
#
#   Locates an installed Perl binary, placing the result in the
#   precious variable $PERL. Accepts a present $PERL, then --with-perl,
#   and failing that searches for perl in the given path (which
#   defaults to the system path). If perl is found, $PERL is set to the
#   full path of the binary; if it is not found, $PERL is set to
#   VALUE-IF-NOT-FOUND, which defaults to 'perl'.
#
# LAST MODIFICATION
#
#   2008-01-29
#
# COPYLEFT
#
#   Copyright (c) 2008 Francesco Salvestrini <salvestrini@users.sourceforge.net>
#
#   Copying and distribution of this file, with or without
#   modification, are permitted in any medium without royalty provided
#   the copyright notice and this notice are preserved.

AC_DEFUN([AX_WITH_PERL],[
    AX_WITH_PROG(PERL,perl)
])
