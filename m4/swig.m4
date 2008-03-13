dnl check to see if SWIG is current enough.
dnl
dnl if it is, then check to see if we have the correct version of python.
dnl
dnl if we do, then set up the appropriate SWIG_ variables to build the 
dnl python bindings.

AC_DEFUN([CEPH_CHECK_SWIG],
[
  AC_ARG_WITH(swig,
              AS_HELP_STRING([--with-swig=PATH],
                             [Try to use 'PATH/bin/swig' to build the
                              swig bindings.  If PATH is not specified,
                              look for a 'swig' binary in your PATH.]),
  [
    case "$withval" in
      "no")
        SWIG_SUITABLE=no
        CEPH_FIND_SWIG(no)
      ;;
      "yes")
        CEPH_FIND_SWIG(check)
      ;;
      *)
        CEPH_FIND_SWIG($withval)
      ;;
    esac
  ],
  [
    CEPH_FIND_SWIG(check)
  ])
])

AC_DEFUN([CEPH_FIND_SWIG],
[
  where=$1

  if test $where = no; then
    AC_PATH_PROG(SWIG, none, none)
  elif test $where = check; then
    AC_PATH_PROG(SWIG, swig, none)
  else
    if test -f "$where"; then
      SWIG="$where"
    else
      SWIG="$where/bin/swig"
    fi
    if test ! -f "$SWIG" || test ! -x "$SWIG"; then
      AC_MSG_ERROR([Could not find swig binary at $SWIG])
    fi 
  fi

  if test "$SWIG" != "none"; then
    AC_MSG_CHECKING([swig version])
    SWIG_VERSION_RAW="`$SWIG -version 2>&1 | \
                       sed -ne 's/^.*Version \(.*\)$/\1/p'`"
    # We want the version as an integer so we can test against
    # which version we're using.  SWIG doesn't provide this
    # to us so we have to come up with it on our own. 
    # The major is passed straight through,
    # the minor is zero padded to two places,
    # and the patch level is zero padded to three places.
    # e.g. 1.3.24 becomes 103024
    SWIG_VERSION="`echo \"$SWIG_VERSION_RAW\" | \
                  sed -e 's/[[^0-9\.]].*$//' \
                      -e 's/\.\([[0-9]]\)$/.0\1/' \
                      -e 's/\.\([[0-9]][[0-9]]\)$/.0\1/' \
                      -e 's/\.\([[0-9]]\)\./0\1/; s/\.//g;'`"
    AC_MSG_RESULT([$SWIG_VERSION_RAW])
    # If you change the required swig version number, don't forget to update:
    #   subversion/bindings/swig/INSTALL
    #   packages/rpm/redhat-8+/subversion.spec
    #   packages/rpm/redhat-7.x/subversion.spec
    #   packages/rpm/rhel-3/subversion.spec
    #   packages/rpm/rhel-4/subversion.spec
    SWIG_SUITABLE=yes
  fi
 
  if test "$PERL" != "none"; then
    AC_MSG_CHECKING([perl version])
    dnl Note that the q() bit is there to avoid unbalanced brackets
    dnl which m4 really doesn't like.
    PERL_VERSION="`$PERL -e 'q([[); print $]] * 1000000,$/;'`"
    AC_MSG_RESULT([$PERL_VERSION])
    if test "$PERL_VERSION" -ge "5008000"; then
      SWIG_PL_INCLUDES="\$(SWIG_INCLUDES) `$PERL -MExtUtils::Embed -e ccopts`"
    else
      AC_MSG_WARN([perl bindings require perl 5.8.0 or newer.])
    fi
  fi


  AC_SUBST(SWIG)
  AC_SUBST(SWIG_PL_INCLUDES)
])
