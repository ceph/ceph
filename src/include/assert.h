#ifndef __CEPH_ASSERT_H
#define __CEPH_ASSERT_H

#include <features.h>

#if defined __cplusplus && __GNUC_PREREQ (2,95)
# define __CEPH_ASSERT_VOID_CAST static_cast<void>
#else
# define __CEPH_ASSERT_VOID_CAST (void)
#endif

/* Version 2.4 and later of GCC define a magical variable `__PRETTY_FUNCTION__'
   which contains the name of the function currently being defined.
   This is broken in G++ before version 2.6.
   C9x has a similar variable called __func__, but prefer the GCC one since
   it demangles C++ function names.  */
# if defined __cplusplus ? __GNUC_PREREQ (2, 6) : __GNUC_PREREQ (2, 4)
#   define __ASSERT_FUNCTION	__PRETTY_FUNCTION__
# else
#  if defined __STDC_VERSION__ && __STDC_VERSION__ >= 199901L
#   define __ASSERT_FUNCTION	__func__
#  else
#   define __ASSERT_FUNCTION	((__const char *) 0)
#  endif
# endif

extern void __ceph_assert_fail(const char *assertion, const char *file, int line, const char *function)
  __attribute__ ((__noreturn__));

#define assert(expr)							\
  ((expr)								\
   ? __CEPH_ASSERT_VOID_CAST (0)					\
   : __ceph_assert_fail (__STRING(expr), __FILE__, __LINE__, __ASSERT_FUNCTION))

#endif
