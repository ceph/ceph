#ifndef CEPH_ASSERT_H
#define CEPH_ASSERT_H

#include <features.h>

#ifdef __CEPH__
# include "acconfig.h"
#endif

/*
 * atomic_ops.h includes the system assert.h, which will redefine
 * 'assert' even if it's already been included.  so, make sure we
 * include atomic_ops.h first so that we don't get an #include
 * <assert.h> again later.
 */
#ifndef NO_ATOMIC_OPS
#include "atomic_ops.h"
#endif

#ifdef __cplusplus

namespace ceph {

class BackTrace;

struct FailedAssertion {
  BackTrace *backtrace;
  FailedAssertion(BackTrace *bt) : backtrace(bt) {}
};

#endif


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
extern void __ceph_assert_warn(const char *assertion, const char *file, int line, const char *function);

#define ceph_assert(expr)							\
  ((expr)								\
   ? __CEPH_ASSERT_VOID_CAST (0)					\
   : __ceph_assert_fail (__STRING(expr), __FILE__, __LINE__, __ASSERT_FUNCTION))

#define assert_warn(expr)							\
  ((expr)								\
   ? __CEPH_ASSERT_VOID_CAST (0)					\
   : __ceph_assert_warn (__STRING(expr), __FILE__, __LINE__, __ASSERT_FUNCTION))

/*
#define assert(expr)							\
  do {									\
	static int __assert_flag = 0;					\
	struct TlsData *tls = tls_get_val();				\
	if (!__assert_flag && tls && tls->disable_assert) {		\
		__assert_flag = 1;					\
		__ceph_assert_warn(__STRING(expr), __FILE__, __LINE__, __ASSERT_FUNCTION); \
	}								\
	((expr)								\
	? __CEPH_ASSERT_VOID_CAST (0)					\
	: __ceph_assert_fail (__STRING(expr), __FILE__, __LINE__, __ASSERT_FUNCTION)); \
  } while (0)
#endif
*/
/*
#define assert_protocol(expr)	assert(expr)
#define assert_disk(expr)	assert(expr)
*/

#ifdef __cplusplus
}

using namespace ceph;

#endif

/*
 * ceph_abort aborts the program with a nice backtrace.
 *
 * Currently, it's the same as assert(0), but we may one day make assert a
 * debug-only thing, like it is in many projects.
 */
#define ceph_abort() assert(0)

#endif

// wipe any prior assert definition
#ifdef assert
# undef assert
#endif

#define assert(expr)							\
  ((expr)								\
   ? __CEPH_ASSERT_VOID_CAST (0)					\
   : __ceph_assert_fail (__STRING(expr), __FILE__, __LINE__, __ASSERT_FUNCTION))
