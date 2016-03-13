#ifndef CEPH_ASSERT_H
#define CEPH_ASSERT_H

#if defined(__linux__)
#include <features.h>

#ifndef __STRING
# define __STRING(x) #x
#endif

#elif defined(__FreeBSD__)
#include <sys/cdefs.h>
#define	__GNUC_PREREQ(minor, major)	__GNUC_PREREQ__(minor, major)
#elif defined(__sun) || defined(_AIX)
#include "include/compat.h"
#include <assert.h>
#endif

#ifdef __CEPH__
# include "acconfig.h"
#endif

#ifdef __cplusplus
class CephContext;
#else
struct CephContext;
typedef struct CephContext CephContext;
#endif

#ifdef __cplusplus
namespace ceph {

struct BackTrace;
#endif


#ifdef HAVE_STATIC_CAST
# define __CEPH_ASSERT_VOID_CAST static_cast<void>
#else
# define __CEPH_ASSERT_VOID_CAST (void)
#endif

/*
 * For GNU, test specific version features. Otherwise (e.g. LLVM) we'll use
 * the defaults selected below.
 */
#ifdef __GNUC_PREREQ

/*
 * Version 2.4 and later of GCC define a magical variable
 * `__PRETTY_FUNCTION__' which contains the name of the function currently
 * being defined.  This is broken in G++ before version 2.6.  C9x has a
 * similar variable called __func__, but prefer the GCC one since it demangles
 * C++ function names. We define __CEPH_NO_PRETTY_FUNC if we want to avoid
 * broken versions of G++.
 */
# if defined __cplusplus ? !__GNUC_PREREQ (2, 6) : !__GNUC_PREREQ (2, 4)
#   define __CEPH_NO_PRETTY_FUNC
# endif

#endif

/*
 * Select a function-name variable based on compiler tests, and any compiler
 * specific overrides.
 */
#if defined(HAVE_PRETTY_FUNC) && !defined(__CEPH_NO_PRETTY_FUNC)
# define __CEPH_ASSERT_FUNCTION __PRETTY_FUNCTION__
#elif defined(HAVE_FUNC)
# define __CEPH_ASSERT_FUNCTION __func__
#else
# define __CEPH_ASSERT_FUNCTION ((__const char *) 0)
#endif

extern void register_assert_context(CephContext *cct);
extern void __ceph_assert_fail(const char *assertion, const char *file, int line, const char *function)
  __attribute__ ((__noreturn__));
extern void __ceph_assertf_fail(const char *assertion, const char *file, int line, const char *function, const char* msg, ...)
  __attribute__ ((__noreturn__));
extern void __ceph_assert_warn(const char *assertion, const char *file, int line, const char *function);

#define ceph_assert(expr)							\
  ((expr)								\
   ? __CEPH_ASSERT_VOID_CAST (0)					\
   : __ceph_assert_fail (__STRING(expr), __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION))

#define assert_warn(expr)							\
  ((expr)								\
   ? __CEPH_ASSERT_VOID_CAST (0)					\
   : __ceph_assert_warn (__STRING(expr), __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION))

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

// make _ASSERT_H something that *must* have a value other than what
// /usr/include/assert.h gives it (nothing!), so that we detect when
// our assert is clobbered.
#undef _ASSERT_H
#define _ASSERT_H _dout_cct

// make __ASSERT_FUNCTION empty (/usr/include/assert.h makes it a function)
// and make our encoding macros break if it non-empty.
#undef __ASSERT_FUNCTION
#define __ASSERT_FUNCTION

#define assert(expr)							\
  ((expr)								\
   ? __CEPH_ASSERT_VOID_CAST (0)					\
   : __ceph_assert_fail (__STRING(expr), __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION))

// Named by analogy with printf.  Along with an expression, takes a format
// string and parameters which are printed if the assertion fails.
#define assertf(expr, ...)                  \
  ((expr)								\
   ? __CEPH_ASSERT_VOID_CAST (0)					\
   : __ceph_assertf_fail (__STRING(expr), __FILE__, __LINE__, __CEPH_ASSERT_FUNCTION, __VA_ARGS__))
