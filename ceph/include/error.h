#include <stdarg.h>

#ifdef	__cplusplus
extern "C" {
#endif

#define SYSERROR() syserror("At %s:%d", __FILE__, __LINE__)

#define ASSERT(c) \
  ((c) || (exiterror("Assertion failed at %s:%d", __FILE__, __LINE__), 1))

/* print usage error message and exit */
extern void userror(const char *use, const char *fmt, ...);

/* print system error message and exit */
extern void syserror(const char *fmt, ...);

/* print error message and exit */
extern void exiterror(const char *fmt, ...);

/* print error message */
extern void error(const char *fmt, ...);

#ifdef	__cplusplus
} // extern "C"
#endif
