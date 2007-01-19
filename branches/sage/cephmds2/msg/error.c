#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "include/error.h"

#define EXIT_USAGE_ERROR -1	/* error codes for program exit */
#define EXIT_SYSTEM_ERROR -2
#define EXIT_GENERIC_ERROR -3
#define MSGSIZ 1024		/* maximum error message length */

/* print usage error message and exit */
void userror(const char *use, const char *fmt, ...)
{
  char msg[MSGSIZ];
  int len;

  va_list ap;
  va_start(ap, fmt);

  len = vsnprintf(msg+len, MSGSIZ-len, fmt, ap);
  len += snprintf(msg+len, MSGSIZ-len, "\n");
  len += snprintf(msg+len, MSGSIZ-len, use);
  fprintf(stderr, "%s\n", msg);
  exit(EXIT_USAGE_ERROR);

  va_end(ap);
}

/* print system error message and exit */
void syserror(const char *fmt, ...)
{
  char msg[MSGSIZ];
  int len;

  va_list ap;
  va_start(ap, fmt);

  len = vsnprintf(msg+len, MSGSIZ-len, fmt, ap);
  len += snprintf(msg+len, MSGSIZ-len, ": %s\n", strerror(errno));
  fprintf(stderr, "%s", msg);
  exit(EXIT_SYSTEM_ERROR);

  va_end(ap);
}

/* print error message and exit */
void exiterror(const char *fmt, ...)
{
  char msg[MSGSIZ];
  int len;

  va_list ap;
  va_start(ap, fmt);

  len = vsnprintf(msg+len, MSGSIZ-len, fmt, ap);
  fprintf(stderr, "%s\n", msg);
  exit(EXIT_GENERIC_ERROR);

  va_end(ap);
}

/* print error message */
void error(const char *fmt, ...)
{
  char msg[MSGSIZ];
  int len;

  va_list ap;
  va_start(ap, fmt);

  len = vsnprintf(msg+len, MSGSIZ-len, fmt, ap);
  fprintf(stderr, "%s\n", msg);

  va_end(ap);
}
