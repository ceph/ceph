/*
 * This must be first to ensure we get the non-Gnu version of strerror_r.
 *
 * There are two versions of strerror_r: POSIX and Gnu.  The difference
 * is that the Gnu version returns the string for convenience and the 
 * POSIX version returns success or failure (one wonders what the failure
 * could possibly be, but there you go, it's POSIX).
 *
 * You might think we could just go ahead and use either version and
 * just get the result out of buf.  However, you'd be wrong, because the
 * Gnu version may or may not fill buf; it may just return a pointer to
 * a static string inside libc.  Which it does.
 *
 * So we really want the POSIX version for portability.
 */
#undef _GNU_SOURCE

#include "common/errno.h"

#include <sstream>
#include <string>
#include <iostream>

#include <string.h>
#include <stdio.h>

std::string cpp_strerror(int err)
{
  char buf[128];

  if (err < 0)
    err = -err;
  std::ostringstream oss;
  buf[0] = '\0';
  ::strerror_r(err, buf, sizeof(buf));
  oss << "(" << err << ") " << buf;

  return oss.str();
}
