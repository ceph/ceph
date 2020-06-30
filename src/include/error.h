// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include <stdarg.h>

#ifdef    __cplusplus
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

#ifdef    __cplusplus
} // extern "C"
#endif
