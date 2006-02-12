// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

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
