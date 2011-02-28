// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2008-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <iostream>
#include <pthread.h>
#include <sstream>
#include <time.h>

#include "BackTrace.h"
#include "common/debug.h"
#include "common/config.h"
#include "include/assert.h"

static bool dout_trylock(void)
{
  time_t cur;
  time_t end_time = time(NULL);
  end_time += 10;

  do {
    int ret = pthread_mutex_trylock(&_dout_lock);
    if (ret == 0) {
      // We got the mutex.
      return true;
    }
    else if ((ret == EDEADLK) || (ret == EAGAIN)) {
      // This thread already owns the mutex.
      // The assertion happened inside a call to dout()?
      return false;
    }
    else if (ret == EINVAL) {
      // We can never get the mutex.
      return false;
    }
    // Otherwise, keep trying.
    usleep(1000);
    cur = time(NULL);
  } while (cur < end_time);

  // If 10 seconds go by and we can't get the mutex, just bail.
  return false;
}

namespace ceph {
  void __ceph_assert_fail(const char *assertion, const char *file, int line, const char *func)
  {
    int got_lock = dout_trylock();

    char buf[8096];
    BackTrace *bt = new BackTrace(1);
    snprintf(buf, sizeof(buf),
	     "%s: In function '%s', in thread '%p'\n"
	     "%s: %d: FAILED assert(%s)\n",
	     file, func, (void*)pthread_self(), file, line, assertion);
    dout_emergency(buf);

    // TODO: get rid of this memory allocation.
    ostringstream oss;
    bt->print(oss);
    dout_emergency(oss.str());

    snprintf(buf, sizeof(buf),
	     " NOTE: a copy of the executable, or `objdump -rdS <executable>` "
	     "is needed to interpret this.\n");
    dout_emergency(oss.str());

    if (got_lock) {
      int ret = pthread_mutex_unlock(&_dout_lock);
      if (ret) {
	; // ignored
      }
    }

    throw FailedAssertion(bt);
  }

  void __ceph_assert_warn(const char *assertion, const char *file,
			  int line, const char *func)
  {
    int got_lock = dout_trylock();

    char buf[8096];
    snprintf(buf, sizeof(buf),
	     "WARNING: assert(%s) at: %s: %d: %s()\n",
	     assertion, file, line, func);
    dout_emergency(buf);

    if (got_lock) {
      int ret = pthread_mutex_unlock(&_dout_lock);
      if (ret) {
	; // ignored
      }
    }
  }
}
