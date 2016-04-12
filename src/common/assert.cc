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

#include "BackTrace.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/Clock.h"
#include "include/assert.h"

#include <errno.h>
#include <iostream>
#include <pthread.h>
#include <sstream>
#include <time.h>

namespace ceph {
  static CephContext *g_assert_context = NULL;

  /* If you register an assert context, assert() will try to lock the dout
   * stream of that context before starting an assert. This is nice because the
   * output looks better. Your assert will not be interleaved with other dout
   * statements.
   *
   * However, this is strictly optional and library code currently does not
   * register an assert context. The extra complexity of supporting this
   * wouldn't really be worth it.
   */
  void register_assert_context(CephContext *cct)
  {
    assert(!g_assert_context);
    g_assert_context = cct;
  }

  void __ceph_assert_fail(const char *assertion, const char *file, int line,
			  const char *func)
  {
    ostringstream tss;
    tss << ceph_clock_now(g_assert_context);

    char buf[8096];
    BackTrace *bt = new BackTrace(1);
    snprintf(buf, sizeof(buf),
	     "%s: In function '%s' thread %llx time %s\n"
	     "%s: %d: FAILED assert(%s)\n",
	     file, func, (unsigned long long)pthread_self(), tss.str().c_str(),
	     file, line, assertion);
    dout_emergency(buf);

    // TODO: get rid of this memory allocation.
    ostringstream oss;
    bt->print(oss);
    dout_emergency(oss.str());

    dout_emergency(" NOTE: a copy of the executable, or `objdump -rdS <executable>` "
		   "is needed to interpret this.\n");

    if (g_assert_context) {
      lderr(g_assert_context) << buf << std::endl;
      bt->print(*_dout);
      *_dout << " NOTE: a copy of the executable, or `objdump -rdS <executable>` "
	     << "is needed to interpret this.\n" << dendl;

      g_assert_context->_log->dump_recent();
    }

    abort();
  }

  void __ceph_assertf_fail(const char *assertion, const char *file, int line,
			   const char *func, const char* msg, ...)
  {
    ostringstream tss;
    tss << ceph_clock_now(g_assert_context);

    class BufAppender {
    public:
      BufAppender(char* buf, int size) : bufptr(buf), remaining(size) {
      }

      void printf(const char * format, ...) {
	va_list args;
	va_start(args, format);
	this->vprintf(format, args);
	va_end(args);
      }

      void vprintf(const char * format, va_list args) {
	int n = vsnprintf(bufptr, remaining, format, args);
	if (n >= 0) {
	  if (n < remaining) {
	    remaining -= n;
	    bufptr += n;
	  } else {
	    remaining = 0;
	  }
	}
      }

    private:
      char* bufptr;
      int remaining;
    };

    char buf[8096];
    BufAppender ba(buf, sizeof(buf));
    BackTrace *bt = new BackTrace(1);
    ba.printf("%s: In function '%s' thread %llx time %s\n"
	     "%s: %d: FAILED assert(%s)\n",
	     file, func, (unsigned long long)pthread_self(), tss.str().c_str(),
	     file, line, assertion);
    ba.printf("Assertion details: ");
    va_list args;
    va_start(args, msg);
    ba.vprintf(msg, args);
    va_end(args);
    ba.printf("\n");
    dout_emergency(buf);

    // TODO: get rid of this memory allocation.
    ostringstream oss;
    bt->print(oss);
    dout_emergency(oss.str());

    dout_emergency(" NOTE: a copy of the executable, or `objdump -rdS <executable>` "
		   "is needed to interpret this.\n");

    if (g_assert_context) {
      lderr(g_assert_context) << buf << std::endl;
      bt->print(*_dout);
      *_dout << " NOTE: a copy of the executable, or `objdump -rdS <executable>` "
	     << "is needed to interpret this.\n" << dendl;

      g_assert_context->_log->dump_recent();
    }

    abort();
  }

  void __ceph_assert_warn(const char *assertion, const char *file,
			  int line, const char *func)
  {
    char buf[8096];
    snprintf(buf, sizeof(buf),
	     "WARNING: assert(%s) at: %s: %d: %s()\n",
	     assertion, file, line, func);
    dout_emergency(buf);
  }
}
