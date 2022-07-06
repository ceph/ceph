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

#include "include/compat.h"
#include "common/debug.h"

using std::ostringstream;

namespace ceph {
  static CephContext *g_assert_context = NULL;

  /* If you register an assert context, ceph_assert() will try to lock the dout
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
    ceph_assert(!g_assert_context);
    g_assert_context = cct;
  }

  [[gnu::cold]] void __ceph_assert_fail(const char *assertion,
					const char *file, int line,
					const char *func)
  {
    g_assert_condition = assertion;
    g_assert_file = file;
    g_assert_line = line;
    g_assert_func = func;
    g_assert_thread = (unsigned long long)pthread_self();
    ceph_pthread_getname(pthread_self(), g_assert_thread_name,
		       sizeof(g_assert_thread_name));

    ostringstream tss;
    tss << ceph_clock_now();

    snprintf(g_assert_msg, sizeof(g_assert_msg),
	     "%s: In function '%s' thread %llx time %s\n"
	     "%s: %d: FAILED ceph_assert(%s)\n",
	     file, func, (unsigned long long)pthread_self(), tss.str().c_str(),
	     file, line, assertion);
    dout_emergency(g_assert_msg);

    // TODO: get rid of this memory allocation.
    ostringstream oss;
    oss << ClibBackTrace(1);
    dout_emergency(oss.str());

    if (g_assert_context) {
      lderr(g_assert_context) << g_assert_msg << std::endl;
      *_dout << oss.str() << dendl;

      // dump recent only if the abort signal handler won't do it for us
      if (!g_assert_context->_conf->fatal_signal_handlers) {
	g_assert_context->_log->dump_recent();
      }
    }

    abort();
  }

  [[gnu::cold]] void __ceph_assert_fail(const assert_data &ctx)
  {
    __ceph_assert_fail(ctx.assertion, ctx.file, ctx.line, ctx.function);
  }

  class BufAppender {
  public:
    BufAppender(char* buf, int size) : bufptr(buf), remaining(size) {}

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


  [[gnu::cold]] void __ceph_assertf_fail(const char *assertion,
					 const char *file, int line,
					 const char *func, const char* msg,
					 ...)
  {
    ostringstream tss;
    tss << ceph_clock_now();

    g_assert_condition = assertion;
    g_assert_file = file;
    g_assert_line = line;
    g_assert_func = func;
    g_assert_thread = (unsigned long long)pthread_self();
    ceph_pthread_getname(pthread_self(), g_assert_thread_name,
		       sizeof(g_assert_thread_name));

    BufAppender ba(g_assert_msg, sizeof(g_assert_msg));
    BackTrace *bt = new ClibBackTrace(1);
    ba.printf("%s: In function '%s' thread %llx time %s\n"
	     "%s: %d: FAILED ceph_assert(%s)\n",
	     file, func, (unsigned long long)pthread_self(), tss.str().c_str(),
	     file, line, assertion);
    ba.printf("Assertion details: ");
    va_list args;
    va_start(args, msg);
    ba.vprintf(msg, args);
    va_end(args);
    ba.printf("\n");
    dout_emergency(g_assert_msg);

    // TODO: get rid of this memory allocation.
    ostringstream oss;
    oss << *bt;
    dout_emergency(oss.str());

    if (g_assert_context) {
      lderr(g_assert_context) << g_assert_msg << std::endl;
      *_dout << oss.str() << dendl;

      // dump recent only if the abort signal handler won't do it for us
      if (!g_assert_context->_conf->fatal_signal_handlers) {
	g_assert_context->_log->dump_recent();
      }
    }

    abort();
  }

  [[gnu::cold]] void __ceph_abort(const char *file, int line,
				  const char *func, const std::string& msg)
  {
    ostringstream tss;
    tss << ceph_clock_now();

    g_assert_condition = "abort";
    g_assert_file = file;
    g_assert_line = line;
    g_assert_func = func;
    g_assert_thread = (unsigned long long)pthread_self();
    ceph_pthread_getname(pthread_self(), g_assert_thread_name,
		       sizeof(g_assert_thread_name));

    BackTrace *bt = new ClibBackTrace(1);
    snprintf(g_assert_msg, sizeof(g_assert_msg),
             "%s: In function '%s' thread %llx time %s\n"
	     "%s: %d: ceph_abort_msg(\"%s\")\n", file, func,
	     (unsigned long long)pthread_self(),
	     tss.str().c_str(), file, line,
	     msg.c_str());
    dout_emergency(g_assert_msg);

    // TODO: get rid of this memory allocation.
    ostringstream oss;
    oss << *bt;
    dout_emergency(oss.str());

    if (g_assert_context) {
      lderr(g_assert_context) << g_assert_msg << std::endl;
      *_dout << oss.str() << dendl;

      // dump recent only if the abort signal handler won't do it for us
      if (!g_assert_context->_conf->fatal_signal_handlers) {
	g_assert_context->_log->dump_recent();
      }
    }

    abort();
  }

  [[gnu::cold]] void __ceph_abortf(const char *file, int line,
				   const char *func, const char* msg,
				   ...)
  {
    ostringstream tss;
    tss << ceph_clock_now();

    g_assert_condition = "abort";
    g_assert_file = file;
    g_assert_line = line;
    g_assert_func = func;
    g_assert_thread = (unsigned long long)pthread_self();
    ceph_pthread_getname(pthread_self(), g_assert_thread_name,
		       sizeof(g_assert_thread_name));

    BufAppender ba(g_assert_msg, sizeof(g_assert_msg));
    BackTrace *bt = new ClibBackTrace(1);
    ba.printf("%s: In function '%s' thread %llx time %s\n"
	      "%s: %d: abort()\n",
	      file, func, (unsigned long long)pthread_self(), tss.str().c_str(),
	      file, line);
    ba.printf("Abort details: ");
    va_list args;
    va_start(args, msg);
    ba.vprintf(msg, args);
    va_end(args);
    ba.printf("\n");
    dout_emergency(g_assert_msg);

    // TODO: get rid of this memory allocation.
    ostringstream oss;
    oss << *bt;
    dout_emergency(oss.str());

    if (g_assert_context) {
      lderr(g_assert_context) << g_assert_msg << std::endl;
      *_dout << oss.str() << dendl;

      // dump recent only if the abort signal handler won't do it for us
      if (!g_assert_context->_conf->fatal_signal_handlers) {
	g_assert_context->_log->dump_recent();
      }
    }

    abort();
  }

  [[gnu::cold]] void __ceph_assert_warn(const char *assertion,
					const char *file,
					int line, const char *func)
  {
    char buf[8096];
    snprintf(buf, sizeof(buf),
	     "WARNING: ceph_assert(%s) at: %s: %d: %s()\n",
	     assertion, file, line, func);
    dout_emergency(buf);
  }
}
