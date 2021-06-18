// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_GLOBAL_SIGNAL_HANDLER_H
#define CEPH_GLOBAL_SIGNAL_HANDLER_H

#include <signal.h>
#include "acconfig.h"
#include <map>
#include <string>

typedef void (*signal_handler_t)(int);
namespace ceph {
  struct BackTrace;
}

#if defined(HAVE_SIGDESCR_NP)
# define sig_str(signum) sigdescr_np(signum)
#elif defined(HAVE_REENTRANT_STRSIGNAL)
# define sig_str(signum) strsignal(signum)
#else
# define sig_str(signum) sys_siglist[signum]
#endif

void install_sighandler(int signum, signal_handler_t handler, int flags);

// handles SIGHUP
void sighup_handler(int signum);

// Install the standard Ceph signal handlers
void install_standard_sighandlers(void);


/// initialize async signal handler framework
void init_async_signal_handler();

/// shutdown async signal handler framework
void shutdown_async_signal_handler();

/// queue an async signal
void queue_async_signal(int signum);

/// install a safe, async, callback for the given signal
void register_async_signal_handler(int signum, signal_handler_t handler);
void register_async_signal_handler_oneshot(int signum, signal_handler_t handler);

/// uninstall a safe async signal callback
void unregister_async_signal_handler(int signum, signal_handler_t handler);

void generate_crash_dump(char *base,
			 const ceph::BackTrace& bt,
			 std::map<std::string,std::string> *extra = 0);

#endif
