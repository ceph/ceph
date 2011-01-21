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

#include "common/BackTrace.h"
#include "common/Logger.h"
#include "common/debug.h"
#include "config.h"

#include <signal.h>
#include <sstream>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>

#define dout_prefix *_dout
#define derr dout(0)

typedef void (*signal_handler_t)(int);

static void install_sighandler(int signum, signal_handler_t handler, int flags)
{
  int ret;
  struct sigaction oldact;
  struct sigaction act;
  memset(&act, 0, sizeof(act));

  act.sa_handler = handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = flags;

  ret = sigaction(signum, &act, &oldact);
  if (ret != 0) {
    *_dout << "install_sighandler: sigaction returned " << ret << " when "
         << "trying to install a signal handler for "
	 << sys_siglist[signum] << "\n";
    exit(1);
  }
}

static void sighup_handler(int signum)
{
  // All this does is set a few bits telling us to re-open our logfiles and
  // restart our central logging service.
  _dout_need_open = true;
  logger_reopen_all();
}

static void handle_fatal_signal(int signum)
{
  // This code may itself trigger a SIGSEGV if the heap is corrupt. In that
  // case, SA_RESETHAND specifies that the default signal handler--
  // presumably dump core-- will handle it.
  *_dout << "*** Caught signal (" << sys_siglist[signum] << ") ***"
	 << std::endl;
  *_dout << "in thread " << std::hex << pthread_self() << std::endl;
  BackTrace bt(0);
  bt.print(*_dout);
  _dout->flush();

  // Use default handler to dump core
  int ret = raise(signum);

  // Normally, we won't get here. If we do, something is very weird.
  if (ret) {
    *_dout << "handle_fatal_signal: failed to re-raise signal " << signum
	   << std::endl;
  }
  else {
    *_dout << "handle_fatal_signal: default handler for signal " << signum
	   << " didn't terminate the process?" << std::endl;
  }
  exit(1);
}

#include <syslog.h>

std::string signal_mask_to_str()
{
  sigset_t old_sigset;
  if (pthread_sigmask(SIG_SETMASK, NULL, &old_sigset)) {
    return "(pthread_signmask failed)";
  }

  ostringstream oss;
  oss << "show_signal_mask: { ";
  string sep("");
  for (int signum = 0; signum < NSIG; ++signum) {
    if (sigismember(&old_sigset, signum) == 1) {
      oss << sep << signum;
      sep = ", ";
    }
  }
  oss << " }";
  return oss.str();
}

void install_standard_sighandlers(void)
{
  install_sighandler(SIGHUP, sighup_handler, SA_RESTART);
  install_sighandler(SIGSEGV, handle_fatal_signal, SA_RESETHAND | SA_NODEFER);
  install_sighandler(SIGABRT, handle_fatal_signal, SA_RESETHAND | SA_NODEFER);
  install_sighandler(SIGBUS, handle_fatal_signal, SA_RESETHAND | SA_NODEFER);
  install_sighandler(SIGILL, handle_fatal_signal, SA_RESETHAND | SA_NODEFER);
  install_sighandler(SIGFPE, handle_fatal_signal, SA_RESETHAND | SA_NODEFER);
  install_sighandler(SIGXCPU, handle_fatal_signal, SA_RESETHAND | SA_NODEFER);
  install_sighandler(SIGXFSZ, handle_fatal_signal, SA_RESETHAND | SA_NODEFER);
  install_sighandler(SIGSYS, handle_fatal_signal, SA_RESETHAND | SA_NODEFER);
}

void block_all_signals(sigset_t *old_sigset)
{
  sigset_t sigset;
  sigfillset(&sigset);
  sigdelset(&sigset, SIGKILL);
  if (pthread_sigmask(SIG_BLOCK, &sigset, old_sigset)) {
    derr << "block_all_signals: sigprocmask failed" << dendl;
    if (old_sigset)
      sigaddset(old_sigset, SIGKILL);
    return;
  }
  if (old_sigset)
    sigdelset(old_sigset, SIGKILL);
}

void restore_sigset(const sigset_t *old_sigset)
{
  if (sigismember(old_sigset, SIGKILL) != 0) {
    derr << "restore_sigset: not restoring invalid old_sigset" << dendl;
    return;
  }
  if (pthread_sigmask(SIG_SETMASK, old_sigset, NULL)) {
    derr << "restore_sigset: sigprocmask failed" << dendl;
  }
}

void unblock_all_signals(sigset_t *old_sigset)
{
  sigset_t sigset;
  sigfillset(&sigset);
  sigdelset(&sigset, SIGKILL);
  if (pthread_sigmask(SIG_UNBLOCK, &sigset, old_sigset)) {
    derr << "unblock_all_signals: sigprocmask failed" << dendl;
    if (old_sigset)
      sigaddset(old_sigset, SIGKILL);
    return;
  }
  if (old_sigset)
    sigdelset(old_sigset, SIGKILL);
}
