// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_signal.h"
#include "global/signal_handler.h"
#include "common/safe_io.h"
#include "common/errno.h"
#include "rgw_main.h"
#include "rgw_log.h"

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context


static int signal_fd[2] = {0, 0};

namespace rgw {
namespace signal {

void sig_handler_noop(int signum) {
  /* NOP */
} /* sig_handler_noop */

void sighup_handler(int signum) {
    if (rgw::AppMain::ops_log_file != nullptr) {
        rgw::AppMain::ops_log_file->reopen();
    }
    g_ceph_context->reopen_logs();
} /* sighup_handler */

void signal_shutdown()
{
  int val = 0;
  int ret = write(signal_fd[0], (char *)&val, sizeof(val));
  if (ret < 0) {
    derr << "ERROR: " << __func__ << ": write() returned "
         << cpp_strerror(errno) << dendl;
  }
} /* signal_shutdown */

void wait_shutdown()
{
  int val;
  int r = safe_read_exact(signal_fd[1], &val, sizeof(val));
  if (r < 0) {
    derr << "safe_read_exact returned with error" << dendl;
  }
} /* wait_shutdown */

int signal_fd_init()
{
  return socketpair(AF_UNIX, SOCK_STREAM, 0, signal_fd);
} /* signal_fd_init */

void signal_fd_finalize()
{
  close(signal_fd[0]);
  close(signal_fd[1]);
} /* signal_fd_finalize */

void handle_sigterm(int signum)
{
  dout(1) << __func__ << dendl;

  // send a signal to make fcgi's accept(2) wake up.  unfortunately the
  // initial signal often isn't sufficient because we race with accept's
  // check of the flag wet by ShutdownPending() above.
  if (signum != SIGUSR1) {
    signal_shutdown();

    // safety net in case we get stuck doing an orderly shutdown.
    uint64_t secs = g_ceph_context->_conf->rgw_exit_timeout_secs;
    if (secs)
      alarm(secs);
    dout(1) << __func__ << " set alarm for " << secs << dendl;
  }
} /* handle_sigterm */

}} /* namespace rgw::signal */
