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

#ifndef CEPH_COMMON_SIGNAL_H
#define CEPH_COMMON_SIGNAL_H

#include <signal.h>
#include <string>

typedef void (*signal_handler_t)(int);

void install_sighandler(int signum, signal_handler_t handler, int flags);

// handles SIGHUP
void sighup_handler(int signum);

// Install the standard Ceph signal handlers
void install_standard_sighandlers(void);

// Returns a string showing the set of blocked signals for the calling thread.
// Other threads may have a different set (this is per-thread thing).
std::string signal_mask_to_str();

// Block all signals. On success, stores the old set of blocked signals in
// old_sigset. On failure, stores an invalid set of blocked signals in
// old_sigset.
void block_all_signals(sigset_t *old_sigset);

// Restore the set of blocked signals. Will not restore an invalid set of
// blocked signals.
void restore_sigset(const sigset_t *old_sigset);

// Unblock all signals. On success, stores the old set of blocked signals in
// old_sigset. On failure, stores an invalid set of blocked signals in
// old_sigset.
void unblock_all_signals(sigset_t *old_sigset);

#endif
