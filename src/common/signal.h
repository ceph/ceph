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

// Returns a string showing the set of blocked signals for the calling thread.
// Other threads may have a different set (this is per-thread thing).
extern std::string signal_mask_to_str();

// Block a list of signals. If siglist == NULL, blocks all signals.
// If not, the list is terminated with a 0 element.
//
// On success, stores the old set of blocked signals in
// old_sigset. On failure, stores an invalid set of blocked signals in
// old_sigset.
extern void block_signals(const int *siglist, sigset_t *old_sigset);

// Restore the set of blocked signals. Will not restore an invalid set of
// blocked signals.
extern void restore_sigset(const sigset_t *old_sigset);

// Unblock all signals. On success, stores the old set of blocked signals in
// old_sigset. On failure, stores an invalid set of blocked signals in
// old_sigset.
extern void unblock_all_signals(sigset_t *old_sigset);

#endif
