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

#ifndef CEPH_LOCKDEP_H
#define CEPH_LOCKDEP_H

#include "include/common_fwd.h"

#ifdef CEPH_DEBUG_MUTEX

extern bool g_lockdep;

extern void lockdep_register_ceph_context(CephContext *cct);
extern void lockdep_unregister_ceph_context(CephContext *cct);
// lockdep tracks dependencies between multiple and different instances
// of locks within a class denoted by `n`.
// Caller is obliged to guarantee name uniqueness.
extern int lockdep_register(const char *n);
extern void lockdep_unregister(int id);
extern int lockdep_will_lock(const char *n, int id, bool force_backtrace=false,
			     bool recursive=false);
extern int lockdep_locked(const char *n, int id, bool force_backtrace=false);
extern int lockdep_will_unlock(const char *n, int id);
extern int lockdep_dump_locks();

#else

static constexpr bool g_lockdep = false;
#define lockdep_register(...) 0
#define lockdep_unregister(...)
#define lockdep_will_lock(...) 0
#define lockdep_locked(...) 0
#define lockdep_will_unlock(...) 0

#endif	// CEPH_DEBUG_MUTEX


#endif
