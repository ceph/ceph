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

class CephContext;

extern int g_lockdep;

extern void lockdep_register_ceph_context(CephContext *cct);
extern void lockdep_unregister_ceph_context(CephContext *cct);
extern int lockdep_register(const char *n);
extern void lockdep_unregister(int id);
extern int lockdep_will_lock(const char *n, int id, bool force_backtrace=false);
extern int lockdep_locked(const char *n, int id, bool force_backtrace=false);
extern int lockdep_will_unlock(const char *n, int id);
extern int lockdep_dump_locks();

#endif
