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

#ifndef CEPH_SIMPLE_SPIN_H
#define CEPH_SIMPLE_SPIN_H

#include <atomic>

inline void simple_spin_lock(std::atomic_flag& lock)
{
 while(lock.test_and_set(std::memory_order_acquire))
  ;
}

inline void simple_spin_unlock(std::atomic_flag& lock)
{
 lock.clear(std::memory_order_release);
}

inline void simple_spin_lock(std::atomic_flag *lock)
{
 simple_spin_lock(*lock);
}

inline void simple_spin_unlock(std::atomic_flag *lock)
{
 simple_spin_unlock(*lock);
}

#endif
