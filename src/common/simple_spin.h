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

/* This is a simple spinlock implementation based on atomic compare-and-swap.
 * Like all spinlocks, it is intended to protect short, simple critical
 * sections. It is signal-safe. Unlike pthread_spin_lock and friends, it has a
 * static initializer so you can write:
 *
 * simple_spinlock_t my_spinlock = SIMPLE_SPINLOCK_INITIALIZER 
 *
 * This allows you to use the lock anywhere you want-- even in global
 * constructors. Since simple_spinlock_t is a primitive type, it will start out
 * correctly initialized.
 */

#include <stdint.h>

typedef uint32_t simple_spinlock_t;

#define SIMPLE_SPINLOCK_INITIALIZER 0

void simple_spin_lock(simple_spinlock_t *lock);
void simple_spin_unlock(simple_spinlock_t *lock);

#endif
