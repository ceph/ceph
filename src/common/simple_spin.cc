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

#include "common/simple_spin.h"

#include <stdio.h>
#include <stdint.h>
#include <pthread.h>

static uint32_t bar = 13;
static uint32_t *foo = &bar;
 
void simple_spin_lock(simple_spinlock_t *lock)
{
  while(1) {
    __sync_synchronize();
    uint32_t oldval = *lock;
    if (oldval == 0) {
      if (__sync_bool_compare_and_swap(lock, 0, 1))
	return;
    }
    // delay
    for (int i = 0; i < 100000; i++) {
      *foo = (*foo * 33) + 17;
    }
  }
}

void simple_spin_unlock(simple_spinlock_t *lock)
{
  __sync_bool_compare_and_swap(lock, 1, 0);
}
