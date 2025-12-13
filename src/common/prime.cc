// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "common/prime.h"

//This is not a performance implementation
//This is a function that will return true if the int n is a prime number and false if not.
bool is_prime(int n) {
    if (n <= 1) return false;
    for (int i = 2; i*i <= n; ++i) {
        if (n % i == 0) {
          return false;
        }
    }
    return true;
}