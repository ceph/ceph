// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
/* Copyright (c) 2011-2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */


#ifndef CEPH_CYCLES_H
#define CEPH_CYCLES_H

/**
 * This class provides static methods that read the fine-grain CPU
 * cycle counter and translate between cycle-level times and absolute
 * times.
 */
class Cycles {
 public:
  static void init();

  /**
   * Return the current value of the fine-grain CPU cycle counter
   * (accessed via the RDTSC instruction).
   */
  static __inline __attribute__((always_inline)) uint64_t rdtsc() {
#if defined(__i386__)
    int64_t ret;
    __asm__ volatile ("rdtsc" : "=A" (ret) );
    return ret;
#elif defined(__x86_64__) || defined(__amd64__)
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t)hi << 32) | lo);
#elif defined(__aarch64__)
    //
    // arch/arm64/include/asm/arch_timer.h
    //
    // static inline u64 arch_counter_get_cntvct(void)
    // {
    //         u64 cval;
    // 
    //         isb();
    //         asm volatile("mrs %0, cntvct_el0" : "=r" (cval));
    // 
    //         return cval;
    // }
    //
    // https://github.com/cloudius-systems/osv/blob/master/arch/aarch64/arm-clock.cc
    uint64_t cntvct;
    asm volatile ("isb; mrs %0, cntvct_el0; isb; " : "=r" (cntvct) :: "memory");
    return cntvct;
#elif defined(__powerpc__) || defined (__powerpc64__)
    // Based on:
    // https://github.com/randombit/botan/blob/net.randombit.botan/src/lib/entropy/hres_timer/hres_timer.cpp
    uint32_t lo = 0, hi = 0;
    asm volatile("mftbu %0; mftb %1" : "=r" (hi), "=r" (lo));
    return (((uint64_t)hi << 32) | lo);
#else
#warning No high-precision counter available for your OS/arch
    return 0;
#endif
  }

  static double per_second();
  static double to_seconds(uint64_t cycles, double cycles_per_sec = 0);
  static uint64_t from_seconds(double seconds, double cycles_per_sec = 0);
  static uint64_t to_microseconds(uint64_t cycles, double cycles_per_sec = 0);
  static uint64_t to_nanoseconds(uint64_t cycles, double cycles_per_sec = 0);
  static uint64_t from_nanoseconds(uint64_t ns, double cycles_per_sec = 0);
  static void sleep(uint64_t us);

private:
  Cycles();

  /// Conversion factor between cycles and the seconds; computed by
  /// Cycles::init.
  static double cycles_per_sec;

  /**
   * Returns the conversion factor between cycles in seconds, using
   * a mock value for testing when appropriate.
   */
  static __inline __attribute__((always_inline)) double get_cycles_per_sec() {
    return cycles_per_sec;
  }
};

#endif  // CEPH_CYCLES_H
