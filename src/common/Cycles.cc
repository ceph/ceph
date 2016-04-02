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


#include <errno.h>
#include <sys/time.h>

#include "errno.h"
#include "debug.h"
#include "Initialize.h"
#include "Cycles.h"

double Cycles::cycles_per_sec = 0;

/**
 * Perform once-only overall initialization for the Cycles class, such
 * as calibrating the clock frequency.  This method must be called
 * before using the Cycles module.
 *
 * It is not initialized by default because the timing loops cause
 * general process startup times to balloon
 * (http://tracker.ceph.com/issues/15225).
 */
void Cycles::init()
{
  if (cycles_per_sec != 0)
    return;

  // Skip initialization if rtdsc is not implemented
  if (rdtsc() == 0)
    return;

  // Compute the frequency of the fine-grained CPU timer: to do this,
  // take parallel time readings using both rdtsc and gettimeofday.
  // After 10ms have elapsed, take the ratio between these readings.

  struct timeval start_time, stop_time;
  uint64_t micros;
  double old_cycles;

  // There is one tricky aspect, which is that we could get interrupted
  // between calling gettimeofday and reading the cycle counter, in which
  // case we won't have corresponding readings.  To handle this (unlikely)
  // case, compute the overall result repeatedly, and wait until we get
  // two successive calculations that are within 0.1% of each other.
  old_cycles = 0;
  while (1) {
    if (gettimeofday(&start_time, NULL) != 0) {
      assert(0 == "couldn't read clock");
    }
    uint64_t start_cycles = rdtsc();
    while (1) {
      if (gettimeofday(&stop_time, NULL) != 0) {
        assert(0 == "couldn't read clock");
      }
      uint64_t stop_cycles = rdtsc();
      micros = (stop_time.tv_usec - start_time.tv_usec) +
          (stop_time.tv_sec - start_time.tv_sec)*1000000;
      if (micros > 10000) {
        cycles_per_sec = static_cast<double>(stop_cycles - start_cycles);
        cycles_per_sec = 1000000.0*cycles_per_sec/ static_cast<double>(micros);
        break;
      }
    }
    double delta = cycles_per_sec/1000.0;
    if ((old_cycles > (cycles_per_sec - delta)) &&
        (old_cycles < (cycles_per_sec + delta))) {
      return;
    }
    old_cycles = cycles_per_sec;
  }
}

/**
 * Return the number of CPU cycles per second.
 */
double Cycles::per_second()
{
  return get_cycles_per_sec();
}

/**
 * Given an elapsed time measured in cycles, return a floating-point number
 * giving the corresponding time in seconds.
 * \param cycles
 *      Difference between the results of two calls to rdtsc.
 * \param cycles_per_sec
 *      Optional parameter to specify the frequency of the counter that #cycles
 *      was taken from. Useful when converting a remote machine's tick counter
 *      to seconds. The default value of 0 will use the local processor's
 *      computed counter frequency.
 * \return
 *      The time in seconds corresponding to cycles.
 */
double Cycles::to_seconds(uint64_t cycles, double cycles_per_sec)
{
  if (cycles_per_sec == 0)
    cycles_per_sec = get_cycles_per_sec();
  return static_cast<double>(cycles)/cycles_per_sec;
}

/**
 * Given a time in seconds, return the number of cycles that it
 * corresponds to.
 * \param seconds
 *      Time in seconds.
 * \param cycles_per_sec
 *      Optional parameter to specify the frequency of the counter that #cycles
 *      was taken from. Useful when converting a remote machine's tick counter
 *      to seconds. The default value of 0 will use the local processor's
 *      computed counter frequency.
 * \return
 *      The approximate number of cycles corresponding to #seconds.
 */
uint64_t Cycles::from_seconds(double seconds, double cycles_per_sec)
{
  if (cycles_per_sec == 0)
    cycles_per_sec = get_cycles_per_sec();
  return (uint64_t) (seconds*cycles_per_sec + 0.5);
}

/**
 * Given an elapsed time measured in cycles, return an integer
 * giving the corresponding time in microseconds. Note: to_seconds()
 * is faster than this method.
 * \param cycles
 *      Difference between the results of two calls to rdtsc.
 * \param cycles_per_sec
 *      Optional parameter to specify the frequency of the counter that #cycles
 *      was taken from. Useful when converting a remote machine's tick counter
 *      to seconds. The default value of 0 will use the local processor's
 *      computed counter frequency.
 * \return
 *      The time in microseconds corresponding to cycles (rounded).
 */
uint64_t Cycles::to_microseconds(uint64_t cycles, double cycles_per_sec)
{
  return to_nanoseconds(cycles, cycles_per_sec) / 1000;
}

/**
 * Given an elapsed time measured in cycles, return an integer
 * giving the corresponding time in nanoseconds. Note: to_seconds()
 * is faster than this method.
 * \param cycles
 *      Difference between the results of two calls to rdtsc.
 * \param cycles_per_sec
 *      Optional parameter to specify the frequency of the counter that #cycles
 *      was taken from. Useful when converting a remote machine's tick counter
 *      to seconds. The default value of 0 will use the local processor's
 *      computed counter frequency.
 * \return
 *      The time in nanoseconds corresponding to cycles (rounded).
 */
uint64_t Cycles::to_nanoseconds(uint64_t cycles, double cycles_per_sec)
{
  if (cycles_per_sec == 0)
    cycles_per_sec = get_cycles_per_sec();
  return (uint64_t) (1e09*static_cast<double>(cycles)/cycles_per_sec + 0.5);
}

/**
 * Given a number of nanoseconds, return an approximate number of
 * cycles for an equivalent time length.
 * \param ns
 *      Number of nanoseconds.
 * \param cycles_per_sec
 *      Optional parameter to specify the frequency of the counter that #cycles
 *      was taken from. Useful when converting a remote machine's tick counter
 *      to seconds. The default value of 0 will use the local processor's
 *      computed counter frequency.
 * \return
 *      The approximate number of cycles for the same time length.
 */
uint64_t
Cycles::from_nanoseconds(uint64_t ns, double cycles_per_sec)
{
  if (cycles_per_sec == 0)
    cycles_per_sec = get_cycles_per_sec();
  return (uint64_t) (static_cast<double>(ns)*cycles_per_sec/1e09 + 0.5);
}

/**
 * Busy wait for a given number of microseconds.
 * Callers should use this method in most reasonable cases as opposed to
 * usleep for accurate measurements. Calling usleep may put the the processor
 * in a low power mode/sleep state which reduces the clock frequency.
 * So, each time the process/thread wakes up from usleep, it takes some time
 * to ramp up to maximum frequency. Thus meausrements often incur higher
 * latencies.
 * \param us
 *      Number of microseconds.
 */
void
Cycles::sleep(uint64_t us)
{
  uint64_t stop = Cycles::rdtsc() + Cycles::from_nanoseconds(1000*us);
  while (Cycles::rdtsc() < stop);
}
