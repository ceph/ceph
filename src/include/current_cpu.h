// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#ifdef __linux__
#include <sched.h>
#if defined(__GLIBC__) && \
    (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 35))
#include <sys/rseq.h>
#define CEPH_HAVE_RSEQ_CPU 1
#endif
#endif

namespace ceph {

/*
 * The CPU this thread runs on, or -1 if that is not known: a hint for
 * picking a per-CPU slot, which may be stale by the time it is used.
 * glibc registers an rseq area for every thread, where the kernel keeps
 * the current CPU: read it there, which costs a load, rather than call
 * sched_getcpu(), which cost a library call and a vDSO call for each of
 * the per-CPU counters on the hot paths.
 */
inline int current_cpu()
{
#ifdef CEPH_HAVE_RSEQ_CPU
  if (__rseq_size > 0) {
    auto rs = reinterpret_cast<const struct rseq*>(
      static_cast<const char*>(__builtin_thread_pointer()) + __rseq_offset);
    int cpu = static_cast<int>(__atomic_load_n(&rs->cpu_id, __ATOMIC_RELAXED));
    if (cpu >= 0)
      return cpu;
  }
#endif
#ifdef __linux__
  return sched_getcpu();
#else
  return -1;
#endif
}

} // namespace ceph
