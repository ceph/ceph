// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <functional>
#include <memory>
#include <thread>

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

// The current CPU, or a number of this thread when that is not known: an
// index for a per-CPU slot (masked by the caller)
inline unsigned cpu_index()
{
  int cpu = current_cpu();
  if (cpu >= 0)
    return unsigned(cpu);
  return unsigned(std::hash<std::thread::id>{}(std::this_thread::get_id()));
}

/*
 * How many per-CPU slots a structure gets: @want if it is not 0 (an
 * option), else @dflt but no more than this machine's CPUs (both rounded
 * up to a power of two).
 */
inline unsigned percpu_slots(unsigned want, unsigned dflt)
{
  if (want)
    return std::bit_ceil(std::clamp(want, 1u, 4096u));
  static const unsigned ncpu = [] {
    unsigned n = std::thread::hardware_concurrency();  // 0 if not known
    return std::bit_ceil(std::clamp(n, 1u, 4096u));
  }();
  return std::min(std::bit_ceil(std::max(dflt, 1u)), ncpu);
}

/*
 * An array of slots, one per CPU modulo its size (a power of two), for
 * counts that threads on many CPUs change at once: each changes its own
 * CPU's.  T should be cache-line aligned.  resize() only while nobody
 * uses it.
 */
template <typename T>
class PerCpuSlots {
public:
  explicit PerCpuSlots(unsigned dflt) : dflt(dflt) {
    resize(0);
  }
  // @want as for percpu_slots()
  void resize(unsigned want) {
    unsigned n = percpu_slots(want, dflt);
    s = std::make_unique<T[]>(n);
    m = n - 1;
  }
  unsigned size() const { return m + 1; }
  unsigned mask() const { return m; }
  T& operator[](size_t i) { return s[i & m]; }
  const T& operator[](size_t i) const { return s[i & m]; }
  T& here() { return s[cpu_index() & m]; }
  T* begin() { return s.get(); }
  T* end() { return s.get() + m + 1; }
  const T* begin() const { return s.get(); }
  const T* end() const { return s.get() + m + 1; }
private:
  const unsigned dflt;
  unsigned m = 0;
  std::unique_ptr<T[]> s;
};

} // namespace ceph
