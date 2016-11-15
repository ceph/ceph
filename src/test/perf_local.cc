// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/* Copyright (c) 2015 Haomai Wang <haomaiwang@gmail.com>
 * Copyright (c) 2011-2014 Stanford University
 * Copyright (c) 2011 Facebook
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

// This program contains a collection of low-level performance measurements
// for Ceph, which can be run either individually or altogether.  These
// tests measure performance in a single stand-alone process, not in a cluster
// with multiple servers.  Invoke the program like this:
//
//     Perf test1 test2 ...
//
// test1 and test2 are the names of individual performance measurements to
// run.  If no test names are provided then all of the performance tests
// are run.
//
// To add a new test:
// * Write a function that implements the test.  Use existing test functions
//   as a guideline, and be sure to generate output in the same form as
//   other tests.
// * Create a new entry for the test in the #tests table.
#include <vector>
#include <sched.h>

#include "acconfig.h"
#ifdef HAVE_SSE
#include <xmmintrin.h>
#endif

#include "include/atomic.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/ceph_hash.h"
#include "include/Spinlock.h"
#include "common/ceph_argparse.h"
#include "common/Cycles.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Thread.h"
#include "common/Timer.h"
#include "msg/async/Event.h"
#include "global/global_init.h"

#include "test/perf_helper.h"

using namespace ceph;

/**
 * Ask the operating system to pin the current thread to a given CPU.
 *
 * \param cpu
 *      Indicates the desired CPU and hyperthread; low order 2 bits
 *      specify CPU, next bit specifies hyperthread.
 */
void bind_thread_to_cpu(int cpu)
{
#ifdef HAVE_SCHED
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  sched_setaffinity(0, sizeof(set), &set);
#endif
}

/*
 * This function just discards its argument. It's used to make it
 * appear that data is used,  so that the compiler won't optimize
 * away the code we're trying to measure.
 *
 * \param value
 *      Pointer to arbitrary value; it's discarded.
 */
void discard(void* value) {
  int x = *reinterpret_cast<int*>(value);
  if (x == 0x43924776) {
    printf("Value was 0x%x\n", x);
  }
}

//----------------------------------------------------------------------
// Test functions start here
//----------------------------------------------------------------------

// Measure the cost of atomic_t::compare_and_swap
double atomic_int_cmp()
{
  int count = 1000000;
  atomic_t value(11);
  int test = 11;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    value.compare_and_swap(test, test+2);
    test += 2;
  }
  uint64_t stop = Cycles::rdtsc();
  // printf("Final value: %d\n", value.load());
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of atomic_t::inc
double atomic_int_inc()
{
  int count = 1000000;
  atomic_t value(11);
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    value.inc();
  }
  uint64_t stop = Cycles::rdtsc();
  // printf("Final value: %d\n", value.load());
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of reading an atomic_t
double atomic_int_read()
{
  int count = 1000000;
  atomic_t value(11);
  int total = 0;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    total += value.read();
  }
  uint64_t stop = Cycles::rdtsc();
  // printf("Total: %d\n", total);
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of storing a new value in a atomic_t
double atomic_int_set()
{
  int count = 1000000;
  atomic_t value(11);
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    value.set(88);
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of acquiring and releasing a mutex in the
// fast case where the mutex is free.
double mutex_nonblock()
{
  int count = 1000000;
  Mutex m("mutex_nonblock::m");
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    m.Lock();
    m.Unlock();
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of allocating and deallocating a buffer, plus
// appending (logically) one ptr.
double buffer_basic()
{
  int count = 1000000;
  uint64_t start = Cycles::rdtsc();
  bufferptr ptr("abcdefg", 7);
  for (int i = 0; i < count; i++) {
    bufferlist b;
    b.append(ptr, 0, 5);
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

struct DummyBlock {
  int a, b, c, d;
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(a, bl);
    ::encode(b, bl);
    ::encode(c, bl);
    ::encode(d, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(a, bl);
    ::decode(b, bl);
    ::decode(c, bl);
    ::decode(d, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(DummyBlock)

// Measure the cost of encoding and decoding a buffer, plus
// allocating space for one chunk.
double buffer_encode_decode()
{
  int count = 1000000;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    bufferlist b;
    DummyBlock dummy_block;
    ::encode(dummy_block, b);
    bufferlist::iterator iter = b.begin();
    ::decode(dummy_block, iter);
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of allocating and deallocating a buffer, plus
// copying in a small block.
double buffer_basic_copy()
{
  int count = 1000000;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    bufferlist b;
    b.append("abcdefg", 6);
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of making a copy of parts of two ptrs.
double buffer_copy()
{
  int count = 1000000;
  bufferlist b;
  b.append("abcde", 5);
  b.append("01234", 5);
  char copy[10];
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    b.copy(2, 6, copy);
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of allocating new space by extending the
// bufferlist
double buffer_encode()
{
  int count = 100000;
  uint64_t total = 0;
  for (int i = 0; i < count; i++) {
    bufferlist b;
    DummyBlock dummy_block;
    ::encode(dummy_block, b);
    uint64_t start = Cycles::rdtsc();
    ::encode(dummy_block, b);
    ::encode(dummy_block, b);
    ::encode(dummy_block, b);
    ::encode(dummy_block, b);
    ::encode(dummy_block, b);
    ::encode(dummy_block, b);
    ::encode(dummy_block, b);
    ::encode(dummy_block, b);
    ::encode(dummy_block, b);
    ::encode(dummy_block, b);
    total += Cycles::rdtsc() - start;
  }
  return Cycles::to_seconds(total)/(count*10);
}

// Measure the cost of retrieving an object from the beginning of a buffer.
double buffer_get_contiguous()
{
  int count = 1000000;
  int value = 11;
  bufferlist b;
  b.append((char*)&value, sizeof(value));
  int sum = 0;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    sum += *reinterpret_cast<int*>(b.get_contiguous(0, sizeof(value)));
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of creating an iterator and iterating over 10
// chunks in a buffer.
double buffer_iterator()
{
  bufferlist b;
  const char s[] = "abcdefghijklmnopqrstuvwxyz";
  bufferptr ptr(s, sizeof(s));
  for (int i = 0; i < 5; i++) {
    b.append(ptr, i, 5);
  }
  int count = 100000;
  int sum = 0;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    bufferlist::iterator it = b.begin();
    while (!it.end()) {
      sum += (static_cast<const char*>(it.get_current_ptr().c_str()))[it.get_remaining()-1];
      ++it;
    }
  }
  uint64_t stop = Cycles::rdtsc();
  discard(&sum);
  return Cycles::to_seconds(stop - start)/count;
}

// Implements the CondPingPong test.
class CondPingPong {
  Mutex mutex;
  Cond cond;
  int prod;
  int cons;
  const int count;

  class Consumer : public Thread {
    CondPingPong *p;
   public:
    explicit Consumer(CondPingPong *p): p(p) {}
    void* entry() {
      p->consume();
      return 0;
    }
  } consumer;

 public:
  CondPingPong(): mutex("CondPingPong::mutex"), prod(0), cons(0), count(10000), consumer(this) {}

  double run() {
    consumer.create("consumer");
    uint64_t start = Cycles::rdtsc();
    produce();
    uint64_t stop = Cycles::rdtsc();
    consumer.join();
    return Cycles::to_seconds(stop - start)/count;
  }

  void produce() {
    Mutex::Locker l(mutex);
    while (cons < count) {
      while (cons < prod)
        cond.Wait(mutex);
      ++prod;
      cond.Signal();
    }
  }

  void consume() {
    Mutex::Locker l(mutex);
    while (cons < count) {
      while (cons == prod)
        cond.Wait(mutex);
      ++cons;
      cond.Signal();
    }
  }
};

// Measure the cost of coordinating between threads using a condition variable.
double cond_ping_pong()
{
  return CondPingPong().run();
}

// Measure the cost of a 32-bit divide. Divides don't take a constant
// number of cycles. Values were chosen here semi-randomly to depict a
// fairly expensive scenario. Someone with fancy ALU knowledge could
// probably pick worse values.
double div32()
{
#if defined(__i386__) || defined(__x86_64__)
  int count = 1000000;
  uint64_t start = Cycles::rdtsc();
  // NB: Expect an x86 processor exception is there's overflow.
  uint32_t numeratorHi = 0xa5a5a5a5U;
  uint32_t numeratorLo = 0x55aa55aaU;
  uint32_t divisor = 0xaa55aa55U;
  uint32_t quotient;
  uint32_t remainder;
  for (int i = 0; i < count; i++) {
    __asm__ __volatile__("div %4" :
                         "=a"(quotient), "=d"(remainder) :
                         "a"(numeratorLo), "d"(numeratorHi), "r"(divisor) :
                         "cc");
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
#else
  return -1;
#endif
}

// Measure the cost of a 64-bit divide. Divides don't take a constant
// number of cycles. Values were chosen here semi-randomly to depict a
// fairly expensive scenario. Someone with fancy ALU knowledge could
// probably pick worse values.
double div64()
{
#if defined(__x86_64__) || defined(__amd64__)
  int count = 1000000;
  // NB: Expect an x86 processor exception is there's overflow.
  uint64_t start = Cycles::rdtsc();
  uint64_t numeratorHi = 0x5a5a5a5a5a5UL;
  uint64_t numeratorLo = 0x55aa55aa55aa55aaUL;
  uint64_t divisor = 0xaa55aa55aa55aa55UL;
  uint64_t quotient;
  uint64_t remainder;
  for (int i = 0; i < count; i++) {
    __asm__ __volatile__("divq %4" :
                         "=a"(quotient), "=d"(remainder) :
                         "a"(numeratorLo), "d"(numeratorHi), "r"(divisor) :
                         "cc");
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
#else
  return -1;
#endif
}

// Measure the cost of calling a non-inlined function.
double function_call()
{
  int count = 1000000;
  uint64_t x = 0;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    x = PerfHelper::plus_one(x);
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the minimum cost of EventCenter::process_events, when there are no
// Pollers and no Timers.
double eventcenter_poll()
{
  int count = 1000000;
  EventCenter center(g_ceph_context);
  center.init(1000);
  center.set_owner();
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    center.process_events(0);
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

class CenterWorker : public Thread {
  CephContext *cct;
  bool done;

 public:
  EventCenter center;
  explicit CenterWorker(CephContext *c): cct(c), done(false), center(c) {
    center.init(100);
  }
  void stop() {
    done = true;
    center.wakeup();
  }
  void* entry() {
    center.set_owner();
    bind_thread_to_cpu(2);
    while (!done)
      center.process_events(1000);
    return 0;
  }
};

class CountEvent: public EventCallback {
  atomic_t *count;

 public:
  explicit CountEvent(atomic_t *atomic): count(atomic) {}
  void do_request(int id) {
    count->dec();
  }
};

double eventcenter_dispatch()
{
  int count = 100000;

  CenterWorker worker(g_ceph_context);
  atomic_t flag(1);
  worker.create("evt_center_disp");
  EventCallbackRef count_event(new CountEvent(&flag));

  worker.center.dispatch_event_external(count_event);
  // Start a new thread and wait for it to ready.
  while (flag.read())
    usleep(100);

  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    flag.set(1);
    worker.center.dispatch_event_external(count_event);
    while (flag.read())
      ;
  }
  uint64_t stop = Cycles::rdtsc();
  worker.stop();
  worker.join();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of copying a given number of bytes with memcpy.
double memcpy_shared(size_t size)
{
  int count = 1000000;
  char src[size], dst[size];

  memset(src, 0, sizeof(src));

  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    memcpy(dst, src, size);
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

double memcpy100()
{
  return memcpy_shared(100);
}

double memcpy1000()
{
  return memcpy_shared(1000);
}

double memcpy10000()
{
  return memcpy_shared(10000);
}

// Benchmark rjenkins hashing performance on cached data.
template <int key_length>
double ceph_str_hash_rjenkins()
{
  int count = 100000;
  char buf[key_length];

  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++)
    ceph_str_hash(CEPH_STR_HASH_RJENKINS, buf, sizeof(buf));
  uint64_t stop = Cycles::rdtsc();

  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of reading the fine-grain cycle counter.
double rdtsc_test()
{
  int count = 1000000;
  uint64_t start = Cycles::rdtsc();
  uint64_t total = 0;
  for (int i = 0; i < count; i++) {
    total += Cycles::rdtsc();
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of the Cycles::to_seconds method.
double perf_cycles_to_seconds()
{
  int count = 1000000;
  double total = 0;
  uint64_t cycles = 994261;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    total += Cycles::to_seconds(cycles);
  }
  uint64_t stop = Cycles::rdtsc();
  // printf("Result: %.4f\n", total/count);
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of the Cylcles::toNanoseconds method.
double perf_cycles_to_nanoseconds()
{
  int count = 1000000;
  uint64_t total = 0;
  uint64_t cycles = 994261;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    total += Cycles::to_nanoseconds(cycles);
  }
  uint64_t stop = Cycles::rdtsc();
  // printf("Result: %lu\n", total/count);
  return Cycles::to_seconds(stop - start)/count;
}


#ifdef HAVE_SSE
/**
 * Prefetch the cache lines containing [object, object + numBytes) into the
 * processor's caches.
 * The best docs for this are in the Intel instruction set reference under
 * PREFETCH.
 * \param object
 *      The start of the region of memory to prefetch.
 * \param num_bytes
 *      The size of the region of memory to prefetch.
 */
static inline void prefetch(const void *object, uint64_t num_bytes)
{
    uint64_t offset = reinterpret_cast<uint64_t>(object) & 0x3fUL;
    const char* p = reinterpret_cast<const char*>(object) - offset;
    for (uint64_t i = 0; i < offset + num_bytes; i += 64)
        _mm_prefetch(p + i, _MM_HINT_T0);
}
#endif

// Measure the cost of the prefetch instruction.
double perf_prefetch()
{
#ifdef HAVE_SSE
  uint64_t total_ticks = 0;
  int count = 10;
  char buf[16 * 64];
  uint64_t start, stop;

  for (int i = 0; i < count; i++) {
    PerfHelper::flush_cache();
    start = Cycles::rdtsc();
    prefetch(&buf[576], 64);
    prefetch(&buf[0],   64);
    prefetch(&buf[512], 64);
    prefetch(&buf[960], 64);
    prefetch(&buf[640], 64);
    prefetch(&buf[896], 64);
    prefetch(&buf[256], 64);
    prefetch(&buf[704], 64);
    prefetch(&buf[320], 64);
    prefetch(&buf[384], 64);
    prefetch(&buf[128], 64);
    prefetch(&buf[448], 64);
    prefetch(&buf[768], 64);
    prefetch(&buf[832], 64);
    prefetch(&buf[64],  64);
    prefetch(&buf[192], 64);
    stop = Cycles::rdtsc();
    total_ticks += stop - start;
  }
  return Cycles::to_seconds(total_ticks) / count / 16;
#else
  return -1;
#endif
}

#if defined(__x86_64__)
/**
 * This function is used to seralize machine instructions so that no
 * instructions that appear after it in the current thread can run before any
 * instructions that appear before it. 
 *
 * It is useful for putting around rdpmc instructions (to pinpoint cache
 * misses) as well as before rdtsc instructions, to prevent time pollution from
 * instructions supposed to be executing before the timer starts.
 */
static inline void serialize() {
    uint32_t eax, ebx, ecx, edx;
    __asm volatile("cpuid"
        : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
        : "a" (1U));
}
#endif

// Measure the cost of cpuid
double perf_serialize() {
#if defined(__x86_64__)
  int count = 1000000;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    serialize();
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
#else
  return -1;
#endif
}

// Measure the cost of an lfence instruction.
double lfence()
{
#ifdef HAVE_SSE2
  int count = 1000000;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    __asm__ __volatile__("lfence" ::: "memory");
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
#else
  return -1;
#endif
}

// Measure the cost of an sfence instruction.
double sfence()
{
#ifdef HAVE_SSE
  int count = 1000000;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    __asm__ __volatile__("sfence" ::: "memory");
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
#else
  return -1;
#endif
}

// Measure the cost of acquiring and releasing a SpinLock (assuming the
// lock is initially free).
double test_spinlock()
{
  int count = 1000000;
  Spinlock lock;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    lock.lock();
    lock.unlock();
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Helper for spawn_thread. This is the main function that the thread executes
// (intentionally empty).
class ThreadHelper : public Thread {
  void *entry() { return 0; }
};

// Measure the cost of start and joining with a thread.
double spawn_thread()
{
  int count = 10000;
  ThreadHelper thread;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    thread.create("thread_helper");
    thread.join();
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

class FakeContext : public Context {
 public:
  virtual void finish(int r) {}
};

// Measure the cost of starting and stopping a Dispatch::Timer.
double perf_timer()
{
  int count = 1000000;
  Mutex lock("perf_timer::lock");
  SafeTimer timer(g_ceph_context, lock);
  FakeContext **c = new FakeContext*[count];
  for (int i = 0; i < count; i++) {
    c[i] = new FakeContext();
  }
  uint64_t start = Cycles::rdtsc();
  Mutex::Locker l(lock);
  for (int i = 0; i < count; i++) {
    timer.add_event_after(12345, c[i]);
    timer.cancel_event(c[i]);
  }
  uint64_t stop = Cycles::rdtsc();
  delete[] c;
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of throwing and catching an int. This uses an integer as
// the value thrown, which is presumably as fast as possible.
double throw_int()
{
  int count = 10000;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    try {
      throw 0;
    } catch (int) { // NOLINT
      // pass
    }
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of throwing and catching an int from a function call.
double throw_int_call()
{
  int count = 10000;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    try {
      PerfHelper::throw_int();
    } catch (int) { // NOLINT
      // pass
    }
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of throwing and catching an Exception. This uses an actual
// exception as the value thrown, which may be slower than throwInt.
double throw_exception()
{
  int count = 10000;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    try {
      throw buffer::end_of_buffer();
    } catch (const buffer::end_of_buffer&) {
      // pass
    }
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of throwing and catching an Exception from a function call.
double throw_exception_call()
{
  int count = 10000;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    try {
      PerfHelper::throw_end_of_buffer();
    } catch (const buffer::end_of_buffer&) {
      // pass
    }
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// Measure the cost of pushing a new element on a std::vector, copying
// from the end to an internal element, and popping the end element.
double vector_push_pop()
{
  int count = 100000;
  std::vector<int> vector;
  vector.push_back(1);
  vector.push_back(2);
  vector.push_back(3);
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    vector.push_back(i);
    vector.push_back(i+1);
    vector.push_back(i+2);
    vector[2] = vector.back();
    vector.pop_back();
    vector[0] = vector.back();
    vector.pop_back();
    vector[1] = vector.back();
    vector.pop_back();
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/(count*3);
}

// Measure the cost of ceph_clock_now
double perf_ceph_clock_now()
{
  int count = 100000;
  uint64_t start = Cycles::rdtsc();
  for (int i = 0; i < count; i++) {
    ceph_clock_now(g_ceph_context);
  }
  uint64_t stop = Cycles::rdtsc();
  return Cycles::to_seconds(stop - start)/count;
}

// The following struct and table define each performance test in terms of
// a string name and a function that implements the test.
struct TestInfo {
  const char* name;             // Name of the performance test; this is
                                // what gets typed on the command line to
                                // run the test.
  double (*func)();             // Function that implements the test;
                                // returns the time (in seconds) for each
                                // iteration of that test.
  const char *description;      // Short description of this test (not more
                                // than about 40 characters, so the entire
                                // test output fits on a single line).
};
TestInfo tests[] = {
  {"atomic_int_cmp", atomic_int_cmp,
    "atomic_t::compare_and_swap"},
  {"atomic_int_inc", atomic_int_inc,
    "atomic_t::inc"},
  {"atomic_int_read", atomic_int_read,
    "atomic_t::read"},
  {"atomic_int_set", atomic_int_set,
    "atomic_t::set"},
  {"mutex_nonblock", mutex_nonblock,
    "Mutex lock/unlock (no blocking)"},
  {"buffer_basic", buffer_basic,
    "buffer create, add one ptr, delete"},
  {"buffer_encode_decode", buffer_encode_decode,
    "buffer create, encode/decode object, delete"},
  {"buffer_basic_copy", buffer_basic_copy,
    "buffer create, copy small block, delete"},
  {"buffer_copy", buffer_copy,
    "copy out 2 small ptrs from buffer"},
  {"buffer_encode10", buffer_encode,
    "buffer encoding 10 structures onto existing ptr"},
  {"buffer_get_contiguous", buffer_get_contiguous,
    "Buffer::get_contiguous"},
  {"buffer_iterator", buffer_iterator,
    "iterate over buffer with 5 ptrs"},
  {"cond_ping_pong", cond_ping_pong,
    "condition variable round-trip"},
  {"div32", div32,
    "32-bit integer division instruction"},
  {"div64", div64,
    "64-bit integer division instruction"},
  {"function_call", function_call,
    "Call a function that has not been inlined"},
  {"eventcenter_poll", eventcenter_poll,
    "EventCenter::process_events (no timers or events)"},
  {"eventcenter_dispatch", eventcenter_dispatch,
    "EventCenter::dispatch_event_external latency"},
  {"memcpy100", memcpy100,
    "Copy 100 bytes with memcpy"},
  {"memcpy1000", memcpy1000,
    "Copy 1000 bytes with memcpy"},
  {"memcpy10000", memcpy10000,
    "Copy 10000 bytes with memcpy"},
  {"ceph_str_hash_rjenkins", ceph_str_hash_rjenkins<16>,
    "rjenkins hash on 16 byte of data"},
  {"ceph_str_hash_rjenkins", ceph_str_hash_rjenkins<256>,
    "rjenkins hash on 256 bytes of data"},
  {"rdtsc", rdtsc_test,
    "Read the fine-grain cycle counter"},
  {"cycles_to_seconds", perf_cycles_to_seconds,
    "Convert a rdtsc result to (double) seconds"},
  {"cycles_to_seconds", perf_cycles_to_nanoseconds,
    "Convert a rdtsc result to (uint64_t) nanoseconds"},
  {"prefetch", perf_prefetch,
    "Prefetch instruction"},
  {"serialize", perf_serialize,
    "serialize instruction"},
  {"lfence", lfence,
    "Lfence instruction"},
  {"sfence", sfence,
    "Sfence instruction"},
  {"spin_lock", test_spinlock,
    "Acquire/release SpinLock"},
  {"spawn_thread", spawn_thread,
    "Start and stop a thread"},
  {"perf_timer", perf_timer,
    "Insert and cancel a SafeTimer"},
  {"throw_int", throw_int,
    "Throw an int"},
  {"throw_int_call", throw_int_call,
    "Throw an int in a function call"},
  {"throw_exception", throw_exception,
    "Throw an Exception"},
  {"throw_exception_call", throw_exception_call,
    "Throw an Exception in a function call"},
  {"vector_push_pop", vector_push_pop,
    "Push and pop a std::vector"},
  {"ceph_clock_now", perf_ceph_clock_now,
   "ceph_clock_now function"},
};

/**
 * Runs a particular test and prints a one-line result message.
 *
 * \param info
 *      Describes the test to run.
 */
void run_test(TestInfo& info)
{
  double secs = info.func();
  int width = printf("%-24s ", info.name);
  if (secs == -1) {
    width += printf(" architecture nonsupport ");
  } else if (secs < 1.0e-06) {
    width += printf("%8.2fns", 1e09*secs);
  } else if (secs < 1.0e-03) {
    width += printf("%8.2fus", 1e06*secs);
  } else if (secs < 1.0) {
    width += printf("%8.2fms", 1e03*secs);
  } else {
    width += printf("%8.2fs", secs);
  }
  printf("%*s %s\n", 32-width, "", info.description);
}

int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  Cycles::init();

  bind_thread_to_cpu(3);
  if (argc == 1) {
    // No test names specified; run all tests.
    for (size_t i = 0; i < sizeof(tests)/sizeof(TestInfo); ++i) {
      run_test(tests[i]);
    }
  } else {
    // Run only the tests that were specified on the command line.
    for (int i = 1; i < argc; i++) {
      bool found_test = false;
      for (size_t j = 0; j < sizeof(tests)/sizeof(TestInfo); ++j) {
        if (strcmp(argv[i], tests[j].name) == 0) {
          found_test = true;
          run_test(tests[j]);
          break;
        }
      }
      if (!found_test) {
        int width = printf("%-24s ??", argv[i]);
        printf("%*s No such test\n", 32-width, "");
      }
    }
  }
}
