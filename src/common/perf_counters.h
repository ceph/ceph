// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 * Copyright (C) 2017 OVH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_COMMON_PERF_COUNTERS_H
#define CEPH_COMMON_PERF_COUNTERS_H

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <cstdint>

#include "common/perf_histogram.h"
#include "include/utime.h"
#include "common/Mutex.h"
#include "common/ceph_context.h"
#include "common/ceph_time.h"

class CephContext;
class PerfCountersBuilder;

enum perfcounter_type_d : uint8_t
{
  PERFCOUNTER_NONE = 0,
  PERFCOUNTER_TIME = 0x1,       // float (measuring seconds)
  PERFCOUNTER_U64 = 0x2,        // integer (note: either TIME or U64 *must* be set)
  PERFCOUNTER_LONGRUNAVG = 0x4, // paired counter + sum (time)
  PERFCOUNTER_COUNTER = 0x8,    // counter (vs gauge)
  PERFCOUNTER_HISTOGRAM = 0x10, // histogram (vector) of values

  PERFCOUNTER_SETABLE = 0x20,
};

enum unit_t : uint8_t
{
  UNIT_BYTES,
  UNIT_NONE
};

struct perf_counter_meta_t {
  enum perfcounter_type_d type { PERFCOUNTER_NONE };
  const char* name { nullptr };
  const char* description { nullptr };
  const char* nick { nullptr };
  uint8_t prio { 0 };
  enum unit_t unit { UNIT_NONE };

  bool operator==(const perf_counter_meta_t& rhs) const {
    if (type != rhs.type)
      return false;
    if (name != rhs.name)
      return false;
    if (description != rhs.description)
      return false;
    if (nick != rhs.nick)
      return false;
    if (prio != rhs.prio)
      return false;
    if (unit != rhs.unit)
      return false;
    return true;
  }
};

template <class T>
class configurable_advance_iterator {
public:
  using value_type = T;
  using reference = std::add_lvalue_reference_t<T>;
  using pointer = std::add_pointer_t<T>;
  using difference_type = std::ptrdiff_t;
  using iterator_category = std::forward_iterator_tag;

  reference operator*() {
    return *reinterpret_cast<pointer>(curr);
  }
  pointer operator->() {
    return reinterpret_cast<pointer>(curr);
  }

  configurable_advance_iterator& operator++() {
    curr += advance;
    return *this;
  }
  configurable_advance_iterator operator++(int) {
    const auto temp(*this);
    ++*this;
    return temp;
  }

  bool operator==(const configurable_advance_iterator& rhs) const {
    return curr == rhs.curr;
  }
  bool operator!=(const configurable_advance_iterator& rhs) const {
    return !(*this==rhs);
  }

  template <class U>
  configurable_advance_iterator(U const *p)
    : curr(reinterpret_cast<std::uintptr_t>(p)),
      advance(sizeof(U)) {
    static_assert(std::is_base_of_v<T, U>);
  }
private:
  std::uintptr_t curr;
  const std::size_t advance;
};

class PerfCountersCollectionable {
public:
  typedef configurable_advance_iterator<perf_counter_meta_t> iterator;

  virtual ~PerfCountersCollectionable() = default;

  virtual const iterator begin() const = 0;
  virtual const iterator end() const = 0;
  virtual std::uint64_t get_u64(const perf_counter_meta_t& meta) const = 0;
  virtual std::uint64_t get_avgcount(const perf_counter_meta_t& meta) const = 0;
  virtual std::uint64_t get_avgcount2(const perf_counter_meta_t& meta) const = 0;
  virtual std::pair<std::uint64_t,std::uint64_t>
  read_avg(const perf_counter_meta_t& meta) const = 0;
  virtual PerfHistogram<>*
  get_histogram(const perf_counter_meta_t& meta) const = 0;

  virtual void dump_formatted(
    ceph::Formatter* f,
    bool schema,
    const std::string &counter = "") = 0;
  virtual void dump_formatted_histograms(
    ceph::Formatter* f,
    bool schema,
    const std::string &counter = "") = 0;
  virtual void dump_formatted_generic(
    ceph::Formatter* f,
    bool schema,
    bool histograms,
    const std::string &counter = "") = 0;

  virtual void reset() = 0;

  virtual const std::string& get_name() const = 0;
  virtual void set_name(std::string s) = 0;

  /// adjust priority values by some value
  virtual void set_prio_adjust(int p) = 0;

  virtual int get_adjusted_priority(int p) const = 0;
};

/* Class for constructing a PerfCounters object.
 *
 * This class performs some validation that the parameters we have supplied are
 * correct in create_perf_counters().
 *
 * In the future, we will probably get rid of the first/last arguments, since
 * PerfCountersBuilder can deduce them itself.
 */
class PerfCountersBuilder
{
public:
  PerfCountersBuilder(CephContext *cct, const std::string &name,
		    int first, int last);
  ~PerfCountersBuilder();

  // prio values: higher is better, and higher values get included in
  // 'ceph daemonperf' (and similar) results.
  // Use of priorities enables us to add large numbers of counters
  // internally without necessarily overwhelming consumers.
  enum {
    PRIO_CRITICAL = 10,
    // 'interesting' is the default threshold for `daemonperf` output
    PRIO_INTERESTING = 8,
    // `useful` is the default threshold for transmission to ceph-mgr
    // and inclusion in prometheus/influxdb plugin output
    PRIO_USEFUL = 5,
    PRIO_UNINTERESTING = 2,
    PRIO_DEBUGONLY = 0,
  };
  void add_u64(int key, const char *name,
	       const char *description=NULL, const char *nick = NULL,
	       int prio=0, int unit=UNIT_NONE);
  void add_u64_counter(int key, const char *name,
		       const char *description=NULL,
		       const char *nick = NULL,
		       int prio=0, int unit=UNIT_NONE);
  void add_u64_avg(int key, const char *name,
		   const char *description=NULL,
		   const char *nick = NULL,
		   int prio=0, int unit=UNIT_NONE);
  void add_time(int key, const char *name,
		const char *description=NULL,
		const char *nick = NULL,
		int prio=0);
  void add_time_avg(int key, const char *name,
		    const char *description=NULL,
		    const char *nick = NULL,
		    int prio=0);
  void add_u64_counter_histogram(
    int key, const char* name,
    PerfHistogramCommon::axis_config_d x_axis_config,
    PerfHistogramCommon::axis_config_d y_axis_config,
    const char *description=NULL,
    const char* nick = NULL,
    int prio=0, int unit=UNIT_NONE);

  void set_prio_default(int prio_)
  {
    prio_default = prio_;
  }

  PerfCounters* create_perf_counters();
private:
  PerfCountersBuilder(const PerfCountersBuilder &rhs);
  PerfCountersBuilder& operator=(const PerfCountersBuilder &rhs);
  void add_impl(int idx, const char *name,
                const char *description, const char *nick, int prio, int ty, int unit=UNIT_NONE,
                unique_ptr<PerfHistogram<>> histogram = nullptr);

  PerfCounters *m_perf_counters;

  int prio_default = 0;
};

void ceph_perf_counters_dump_formatted_generic(
  PerfCountersCollectionable& pc,
  Formatter *f,
  bool schema,
  bool histograms,
  const std::string &counter);

/*
 * A PerfCounters object is usually associated with a single subsystem.
 * It contains counters which we modify to track performance and throughput
 * over time. 
 *
 * PerfCounters can track several different types of values:
 * 1) integer values & counters
 * 2) floating-point values & counters
 * 3) floating-point averages
 * 4) 2D histograms of quantized value pairs
 *
 * The difference between values, counters and histograms is in how they are initialized
 * and accessed. For a counter, use the inc(counter, amount) function (note
 * that amount defaults to 1 if you don't set it). For a value, use the
 * set(index, value) function. For histogram use the hinc(value1, value2) function.
 * (For time, use the tinc and tset variants.)
 *
 * If for some reason you would like to reset your counters, you can do so using
 * the set functions even if they are counters, and you can also
 * increment your values if for some reason you wish to.
 *
 * For the time average, it returns the current value and
 * the "avgcount" member when read off. avgcount is incremented when you call
 * tinc. Calling tset on an average is an error and will assert out.
 */
class PerfCounters : public PerfCountersCollectionable
{
  auto _meta2iter(const perf_counter_meta_t& meta) const {
    const auto iter = std::find_if(std::begin(m_data), std::end(m_data),
      [&meta](const auto& v) {
	return v == meta;
      });
    assert(iter != std::end(m_data));
    return iter;
  }

public:
  /** Represents a PerfCounters data element. */
  struct perf_counter_data_any_d : ::perf_counter_meta_t {
    perf_counter_data_any_d() = default;
    perf_counter_data_any_d(const perf_counter_data_any_d& o)
      : perf_counter_meta_t { o.type, o.name, o.description, o.nick, o.prio, o.unit },
	u64(o.u64.load())
    {
      const std::pair<std::uint64_t, std::uint64_t> a = o.read_avg();
      u64 = a.first;
      avgcount = a.second;
      avgcount2 = a.second;
      if (o.histogram) {
        histogram.reset(new PerfHistogram<>(*o.histogram));
      }
    }

    std::atomic<uint64_t> u64 = { 0 };
    std::atomic<uint64_t> avgcount = { 0 };
    std::atomic<uint64_t> avgcount2 = { 0 };
    std::unique_ptr<PerfHistogram<>> histogram;

    void reset()
    {
      if (type != PERFCOUNTER_U64) {
	    u64 = 0;
	    avgcount = 0;
	    avgcount2 = 0;
      }
      if (histogram) {
        histogram->reset();
      }
    }

    // read <sum, count> safely by making sure the post- and pre-count
    // are identical; in other words the whole loop needs to be run
    // without any intervening calls to inc, set, or tinc.
    pair<uint64_t,std::uint64_t> read_avg() const {
      uint64_t sum, count;
      do {
	count = avgcount;
	sum = u64;
      } while (avgcount2 != count);
      return make_pair(sum, count);
    }
  };

  template <typename T>
  struct avg_tracker {
    pair<uint64_t, T> last;
    pair<uint64_t, T> cur;
    avg_tracker() : last(0, 0), cur(0, 0) {}
    T current_avg() const {
      if (cur.first == last.first)
        return 0;
      return (cur.second - last.second) / (cur.first - last.first);
    }
    void consume_next(const pair<uint64_t, T> &next) {
      last = cur;
      cur = next;
    }
  };

  ~PerfCounters();

  void inc(int idx, uint64_t v = 1);
  void dec(int idx, uint64_t v = 1);
  void set(int idx, uint64_t v);
  uint64_t get(int idx) const;

  void tset(int idx, utime_t v);
  void tinc(int idx, utime_t v);
  void tinc(int idx, ceph::timespan v);
  utime_t tget(int idx) const;

  void hinc(int idx, int64_t x, int64_t y);

  const iterator begin() const override final {
    return { &m_data.front() };
  }
  const iterator end() const override final {
    return { &m_data.back() + 1 };
  }

  std::uint64_t get_u64(const perf_counter_meta_t& meta) const override final {
    return _meta2iter(meta)->u64;
  }
  std::uint64_t get_avgcount(const perf_counter_meta_t& meta) const override final {
    return _meta2iter(meta)->avgcount;
  }
  std::uint64_t get_avgcount2(const perf_counter_meta_t& meta) const override final {
    return _meta2iter(meta)->avgcount2;
  }
  // TODO: refactor of getters is really needed
  std::pair<std::uint64_t,std::uint64_t> read_avg(
    const perf_counter_meta_t& meta) const override final {
    return _meta2iter(meta)->read_avg();
  }
  PerfHistogram<>*
  get_histogram(const perf_counter_meta_t& meta) const override final {
    return _meta2iter(meta)->histogram.get();
  }

  void reset() override;
  void dump_formatted(ceph::Formatter *f, bool schema,
                      const std::string &counter = "") override {
    dump_formatted_generic(f, schema, false, counter);
  }
  void dump_formatted_histograms(ceph::Formatter *f, bool schema,
                                 const std::string &counter = "") override {
    dump_formatted_generic(f, schema, true, counter);
  }
  pair<uint64_t, uint64_t> get_tavg_ns(int idx) const;

  const std::string& get_name() const override;
  void set_name(std::string s) override {
    m_name = s;
  }

  /// adjust priority values by some value
  void set_prio_adjust(int p) override {
    prio_adjust = p;
  }

  int get_adjusted_priority(int p) const override {
    return std::max(std::min(p + prio_adjust,
                             (int)PerfCountersBuilder::PRIO_CRITICAL),
                    0);
  }

private:
  PerfCounters(CephContext *cct, const std::string &name,
	     int lower_bound, int upper_bound);
  PerfCounters(const PerfCounters &rhs);
  PerfCounters& operator=(const PerfCounters &rhs);
  void dump_formatted_generic(ceph::Formatter *f, bool schema, bool histograms,
                              const std::string &counter = "") override {
    ceph_perf_counters_dump_formatted_generic(*this, f, schema,
					      histograms, counter);
  }

  typedef std::vector<perf_counter_data_any_d> perf_counter_data_vec_t;

  CephContext *m_cct;
  int m_lower_bound;
  int m_upper_bound;
  std::string m_name;
  const std::string m_lock_name;

  int prio_adjust = 0;

  /** Protects m_data */
  mutable Mutex m_lock;

  perf_counter_data_vec_t m_data;

  friend class PerfCountersBuilder;
  friend class PerfCountersCollection;
};

class SortPerfCountersByName {
public:
  bool operator()(
    const PerfCountersCollectionable* const lhs,
    const PerfCountersCollectionable* const rhs) const
  {
    return (lhs->get_name() < rhs->get_name());
  }
};

using perf_counters_set_t = \
  std::set<PerfCountersCollectionable*, SortPerfCountersByName>;

/*
 * PerfCountersCollection manages PerfCounters objects for a Ceph process.
 */
class PerfCountersCollection
{
public:
  PerfCountersCollection(CephContext *cct);
  ~PerfCountersCollection();
  void add(class PerfCountersCollectionable *l);
  void remove(class PerfCountersCollectionable *l);
  void clear();
  bool reset(const std::string &name);

  void dump_formatted(ceph::Formatter *f, bool schema,
                      const std::string &logger = "",
                      const std::string &counter = "") {
    dump_formatted_generic(f, schema, false, logger, counter);
  }

  void dump_formatted_histograms(ceph::Formatter *f, bool schema,
                                 const std::string &logger = "",
                                 const std::string &counter = "") {
    dump_formatted_generic(f, schema, true, logger, counter);
  }

  // A reference to a perf_counter_data_any_d, with an accompanying
  // pointer to the enclosing PerfCounters, in order that the consumer
  // can see the prio_adjust
  class PerfCounterRef
  {
    public:
    ::perf_counter_meta_t* data;
    PerfCountersCollectionable* perf_counters;
  };
  typedef std::map<std::string,
          PerfCounterRef> CounterMap;

  void with_counters(std::function<void(const CounterMap &)>) const;

private:
  void dump_formatted_generic(ceph::Formatter *f, bool schema, bool histograms,
                              const std::string &logger = "",
                              const std::string &counter = "");

  CephContext *m_cct;

  /** Protects m_loggers */
  mutable Mutex m_lock;

  perf_counters_set_t m_loggers;

  CounterMap by_path; 

  friend class PerfCountersCollectionTest;
};


class PerfGuard {
  const ceph::real_clock::time_point start;
  PerfCounters* const counters;
  const int event;

public:
  PerfGuard(PerfCounters* const counters,
            const int event)
  : start(ceph::real_clock::now()),
    counters(counters),
    event(event) {
  }

  ~PerfGuard() {
    counters->tinc(event, ceph::real_clock::now() - start);
  }
};


class PerfCountersDeleter {
  CephContext* cct;

public:
  PerfCountersDeleter() noexcept : cct(nullptr) {}
  PerfCountersDeleter(CephContext* cct) noexcept : cct(cct) {}
  void operator()(PerfCountersCollectionable* const p) noexcept {
    if (cct)
      cct->get_perfcounters_collection()->remove(p);
    delete p;
  }
};

using PerfCountersRef = std::unique_ptr<PerfCounters, PerfCountersDeleter>;

namespace ceph {

using perf_counter_meta_t = ::perf_counter_meta_t;

// for passing extra arguments for pert counter's construction phase that aren't
// needed later. In such case making them part of the meta_t structure would be
// only waste of memory.
// Typical client of this bit is histogram-typed counters. It's being used to
// describe i.e. axis.
template <const perf_counter_meta_t& pcid> struct perf_counter_traits_t {};

static constexpr auto PERFCOUNTER_U64_CTR = \
  static_cast<enum perfcounter_type_d>(PERFCOUNTER_U64 | PERFCOUNTER_COUNTER);
static constexpr auto PERFCOUNTER_U64_SETABLE = \
  static_cast<enum perfcounter_type_d>(PERFCOUNTER_U64 | PERFCOUNTER_SETABLE);
static constexpr auto PERFCOUNTER_TIME_AVG = \
  static_cast<enum perfcounter_type_d>(
    PERFCOUNTER_TIME | PERFCOUNTER_LONGRUNAVG);
static constexpr auto PERFCOUNTER_U64_HIST = \
  static_cast<enum perfcounter_type_d>(
    PERFCOUNTER_U64 | PERFCOUNTER_HISTOGRAM | PERFCOUNTER_COUNTER);

// add_u64_counter
#define PERF_COUNTERS_ADD_U64_COUNTER(id, name, desc, ...)	\
  static constexpr perf_counter_meta_t id {			\
    PERFCOUNTER_U64_CTR, name, desc, __VA_ARGS__		\
  }

// add_time_avg
#define PERF_COUNTERS_ADD_TIME_AVG(id, name, desc, ...)		\
  static constexpr perf_counter_meta_t id {			\
   PERFCOUNTER_TIME_AVG, name, desc, __VA_ARGS__		\
  }
#define PERF_COUNTERS_ADD_U64_SETABLE(id, name, desc, ...)	\
  static constexpr perf_counter_meta_t id {			\
    PERFCOUNTER_U64_SETABLE, name, desc, __VA_ARGS__		\
  }

// add_u64_counter_histogram
#define PERF_COUNTERS_ADD_U64_COUNTER_HIST(id, name, xcfg, ycfg, desc, ...) \
  static constexpr perf_counter_meta_t id {			\
    PERFCOUNTER_U64_HIST, name, desc, __VA_ARGS__		\
  };								\
  namespace ceph {						\
  template <> struct perf_counter_traits_t<id> {		\
    static constexpr PerfHistogramCommon::axis_config_d x_axis_params {xcfg};	\
    static constexpr PerfHistogramCommon::axis_config_d y_axis_params {ycfg};	\
  };								\
  }


static constexpr std::size_t CACHE_LINE_SIZE_ { 64 };
static constexpr std::size_t EXPECTED_THREAD_NUM { 32 };

#define DEBUG_NOINLINE __attribute__((noinline))

template <const perf_counter_meta_t&... P>
class perf_counters_t : public PerfCountersCollectionable {
  union perf_counter_any_data_t {
    struct val_with_counter_t {
      std::uint32_t val;
      std::uint32_t cnt;
    };

    // for plain u64 counter
    std::uint64_t val;

    // for longrunavg. Typically there will be no locked operation nor memory
    // barrier on the main path
    std::atomic<val_with_counter_t> val_with_counter;
  };

  union perf_counter_atomic_any_data_t {
    std::atomic_uint64_t val;
    struct {
      std::atomic_uint64_t val;
      std::atomic_uint64_t cnt;
    } val_with_counter;
    PerfHistogram<>* histogram;
  };

  struct alignas(CACHE_LINE_SIZE_) thread_group_t
    : std::array<perf_counter_any_data_t, sizeof...(P)> {
  };

  struct alignas(CACHE_LINE_SIZE_) atomic_group_t
    : std::array<perf_counter_atomic_any_data_t, sizeof...(P)> {
  };

  std::string name;
  int prio_adjust = 0;

  std::array<thread_group_t, EXPECTED_THREAD_NUM> threaded_perf_counters;
  atomic_group_t atomic_perf_counters;

  perf_counter_any_data_t* _get_threaded_counters(const std::size_t idx) {
    static std::atomic_size_t last_allocated_selector{ 0 };
    static constexpr std::size_t FIRST_SEEN_THREAD{
      std::tuple_size<decltype(threaded_perf_counters)>::value
    };
    thread_local std::size_t thread_selector{ FIRST_SEEN_THREAD };

    if (likely(thread_selector < threaded_perf_counters.size())) {
      return &threaded_perf_counters[thread_selector][idx];
    } else if (likely(thread_selector == last_allocated_selector)) {
      // Well, it looks we're ran out of per-thread slots.
      return nullptr;
    } else {
      // The rare case
      thread_selector = last_allocated_selector.fetch_add(1);
      assert(thread_selector < threaded_perf_counters.size());
      return &threaded_perf_counters[thread_selector][idx];
    }
  }

  // Args-pack helpers. Many of them are here only because the constexpr
  // -capable variant of <algorithm> isn't available in C++17. Sorry.
  template<const perf_counter_meta_t& to_count,
	   const perf_counter_meta_t& H,
	   const perf_counter_meta_t&... T>
  static constexpr std::size_t count() {
    constexpr std::size_t curval = &to_count == &H ? 1 : 0;
    if constexpr (sizeof...(T)) {
      return curval + count<to_count, T...>();
    } else {
      return curval;
    }
  }

  template<const perf_counter_meta_t& to_find,
	   const perf_counter_meta_t& H,
	   const perf_counter_meta_t&... T>
  static constexpr std::size_t index_of() {
    if constexpr (&to_find == &H) {
      return sizeof...(P) - sizeof...(T) - 1 /* for H */;
    } else {
      return index_of<to_find, T...>();
    }
  }

  template<const perf_counter_meta_t& H,
	   const perf_counter_meta_t&... T>
  constexpr void init_histograms() {
    if constexpr (H.type == PERFCOUNTER_U64_HIST) {
      constexpr std::size_t idx = perf_counters_t::index_of<H, P...>();
      atomic_perf_counters[idx].histogram = \
	new PerfHistogram<>({ perf_counter_traits_t<H>::x_axis_params,
			    perf_counter_traits_t<H>::y_axis_params });
    }

    if constexpr (sizeof...(T)) {
      return init_histograms<T...>();
    }
  }

  template<const perf_counter_meta_t& pcid>
  void _inc(const std::size_t amount = 1) {
    static_assert(perf_counters_t::count<pcid, P...>() == 1);
    static_assert(0 == (pcid.type & PERFCOUNTER_SETABLE));

    constexpr std::size_t idx = perf_counters_t::index_of<pcid, P...>();
    perf_counter_any_data_t* const threaded_counters = \
      _get_threaded_counters(idx);

    if (likely(threaded_counters != nullptr)) {
      if constexpr (pcid.type & PERFCOUNTER_LONGRUNAVG) {
        // we are operating on the memory atomically with very weak ordering
	// requirements. As the struct consists just two 32 bit unsigned
	// integers, on most platforms this will be a plain load.
	auto val_n_cnt = \
	  threaded_counters->val_with_counter.load(std::memory_order_relaxed);

	const bool val_overflowed = \
	  __builtin_uadd_overflow(val_n_cnt.val, amount, &val_n_cnt.val);
	if (unlikely(val_overflowed)) {
	  // amount is always greater-or-equal 1, so val moves at greater-or-eq
	  // pace than cnt. This is pretty handy as we can asumme if val doesn't
	  // overflow, then cnt is overflow-free as well.
	  atomic_perf_counters[idx].val_with_counter.val += \
	    val_n_cnt.val + std::numeric_limits<decltype(val_n_cnt.val)>::max();
	  atomic_perf_counters[idx].val_with_counter.cnt += \
	    static_cast<std::uint64_t>(val_n_cnt.cnt) + 1;
	  threaded_counters->val_with_counter.store({ 0, 0 },
						    std::memory_order_relaxed);
	} else {
	  // there is no compare-and-exchange. This slot belongs to the current
	  // thread and we don't expect any other writer, so employing costly
	  // synchronization would be pointless. On x86 this will be translated
	  // into plain store without MFENCE.
	  ++val_n_cnt.cnt;
	  threaded_counters->val_with_counter.store(val_n_cnt,
						    std::memory_order_relaxed);
	}
      } else {
	threaded_counters->val += amount;
      }
    } else {
      if constexpr (pcid.type & PERFCOUNTER_LONGRUNAVG) {
	atomic_perf_counters[idx].val_with_counter.val += amount;
	atomic_perf_counters[idx].val_with_counter.cnt += 1;
      } else {
	atomic_perf_counters[idx].val += amount;
      }
    }
  }

public:
  perf_counters_t(std::string name)
    : name(std::move(name))
  {
    // iterate over all thread slots
    for (auto& perf_counters : threaded_perf_counters) {
      memset(&perf_counters, 0, sizeof(perf_counters));
    }
    memset(&atomic_perf_counters, 0, sizeof(atomic_perf_counters));
    init_histograms<P...>();
  }

  ~perf_counters_t()
  {
    for (std::size_t idx = 0; idx < std::size(m_meta); idx++) {
      if (m_meta[idx].type == PERFCOUNTER_U64_HIST) {
	delete atomic_perf_counters[idx].histogram;
      }
    }
  }

  template<const perf_counter_meta_t& pcid>
  void DEBUG_NOINLINE inc(const std::size_t count = 1) {
    static_assert(pcid.type & PERFCOUNTER_U64);
    return _inc<pcid>(count);
  }

  template<const perf_counter_meta_t& pcid>
  void DEBUG_NOINLINE dec(const std::size_t amount = 1) {
    static_assert(pcid.type & PERFCOUNTER_U64);
    static_assert(0 == (pcid.type & PERFCOUNTER_SETABLE));
    // don't touching the logic of the original PerfCounters
    static_assert(0 == (pcid.type & PERFCOUNTER_LONGRUNAVG));
    static_assert(perf_counters_t::count<pcid, P...>() == 1);

    constexpr std::size_t idx = perf_counters_t::index_of<pcid, P...>();
    perf_counter_any_data_t* const threaded_counters = \
      _get_threaded_counters(idx);

    if (likely(threaded_counters != nullptr)) {
      threaded_counters->val -= amount;
    } else {
      atomic_perf_counters[idx].val -= amount;
    }
  }

  template<const perf_counter_meta_t& pcid>
  void DEBUG_NOINLINE set(const std::uint64_t amount) {
    static_assert(perf_counters_t::count<pcid, P...>() == 1);
    static_assert(pcid.type & PERFCOUNTER_SETABLE);

    constexpr std::size_t idx = perf_counters_t::index_of<pcid, P...>();
    // Well, all callers touch the same cache-line, so ping-pong must be
    // expected. Thankfully to the relaxed memory ordering we don't emit
    // MFENCE on x86, so CPU will get a chance to hide the latency. This
    // doesn't mean it won't pop up -- any locked instruction can expose
    // it back. I'm particularly afraid about calling ::set() under lock
    // by Throttle as the futex manipulation is quite close.

    // Another issue is that ::set() doesn't play with inc() nor dec() at
    // all. This could be fight with extra complexity (e.g. invalidation
    // bits) but the simplest (dumb!) solution has been chosen as we can
    // 1) replace the call with the value-taking ::reset() on cold paths
    // while 2) moving to set-only counters on hot paths with validation
    // at compile-time.
    atomic_perf_counters[idx].val.store(amount, std::memory_order_relaxed);
  }

  template<const perf_counter_meta_t& pcid>
  void set_slow(const std::uint64_t amount) {
    static_assert(perf_counters_t::count<pcid, P...>() == 1);
    static_assert(pcid.type & PERFCOUNTER_U64);

    constexpr std::size_t idx = perf_counters_t::index_of<pcid, P...>();
    atomic_perf_counters[idx].val_with_counter.val -= \
      get_u64(pcid) - amount;
    atomic_perf_counters[idx].val_with_counter.cnt -= \
      get_avgcount(pcid) - amount;
  }

  template<const perf_counter_meta_t& pcid>
  std::size_t DEBUG_NOINLINE get() const {
    static_assert(perf_counters_t::count<pcid, P...>() == 1);
    static_assert(pcid.type & PERFCOUNTER_U64);

    constexpr std::size_t idx{ index_of<pcid, P...>() };
    std::size_t aggregate{ atomic_perf_counters[idx].val };
    for (const auto& threaded_counters : threaded_perf_counters) {
      aggregate += threaded_counters[idx].val;
    }
    return aggregate;
  }

  template<const perf_counter_meta_t& pcid>
  void DEBUG_NOINLINE tinc(const utime_t amt) {
    static_assert(pcid.type & PERFCOUNTER_TIME);
    return _inc<pcid>(amt.to_nsec());
  }

  template<const perf_counter_meta_t& pcid>
  void DEBUG_NOINLINE tinc(const ceph::timespan amt) {
    static_assert(pcid.type & PERFCOUNTER_TIME);
    return _inc<pcid>(amt.count());
  }

  template<const perf_counter_meta_t& pcid>
  void DEBUG_NOINLINE hinc(const std::int64_t x, const std::int64_t y) {
    static_assert(pcid.type == PERFCOUNTER_U64_HIST);
    constexpr std::size_t idx = perf_counters_t::index_of<pcid, P...>();
    assert(atomic_perf_counters[idx].histogram);
    atomic_perf_counters[idx].histogram->inc(x, y);
  }

  // helper structures, NOT for hot paths
  static constexpr std::array<perf_counter_meta_t,
			      sizeof...(P)> m_meta {
    P...
  };

  template<const perf_counter_meta_t& pcid>
  std::pair<std::uint64_t, std::uint64_t> get_tavg_ns() const {
    static_assert(pcid.type & PERFCOUNTER_TIME);
    static_assert(pcid.type & PERFCOUNTER_LONGRUNAVG);

    std::pair<std::uint64_t, std::uint64_t> a = read_avg(pcid);
    return make_pair(a.second, a.first);
  }

  // virtuals begins here
  const iterator begin() const override final {
    return { &m_meta.front() };
  }
  const iterator end() const override final {
    return { &m_meta.back() + 1 };
  }

  std::uint64_t get_u64(const perf_counter_meta_t& meta) const override final {
    const auto iter = std::find_if(std::begin(m_meta), std::end(m_meta),
      [&meta](const auto& v) {
	return v == meta;
      });
    assert(iter != std::end(m_meta));

    const std::size_t idx = std::distance(std::begin(m_meta), iter);
    std::size_t aggregate = atomic_perf_counters[idx].val;
    for (const auto& threaded_counters : threaded_perf_counters) {
      aggregate += threaded_counters[idx].val;
    }
    return aggregate;
  }
  std::uint64_t get_avgcount(const perf_counter_meta_t& meta) const override final {
    const auto iter = std::find_if(std::begin(m_meta), std::end(m_meta),
      [&meta](const auto& v) {
	return v == meta;
      });
    assert(iter != std::end(m_meta));

    const std::size_t idx = std::distance(std::begin(m_meta), iter);
    std::size_t aggregate = atomic_perf_counters[idx].val_with_counter.cnt;
    for (const auto& threaded_counters : threaded_perf_counters) {
      aggregate += threaded_counters[idx].val_with_counter.load().cnt;
    }
    return aggregate;
  }
  std::uint64_t get_avgcount2(const perf_counter_meta_t& meta) const override final {
    return get_avgcount(meta);
  }
  std::pair<std::uint64_t,std::uint64_t> read_avg(
    const perf_counter_meta_t& meta) const override final {
    return std::make_pair(get_u64(meta), get_avgcount(meta));
  }
  PerfHistogram<>*
  get_histogram(const perf_counter_meta_t& meta) const override final {
    const auto iter = std::find_if(std::begin(m_meta), std::end(m_meta),
      [&meta](const auto& v) {
	return v == meta;
      });
    assert(iter != std::end(m_meta));

    const std::size_t idx = std::distance(std::begin(m_meta), iter);
    return atomic_perf_counters[idx].histogram;
  }

#if 0
  value_t get(const perf_counter_meta_t& id) const override;
  value_t get(const std::string& name) const override;
#endif

  void reset() override {
    assert(std::size(m_meta) == std::size(atomic_perf_counters));
    assert(std::size(m_meta) == std::size(threaded_perf_counters[0]));

    for (std::size_t idx = 0; idx < std::size(m_meta); idx++) {
      const perf_counter_meta_t& meta = m_meta[idx];

      if (meta.type != PERFCOUNTER_U64) {
	atomic_perf_counters[idx].val_with_counter.val -= get_u64(meta);
	atomic_perf_counters[idx].val_with_counter.cnt -= get_avgcount(meta);
      } else if (meta.type & PERFCOUNTER_HISTOGRAM) {
	atomic_perf_counters[idx].histogram->reset();
      }
    }
  }

  const std::string& get_name() const override {
    return name;
  }
  void set_name(std::string s) override {
    name = std::move(s);
  }

  /// adjust priority values by some value
  void set_prio_adjust(int p) override {
    prio_adjust = p;
  }

  int get_adjusted_priority(int p) const override {
    return std::max(0,
      std::min(p + prio_adjust, (int)PerfCountersBuilder::PRIO_CRITICAL));
  }

  void dump_formatted(ceph::Formatter *f, bool schema,
                      const std::string &counter = "") override {
    dump_formatted_generic(f, schema, false, counter);
  }
  void dump_formatted_histograms(ceph::Formatter *f, bool schema,
                                 const std::string &counter = "") override {
    dump_formatted_generic(f, schema, true, counter);
  }

private:
  void dump_formatted_generic(ceph::Formatter *f, bool schema, bool histograms,
                              const std::string &counter = "") override {
    ceph_perf_counters_dump_formatted_generic(*this, f, schema,
					      histograms, counter);
  }
};

} // namespace ceph
#endif
