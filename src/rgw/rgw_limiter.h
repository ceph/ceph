// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <common/ceph_mutex.h>
#include <common/ceph_time.h>
#include <common/dout.h>
#include <common/dout_fmt.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/io_context.hpp>
#include <cmath>
#include <limits>
#include <mutex>

#include "common/async/completion.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/perf_counters.h"
#include "common/perf_counters_collection.h"

namespace rgw::limiter {
namespace async = ceph::async;

class StaticLimiter;

class ConcurrencyLimiter {
 public:
  struct Sample {
    std::chrono::nanoseconds rtt{0};
    int64_t inflight = 0;
    bool dropped = false;
  };

  virtual ~ConcurrencyLimiter() = default;

  // current ceiling of cuncurrent in-flight requests
  virtual int64_t limit() const = 0;

  // per completion feedback: adaptive filters use this to update
  // their internal estimates
  virtual void sample(const Sample&) = 0;
};

class StaticLimiter : public ConcurrencyLimiter, public md_config_obs_t {
 private:
  CephContext* cct;
  std::atomic<int64_t> max_requests;

 public:
   explicit StaticLimiter(CephContext* cct) :
     cct(cct),
     max_requests(cct->_conf.get_val<int64_t>("rgw_max_concurrent_requests"))
   {
     if (max_requests <= 0) {
       max_requests = std::numeric_limits<int64_t>::max();
     }
     cct->_conf.add_observer(this);
   }
  ~StaticLimiter() override = default;

  int64_t limit() const override { return max_requests.load(); }

  void sample(const Sample&) override {};

  std::vector<std::string> get_tracked_keys() const noexcept override {
    return {std::string{"rgw_max_concurrent_requests"}};
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override
  {
    if (changed.contains("rgw_max_concurrent_requests")) {
      auto new_max = conf.get_val<int64_t>("rgw_max_concurrent_requests");
      max_requests = new_max > 0 ? new_max : std::numeric_limits<int64_t>::max();
    }
  }
};

/// A port of Netflix's Gradient2 Concurrency Limiter
/// https://github.com/Netflix/concurrency-limits
class Gradient2 : public ConcurrencyLimiter, public md_config_obs_t {
 protected:
  enum class Metric {
    metrics_start = 94000,
    long_rtt,
    last_rtt,
    gradient,
    limit,
    effective_limit,
    min,
    max,
    samples_total,
    app_limited_total,
    drops_total,
    rtt,
    min_rtt,
    metrics_stop,
  };

  class ExpAvg {
 private:
    double _value = 0.0;
    double _sum = 0;
    const int _window;
    const int _warmup_window;
    int _count = 0;

   public:
    ExpAvg(int window, int warmup_window)
        : _window(window), _warmup_window(warmup_window) {}
    double add(double sample) {
      if (_count < _warmup_window) {
        _count++;
        _sum += sample;
        _value = _sum / _count;  // XXX handle sum overflow and things
      } else {
        const double factor = 2.00 / (_window + 1);
        _value = _value * (1 - factor) + sample * factor;
      }
      return _value;
    }
    void decay(double factor) {
      _value = _value * factor;
    }
    double get() const { return _value; }
  };

  // Alternate way to ExpAvg to track long term rtt
  class WindowedMin {
   private:
    double _current = std::numeric_limits<double>::max();
    double _published = 0.0;
    const int _window;
    int _count = 0;

   public:
    explicit WindowedMin(int window) : _window(window) {}
    void add(double sample) {
      _current = std::min(_current, sample);
      if (++_count >= _window) {
        _published = _current;
        _current = std::numeric_limits<double>::max();
        _count = 0;
      }
    }
    double get() const { return _published; }
  };

 private:
  CephContext* _cct;
  PerfCountersRef _perf;
  const DoutPrefix _dp;
  mutable ceph::mutex _mutex = ceph::make_mutex("Gradient2::lock");

  // config
  std::atomic<int64_t> _min_requests;
  std::atomic<int64_t> _max_requests;
  std::atomic<double> _smoothing;
  std::atomic<double> _tolerance;
  // other implementations call this queue size, but since we have no
  // standing queue here this becomes the amount of concurrency steps
  // we are willing to probe for
  std::atomic<int64_t> _probe_step;

  // updated on sample()
  std::atomic<double> _estimated_requests;
  ExpAvg _long_rtt_sec;
  WindowedMin _min_rtt_sec;

 public:
  explicit Gradient2(CephContext* cct)
      : _cct(cct),
        _perf(initialize_perf_counters(cct, "rgw-limiter")),
        _dp(cct, ceph_subsys_rgw, "limiter/gradient2: "),
        _min_requests(
            cct->_conf.get_val<int64_t>("rgw_min_concurrent_requests")),
        _max_requests(
            cct->_conf.get_val<int64_t>("rgw_max_concurrent_requests")),
        _smoothing(cct->_conf.get_val<double>("rgw_gradient2_smoothing")),
        _tolerance(cct->_conf.get_val<double>("rgw_gradient2_rtt_tolerance")),
        _probe_step(
            cct->_conf.get_val<int64_t>("rgw_gradient2_estimate_growth")),
        _estimated_requests(cct->_conf.get_val<int64_t>(
            "rgw_gradient2_initial_concurrent_requests")),
        _long_rtt_sec(ExpAvg(
            cct->_conf.get_val<int64_t>("rgw_gradient2_long_window"), 10)),
        _min_rtt_sec(
            cct->_conf.get_val<int64_t>("rgw_gradient2_long_window")) {
    if (_max_requests <= 0) {
      _max_requests = std::numeric_limits<int64_t>::max();
    }
    if (_min_requests <= 0) {
      _min_requests = 1;
    }
    _estimated_requests = std::clamp(_estimated_requests.load(),
        static_cast<double>(_min_requests.load()),
        static_cast<double>(_max_requests.load()));

    cct->_conf.add_observer(this);
    _perf->set(static_cast<int>(Metric::min), _min_requests);
    _perf->set(static_cast<int>(Metric::max), _max_requests);
    _perf->set(static_cast<int>(Metric::effective_limit), _estimated_requests);
  }
  ~Gradient2() override = default;

  std::vector<std::string> get_tracked_keys() const noexcept override {
    return {
        std::string{"rgw_max_concurrent_requests"},
        std::string{"rgw_min_concurrent_requests"},
        std::string{"rgw_gradient2_smoothing"},
        std::string{"rgw_gradient2_rtt_tolerance"},
        std::string{"rgw_gradient2_estimate_growth"},
    };
  }

  void handle_conf_change(
      const ConfigProxy& conf, const std::set<std::string>& changed) override {
    if (changed.contains("rgw_max_concurrent_requests")) {
      auto new_max = conf.get_val<int64_t>("rgw_max_concurrent_requests");
      _max_requests =
          new_max > 0 ? new_max : std::numeric_limits<int64_t>::max();
      _perf->set(static_cast<int>(Metric::max), _max_requests);
    }
    if (changed.contains("rgw_min_concurrent_requests")) {
      auto new_min = conf.get_val<int64_t>("rgw_min_concurrent_requests");
      _min_requests = new_min > 0 ? new_min : 1;
      _perf->set(static_cast<int>(Metric::min), _min_requests);
    }
    if (changed.contains("rgw_gradient2_smoothing")) {
      _smoothing = conf.get_val<double>("rgw_gradient2_smoothing");
    }
    if (changed.contains("rgw_gradient2_rtt_tolerance")) {
      _tolerance = conf.get_val<double>("rgw_gradient2_rtt_tolerance");
    }
    if (changed.contains("rgw_gradient2_estimate_growth")) {
      _probe_step = conf.get_val<int64_t>("rgw_gradient2_estimate_growth");
    }
  }

  int64_t limit() const override {
    return static_cast<int64_t>(
        _estimated_requests.load(std::memory_order_relaxed));
  }

  void sample(const Sample& sample) override {
    std::lock_guard<ceph::mutex> lock(_mutex);
    _perf->inc(static_cast<int>(Metric::samples_total));
    if (sample.dropped) {  // use this signal somehow?
      _perf->inc(static_cast<int>(Metric::drops_total));
    }

    const double estimated_limit = _estimated_requests.load();
    _perf->tset(static_cast<int>(Metric::last_rtt), sample.rtt);

    // Sum and count of the sampled RTTs. rate(rtt_sum) is seconds of work
    // completed per second, i.e. the concurrency implied by the completion
    // flow -- the lambda*W term of a Little's-law residual against the
    // in-flight gauge. Recorded before the app-limited return below so the
    // sum covers every sampled request, matching what the gauge counts.
    _perf->tinc(static_cast<int>(Metric::rtt), sample.rtt);

    const auto short_rtt_sec =
        std::chrono::duration<double>(sample.rtt).count();
    _min_rtt_sec.add(short_rtt_sec);
    _perf->tset(static_cast<int>(Metric::min_rtt),
        make_timespan(_min_rtt_sec.get()));

    const auto long_rtt_sec_now = _long_rtt_sec.add(short_rtt_sec);

    // If the long RTT is substantially larger than the short RTT then
    // reduce the long RTT measurement. This can happen when latency
    // returns to normal after a prolonged prior of excessive load.
    // Reducing the long RTT without waiting for the exponential
    // smoothing helps bring the system back to steady state.
    if (short_rtt_sec > 0 && long_rtt_sec_now / short_rtt_sec > 2) {
      _long_rtt_sec.decay(0.95);
    }
    // Exported after the decay so it reflects the stored baseline. The
    // gradient below deliberately uses the pre-decay value, as upstream does.
    _perf->tset(static_cast<int>(Metric::long_rtt),
        make_timespan(_long_rtt_sec.get()));

    // Don't grow the limit if we are app limited
    if (sample.inflight < estimated_limit / 2) {
      _perf->inc(static_cast<int>(Metric::app_limited_total));
      return;
    }

    // Rtt could be higher than rtt_noload because of smoothing rtt
    // noload updates so set to 1.0 to indicate no queuing. Otherwise
    // calculate the slope and don't allow it to be reduced by more
    // than half to avoid aggressive load-shedding due to outliers.

    double gradient = 1.0;
    if (short_rtt_sec > 0 && long_rtt_sec_now > 0) {
      const auto g = _tolerance * long_rtt_sec_now / short_rtt_sec;
      gradient = std::clamp(g, 0.5, 1.0);
    }
    double new_limit = (estimated_limit * gradient) + _probe_step.load();
    new_limit = estimated_limit * (1 - _smoothing.load()) + new_limit * _smoothing.load();

    const auto clamped = std::clamp(new_limit,
        static_cast<double>(_min_requests.load()),
        static_cast<double>(_max_requests.load()));
    const auto previous =
        _estimated_requests.exchange(clamped, std::memory_order_relaxed);
    if (clamped != previous) {
      ldpp_dout_fmt(&_dp, 10,
          "new limit: {} -> {} short_rtt:{}s long_rtt:{}s gradient:{} "
          "growth:{} min:{} max:{}",
          previous, clamped, short_rtt_sec, long_rtt_sec_now, gradient,
          _probe_step.load(), _min_requests.load(), _max_requests.load());
    }
    _perf->set(static_cast<int>(Metric::gradient), llround(gradient * 1000));
    _perf->set(static_cast<int>(Metric::limit), new_limit);
    _perf->set(static_cast<int>(Metric::effective_limit), _estimated_requests.load());
  }

  static PerfCountersRef initialize_perf_counters(
      CephContext* cct, const std::string& name) {
    PerfCountersBuilder pcb(cct, name, static_cast<int>(Metric::metrics_start),
        static_cast<int>(Metric::metrics_stop));
    pcb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
    pcb.add_time(static_cast<int>(Metric::last_rtt), "last_rtt", "");
    pcb.add_time(static_cast<int>(Metric::long_rtt), "long_rtt",
        "Exponentially averaged RTT baseline");
    pcb.add_time(static_cast<int>(Metric::min_rtt), "min_rtt",
        "Windowed-minimum RTT: no-load service time");
    pcb.add_time_avg(static_cast<int>(Metric::rtt), "rtt",
        "Sum and count of sampled RTTs; rate(rtt_sum) is the concurrency "
        "implied by the completion flow");
    pcb.add_u64(static_cast<int>(Metric::gradient), "gradient", "");
    pcb.add_u64(static_cast<int>(Metric::limit), "limit", "");
    pcb.add_u64(
        static_cast<int>(Metric::effective_limit), "effective_limit", "");
    pcb.add_u64(static_cast<int>(Metric::min), "min", "");
    pcb.add_u64(static_cast<int>(Metric::max), "max", "");
    pcb.add_u64_counter(
        static_cast<int>(Metric::samples_total), "samples_total", "");
    pcb.add_u64_counter(
        static_cast<int>(Metric::app_limited_total), "app_limited_total", "");
    pcb.add_u64_counter(
        static_cast<int>(Metric::drops_total), "drops_total", "");

    auto logger = PerfCountersRef{pcb.create_perf_counters(), cct};
    cct->get_perfcounters_collection()->add(logger.get());
    return logger;
  }
};

static std::unique_ptr<ConcurrencyLimiter> create_by_name(
    CephContext* cct, std::string_view name) {
  if (name == "static") {
    return std::make_unique<StaticLimiter>(cct);
  } else if (name == "gradient2") {
    return std::make_unique<Gradient2>(cct);
  }
  return nullptr;
}

}  // namespace rgw::limiter
