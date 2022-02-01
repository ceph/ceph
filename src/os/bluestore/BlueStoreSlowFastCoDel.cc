// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "BlueStoreSlowFastCoDel.h"

#include "common/regression_utils.h"

BlueStoreSlowFastCoDel::BlueStoreSlowFastCoDel(
  CephContext *_cct,
  std::function<void(int64_t)> _bluestore_budget_reset_callback,
  std::function<int64_t()> _get_kv_throttle_current) :
  fast_timer(_cct, fast_timer_lock),
  slow_timer(_cct, slow_timer_lock),
  bluestore_budget_reset_callback(_bluestore_budget_reset_callback),
  get_kv_throttle_current(_get_kv_throttle_current) {
  on_config_changed(_cct);
}

BlueStoreSlowFastCoDel::~BlueStoreSlowFastCoDel() {
  {
    std::lock_guard l1{fast_timer_lock};
    fast_timer.cancel_all_events();
    fast_timer.shutdown();
  }

  {
    std::lock_guard l2{slow_timer_lock};
    slow_timer.cancel_all_events();
    slow_timer.shutdown();
  }

  regression_throughput_history.clear();
  regression_target_latency_history.clear();
}

void BlueStoreSlowFastCoDel::update_from_txc_info(
  ceph::mono_clock::time_point txc_start_time,
  uint64_t txc_bytes) {
  std::lock_guard l(register_lock);
  ceph::mono_clock::time_point now = ceph::mono_clock::now();
  int64_t latency = std::chrono::nanoseconds(now - txc_start_time).count();

  if (activated && max_queue_length < get_kv_throttle_current()) {
    max_queue_length = get_kv_throttle_current();
  }
  if (min_latency == INITIAL_LATENCY_VALUE || latency < min_latency) {
    min_latency = latency;
  }
  slow_interval_txc_cnt++;
  slow_interval_registered_bytes += txc_bytes;
}

void BlueStoreSlowFastCoDel::on_min_latency_violation() {
  if (target_latency > 0) {
    double diff = (double) (target_latency - min_latency);
    auto error_ratio = std::abs(diff) / min_latency;
    if (error_ratio > 0.5) {
      error_ratio = 0.5;
    }
    bluestore_budget = std::max(bluestore_budget * (1 - error_ratio),
                                min_bluestore_budget * 1.0);
  }
}

void BlueStoreSlowFastCoDel::on_no_violation() {
  if (bluestore_budget < max_queue_length * 1.5) {
    bluestore_budget = bluestore_budget + bluestore_budget_increment;
  }
}

void BlueStoreSlowFastCoDel::on_config_changed(CephContext *cct) {
  {
    std::lock_guard l(register_lock);

    activated = cct->_conf->bluestore_codel;
    target_slope = cct->_conf->bluestore_codel_throughput_latency_tradeoff;
    slow_interval = ((int64_t) cct->_conf->bluestore_codel_slow_interval) *
            1000 * 1000;
    initial_fast_interval = ((int64_t)
            cct->_conf->bluestore_codel_fast_interval) * 1000 * 1000;
    initial_target_latency = ((int64_t)
            cct->_conf->bluestore_codel_initial_target_latency) * 1000 * 1000;
    min_target_latency = ((int64_t)
            cct->_conf->bluestore_codel_min_target_latency) * 1000 * 1000;
    max_target_latency = ((int64_t)
            cct->_conf->bluestore_codel_max_target_latency) * 1000 * 1000;
    initial_bluestore_budget = cct->_conf->bluestore_codel_initial_budget_bytes;
    min_bluestore_budget = cct->_conf->bluestore_codel_min_budget_bytes;
    bluestore_budget_increment =
            cct->_conf->bluestore_codel_budget_increment_bytes;
    regression_history_size =
            cct->_conf->bluestore_codel_regression_history_size;

    bluestore_budget = initial_bluestore_budget;
    min_bluestore_budget = initial_bluestore_budget;
    max_queue_length = min_bluestore_budget;
    fast_interval = initial_fast_interval;
    target_latency = initial_target_latency;
    min_latency = INITIAL_LATENCY_VALUE;
    slow_interval_registered_bytes = 0;
    regression_throughput_history.clear();
    regression_target_latency_history.clear();
    slow_interval_start = ceph::mono_clock::zero();
  }

  {
    std::lock_guard l1{fast_timer_lock};
    fast_timer.cancel_all_events();
    fast_timer.init();
  }
  _fast_interval_process();
  {
    std::lock_guard l2{slow_timer_lock};
    slow_timer.cancel_all_events();
    slow_timer.init();
  }
  _slow_interval_process();
}

void BlueStoreSlowFastCoDel::reset_bluestore_budget() {
  if (activated) {
    bluestore_budget = std::max(min_bluestore_budget, bluestore_budget);
    bluestore_budget_reset_callback(bluestore_budget);
  }
}

void BlueStoreSlowFastCoDel::_fast_interval_process() {
  std::lock_guard l(register_lock);
  if (target_latency != INITIAL_LATENCY_VALUE &&
      min_latency != INITIAL_LATENCY_VALUE) {
    if (activated) {
      if (_check_latency_violation()) {
        // min latency violation
        violation_count++;
        _update_interval();
        on_min_latency_violation(); // handle the violation
      } else {
        // no latency violation
        violation_count = 0;
        fast_interval = initial_fast_interval;
        on_no_violation();
      }
      bluestore_budget = std::max(min_bluestore_budget, bluestore_budget);
      bluestore_budget_reset_callback(bluestore_budget);
    }

    // reset interval
    min_latency = INITIAL_LATENCY_VALUE;

    on_fast_interval_finished();
  }

  auto codel_ctx = new LambdaContext(
    [this](int r) {
      _fast_interval_process();
    });
  auto interval_duration = std::chrono::nanoseconds(fast_interval);
  fast_timer.add_event_after(interval_duration, codel_ctx);
}

void BlueStoreSlowFastCoDel::_slow_interval_process() {
  std::lock_guard l(register_lock);
  ceph::mono_clock::time_point now = ceph::mono_clock::now();
  if (activated && !ceph::mono_clock::is_zero(slow_interval_start)
      && slow_interval_txc_cnt > 0) {
    double time_sec = nanosec_to_sec(
      std::chrono::nanoseconds(now - slow_interval_start).count());

    double slow_interval_throughput =
      (slow_interval_registered_bytes * 1.0) / time_sec;
    slow_interval_throughput = slow_interval_throughput / (1024.0 * 1024.0);
    regression_target_latency_history.push_back(
      nanosec_to_millisec(target_latency));
    regression_throughput_history.push_back(slow_interval_throughput);
    if (regression_target_latency_history.size() > regression_history_size) {
      regression_target_latency_history.erase(
        regression_target_latency_history.begin());
      regression_throughput_history.erase(
        regression_throughput_history.begin());
    }
    std::vector<double> targets;
    std::vector<double> throughputs;
    double target_ms = nanosec_to_millisec(initial_target_latency);
    // If there is sufficient number of points, use the regression to find the
    //  target_ms. Otherwise, target_ms will be initial_target_latency
    if (regression_target_latency_history.size() >= regression_history_size) {
      target_ms = ceph::find_slope_on_curve(
        regression_target_latency_history,
        regression_throughput_history,
        target_slope);
    }

    target_latency_without_noise = millisec_to_nanosec(target_ms);
    target_latency_without_noise = std::max(target_latency_without_noise,
                                            min_target_latency);
    target_latency_without_noise = std::min(target_latency_without_noise,
                                            max_target_latency);
    target_ms = nanosec_to_millisec(target_latency_without_noise);

    // add log_normal noise
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine generator(seed);
    double dist_params[2];
    double rnd_std_dev = 5;
    ceph::find_log_normal_dist_params(
      target_ms,
      nanosec_to_millisec(min_target_latency),
      target_ms * rnd_std_dev,
      dist_params);
    std::lognormal_distribution<double> distribution(dist_params[0],
                                                     dist_params[1]);

    target_latency = millisec_to_nanosec(distribution(generator));
    target_latency += min_target_latency;

    if (target_latency < millisec_to_nanosec(target_ms)) {
      std::uniform_real_distribution<> distr(0, 0.5);
      target_latency = target_latency +
                       (target_latency - millisec_to_nanosec(target_ms)) *
                       distr(generator);
    }

    if (target_latency != INITIAL_LATENCY_VALUE) {
      target_latency = std::max(target_latency, min_target_latency);
      target_latency = std::min(target_latency, max_target_latency);
    }

    on_slow_interval_finished();
  }

  slow_interval_start = ceph::mono_clock::now();
  slow_interval_registered_bytes = 0;
  slow_interval_txc_cnt = 0;
  max_queue_length = min_bluestore_budget;

  auto codel_ctx = new LambdaContext(
    [this](int r) {
      _slow_interval_process();
    });
  auto interval_duration = std::chrono::nanoseconds(slow_interval);
  slow_timer.add_event_after(interval_duration, codel_ctx);
}


/**
* check if the min latency violate the target
* @return true if min latency violate the target, false otherwise
*/
bool BlueStoreSlowFastCoDel::_check_latency_violation() {
  if (target_latency != INITIAL_LATENCY_VALUE &&
      min_latency != INITIAL_LATENCY_VALUE) {
    if (min_latency > target_latency) {
      return true;
    }
  }
  return false;
}

void BlueStoreSlowFastCoDel::_update_interval() {
  auto sqrt = (int) std::round(std::sqrt(violation_count));
  fast_interval = initial_fast_interval / sqrt;
  if (fast_interval <= 0) {
    fast_interval = 1000;
  }
}

int64_t BlueStoreSlowFastCoDel::get_bluestore_budget() {
  return bluestore_budget;
}

int64_t BlueStoreSlowFastCoDel::get_target_latency() {
  return target_latency;
}
