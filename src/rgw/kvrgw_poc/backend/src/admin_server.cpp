// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "admin_server.hpp"

#include "fdb_latency.hpp"
#include "kvrgw.pb.h"
#include "ops_stats.hpp"
#include "service_impl.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace kvrgw {

namespace {

bool parse_bool(const std::string &value, bool *out)
{
  if (value == "0" || value == "false" || value == "off") {
    *out = false;
    return true;
  }
  if (value == "1" || value == "true" || value == "on") {
    *out = true;
    return true;
  }
  return false;
}

bool parse_int_field(const std::string &key, const std::string &value,
                     GcPolicy *policy, std::string *err)
{
  try {
    if (key == "suspended") {
      return parse_bool(value, &policy->suspended);
    }
    if (key == "interval_sec") {
      policy->interval_sec = std::stoi(value);
      return true;
    }
    if (key == "max_objects_per_sec") {
      policy->max_objects_per_sec = std::stoi(value);
      return true;
    }
    if (key == "max_mb_per_sec") {
      policy->max_mb_per_sec = std::stoi(value);
      return true;
    }
    *err = "unknown field: " + key;
    return false;
  }
  catch (const std::exception &ex) {
    *err = std::string("invalid value for ") + key + ": " + ex.what();
    return false;
  }
}

bool parse_set_policy(const std::string &args, GcPolicy *policy,
                      std::string *err)
{
  if (args.empty()) {
    *err = "SET requires key=value fields";
    return false;
  }
  std::istringstream in(args);
  std::string token;
  while (in >> token) {
    const auto eq = token.find('=');
    if (eq == std::string::npos) {
      *err = "expected key=value, got: " + token;
      return false;
    }
    const std::string key = token.substr(0, eq);
    const std::string value = token.substr(eq + 1);
    if (!parse_int_field(key, value, policy, err)) {
      return false;
    }
  }
  if (policy->interval_sec < 0 || policy->max_objects_per_sec < 0 ||
      policy->max_mb_per_sec < 0) {
    *err = "numeric fields must be non-negative";
    return false;
  }
  return true;
}

std::string format_config_line(const GcConfigSnapshot &snap)
{
  std::ostringstream out;
  out << "CONFIG active_age=" << snap.active_age
      << " pending_age=" << snap.pending_age
      << " suspended=" << (snap.active.suspended ? 1 : 0)
      << " interval_sec=" << snap.active.interval_sec
      << " max_objects_per_sec=" << snap.active.max_objects_per_sec
      << " max_mb_per_sec=" << snap.active.max_mb_per_sec;
  return out.str();
}

} // namespace

AdminServer::AdminServer(GcConfigState &gc_config, TierConfigState &tier_config,
                         std::string socket_path, std::atomic<bool> &stop_flag,
                         LatencyStats *latency_stats, ErrorStats *error_stats,
                         BatchStats *batch_stats, ErrInsertion *err_insertion,
                         OpsStats *ops_stats)
    : config_(gc_config), tier_config_(tier_config),
      socket_path_(std::move(socket_path)), stop_flag_(stop_flag),
      latency_stats_(latency_stats), error_stats_(error_stats),
      batch_stats_(batch_stats), err_insertion_(err_insertion),
      ops_stats_(ops_stats)
{
}

std::string AdminServer::handle_line(const std::string &line)
{
  if (line.empty()) {
    return "ERROR empty request";
  }

  const auto space = line.find(' ');
  const std::string cmd =
      (space == std::string::npos) ? line : line.substr(0, space);
  const std::string args =
      (space == std::string::npos) ? "" : line.substr(space + 1);

  if (cmd == "QUERY") {
    return "ACTIVE_AGE=" + std::to_string(config_.active_age());
  }

  if (cmd == "GET") {
    return format_config_line(config_.snapshot());
  }

  if (cmd == "SET") {
    GcPolicy policy = config_.active_policy_copy();
    std::string err;
    if (!parse_set_policy(args, &policy, &err)) {
      return "ERROR " + err;
    }
    const auto handle = config_.try_stage(policy);
    if (!handle) {
      return "BUSY";
    }
    return "OK HANDLE=" + std::to_string(*handle);
  }

  if (cmd == "get-tier-config") {
    const auto snap = tier_config_.snapshot();
    std::ostringstream out;
    out << "max_inline=" << snap.active.max_inline
        << " max_kv_store=" << snap.active.max_kv_store
        << " kv_store_coalescing=" << (snap.active.kv_store_coalescing ? 1 : 0)
        << " batch_size=" << snap.active.batch_size
        << " batch_timeout_us=" << snap.active.batch_timeout_us
        << " batch_threads=" << snap.active.batch_threads
        << " active_age=" << snap.active_age
        << " pending_age=" << snap.pending_age;
    return out.str();
  }

  if (cmd == "set-tier-config") {
    TierConfig tc = tier_config_.active_copy();
    std::istringstream ss(args);
    std::string token;
    while (ss >> token) {
      const auto eq = token.find('=');
      if (eq == std::string::npos) {
        return "ERROR bad key=value: " + token;
      }
      const auto key = token.substr(0, eq);
      const auto val = token.substr(eq + 1);
      if (key == "max_inline") {
        tc.max_inline = static_cast<uint32_t>(std::stoul(val));
      }
      else if (key == "max_kv_store") {
        tc.max_kv_store = static_cast<uint32_t>(std::stoul(val));
      }
      else if (key == "kv_store_coalescing") {
        tc.kv_store_coalescing = (val != "0" && val != "false");
      }
      else if (key == "batch_size") {
        tc.batch_size = std::atoi(val.c_str());
      }
      else if (key == "batch_timeout_us") {
        tc.batch_timeout_us = std::atoi(val.c_str());
      }
      else if (key == "batch_threads") {
        tc.batch_threads = std::atoi(val.c_str());
      }
      else {
        return "ERROR unknown tier key: " + key;
      }
    }
    if (tc.max_kv_store != 0 && tc.max_inline != 0 &&
        tc.max_kv_store <= tc.max_inline) {
      return "ERROR invariant: max_kv_store must be > max_inline";
    }
    const auto handle = tier_config_.try_stage(tc);
    if (!handle) {
      return "BUSY";
    }
    return "OK HANDLE=" + std::to_string(*handle);
  }

  if (cmd == "query-tier-config") {
    return "ACTIVE_AGE=" + std::to_string(tier_config_.active_age());
  }

  if (cmd == "get-latency") {
    if (!latency_stats_) {
      return "ERROR latency stats not available";
    }
    auto now = std::chrono::steady_clock::now();
    double elapsed = 0;
    if (lat_last_time_.time_since_epoch().count() > 0) {
      elapsed = std::chrono::duration<double>(now - lat_last_time_).count();
    }
    lat_last_time_ = now;
    std::ostringstream out;
    out << "OK\n";
    for (int i = 0; i < static_cast<int>(OpType::kCount); ++i) {
      auto &s = latency_stats_->ops[i];
      auto cnt = s.count.load(std::memory_order_relaxed);
      if (cnt == 0) {
        continue;
      }
      auto total = s.total_us.load(std::memory_order_relaxed);
      auto fdb = s.fdb_us.load(std::memory_order_relaxed);
      double pct = total > 0 ? 100.0 * fdb / total : 0.0;
      auto disk = s.disk_us.load(std::memory_order_relaxed);
      double disk_pct = total > 0 ? 100.0 * disk / total : 0.0;

      int64_t dc = cnt - lat_prev_count_[i];
      int64_t dt = total - lat_prev_total_us_[i];
      int64_t interval_total_us = dc > 0 ? dt / dc : 0;
      double interval_hz =
          (elapsed > 0.01 && dc > 0) ? static_cast<double>(dc) / elapsed : 0;
      lat_prev_count_[i] = cnt;
      lat_prev_total_us_[i] = total;

      out << op_type_name(static_cast<OpType>(i)) << " count=" << cnt
          << " avg_total_us=" << total / cnt
          << " interval_total_us=" << interval_total_us
          << " interval_hz=" << std::fixed << std::setprecision(0)
          << interval_hz << " avg_fdb_us=" << fdb / cnt
          << " fdb_pct=" << std::setprecision(1) << pct << " avg_get_us="
          << s.fdb_get_us.load(std::memory_order_relaxed) / cnt
          << " avg_commit_us="
          << s.fdb_commit_us.load(std::memory_order_relaxed) / cnt
          << " avg_scan_us="
          << s.fdb_scan_us.load(std::memory_order_relaxed) / cnt
          << " avg_disk_us=" << disk / cnt << " disk_pct=" << disk_pct << "\n";
    }
    return out.str();
  }

  if (cmd == "get-ops-stats") {
    if (!ops_stats_) {
      return "ERROR ops stats not available";
    }
    std::array<uint64_t, OpsStats::kN> snap{};
    ops_stats_->snapshot(snap);
    auto now = std::chrono::steady_clock::now();
    double get_hz = 0;
    const uint64_t get_cnt = snap[static_cast<std::size_t>(OpType::kGetObject)];
    if (ops_last_time_.time_since_epoch().count() > 0) {
      double elapsed =
          std::chrono::duration<double>(now - ops_last_time_).count();
      if (elapsed > 0.01) {
        get_hz = static_cast<double>(get_cnt - ops_prev_get_) / elapsed;
      }
    }
    ops_last_time_ = now;
    ops_prev_get_ = get_cnt;
    std::ostringstream out;
    bool any = false;
    for (std::size_t i = 0; i < OpsStats::kN; ++i) {
      auto op = static_cast<OpType>(i);
      if (op == OpType::kOther || op == OpType::kCount) {
        continue;
      }
      if (snap[i] == 0) {
        continue;
      }
      if (any) {
        out << ' ';
      }
      out << op_type_name(op) << '=' << snap[i];
      any = true;
    }
    if (!any) {
      out << "OK (all zero)";
    }
    out << " GetObject_hz=" << std::fixed << std::setprecision(0) << get_hz;
    return out.str();
  }

  if (cmd == "reset-latency") {
    if (!latency_stats_) {
      return "ERROR latency stats not available";
    }
    latency_stats_->reset();
    for (int i = 0; i < static_cast<int>(OpType::kCount); ++i) {
      lat_prev_count_[i] = 0;
      lat_prev_total_us_[i] = 0;
    }
    lat_last_time_ = {};
    return "OK";
  }

  if (cmd == "get-error-stats") {
    if (!error_stats_) {
      return "ERROR error stats not available";
    }
    std::ostringstream out;
    bool any = false;
    for (int i = 0; i < ::kvrgw::v1::KvrgwErrorCode_ARRAYSIZE; ++i) {
      auto cnt = error_stats_->counts[i].load(std::memory_order_relaxed);
      if (cnt == 0) {
        continue;
      }
      if (any) {
        out << ' ';
      }
      auto ec = static_cast<KvrgwErrorCode>(i);
      out << ::kvrgw::v1::KvrgwErrorCode_Name(ec) << '=' << cnt;
      any = true;
    }
    if (!any) {
      return "OK (all zero)";
    }
    return out.str();
  }

  if (cmd == "reset-error-stats") {
    if (!error_stats_) {
      return "ERROR error stats not available";
    }
    std::ostringstream out;
    bool any = false;
    for (int i = 0; i < ::kvrgw::v1::KvrgwErrorCode_ARRAYSIZE; ++i) {
      auto cnt = error_stats_->counts[i].exchange(0, std::memory_order_relaxed);
      if (cnt == 0) {
        continue;
      }
      if (any) {
        out << ' ';
      }
      auto ec = static_cast<KvrgwErrorCode>(i);
      out << ::kvrgw::v1::KvrgwErrorCode_Name(ec) << '=' << cnt;
      any = true;
    }
    if (!any) {
      return "OK (all zero)";
    }
    return out.str();
  }

  if (cmd == "get-batch-stats") {
    if (!batch_stats_) {
      return "ERROR batch stats not available";
    }
    auto now = std::chrono::steady_clock::now();
    int64_t bc = batch_stats_->batch_commits.load(std::memory_order_relaxed);
    int64_t te =
        batch_stats_->total_entries_batched.load(std::memory_order_relaxed);
    int64_t tw = batch_stats_->total_wait_us.load(std::memory_order_relaxed);
    int64_t tq = batch_stats_->total_queue_size_at_extract.load(
        std::memory_order_relaxed);
    double avg_bs = bc > 0 ? static_cast<double>(te) / bc : 0;
    int64_t avg_wait = bc > 0 ? tw / bc : 0;
    double avg_qsz = bc > 0 ? static_cast<double>(tq) / bc : 0;

    double entries_hz = 0, commits_hz = 0, interval_wait_us = 0,
           interval_qsz = 0;
    if (bs_last_time_.time_since_epoch().count() > 0) {
      double elapsed =
          std::chrono::duration<double>(now - bs_last_time_).count();
      if (elapsed > 0.01) {
        int64_t dc = bc - bs_prev_commits_;
        int64_t de = te - bs_prev_entries_;
        entries_hz = de / elapsed;
        commits_hz = dc / elapsed;
        interval_wait_us =
            dc > 0 ? static_cast<double>(tw - bs_prev_wait_) / dc : 0;
        interval_qsz =
            dc > 0 ? static_cast<double>(tq - bs_prev_queue_) / dc : 0;
      }
    }
    bs_last_time_ = now;
    bs_prev_commits_ = bc;
    bs_prev_entries_ = te;
    bs_prev_wait_ = tw;
    bs_prev_queue_ = tq;

    std::ostringstream out;
    out << "batch_commits=" << bc << " entries_batched=" << te
        << " conflict_pushbacks="
        << batch_stats_->conflict_pushbacks.load(std::memory_order_relaxed)
        << " avg_batch_size=" << std::fixed << std::setprecision(1) << avg_bs
        << " min_batch_size="
        << batch_stats_->min_batch_size.load(std::memory_order_relaxed)
        << " max_batch_size="
        << batch_stats_->max_batch_size.load(std::memory_order_relaxed)
        << " avg_queue_size=" << std::setprecision(1) << avg_qsz
        << " avg_wait_us=" << avg_wait << " min_wait_us="
        << batch_stats_->min_wait_us.load(std::memory_order_relaxed)
        << " max_wait_us="
        << batch_stats_->max_wait_us.load(std::memory_order_relaxed)
        << " entries_hz=" << std::setprecision(0) << entries_hz
        << " commits_hz=" << std::setprecision(0) << commits_hz
        << " interval_wait_us=" << std::setprecision(0) << interval_wait_us
        << " interval_queue_size=" << std::setprecision(1) << interval_qsz;
    return out.str();
  }

  if (cmd == "set-error") {
    if (!err_insertion_) {
      return "ERROR err_insertion not available";
    }
    std::istringstream ss(args);
    std::string name;
    ss >> name;
    if (name.empty()) {
      return "ERROR usage: set-error <name> [period=N] [burst=N] "
             "[interval_us=N]";
    }

    std::optional<FaultType> ft;
    if (name == "kAbortAfterBatchPhase2") {
      ft = FaultType::kAbortAfterBatchPhase2;
    }
    else if (name == "kAbortAfterSinglePhase2") {
      ft = FaultType::kAbortAfterSinglePhase2;
    }
    else if (name == "kAbortSweeperAfterPutGo") {
      ft = FaultType::kAbortSweeperAfterPutGo;
    }
    else if (name == "kAbortGcWorkerMidGroup") {
      ft = FaultType::kAbortGcWorkerMidGroup;
    }
    if (!ft) {
      return "ERROR unknown fault type: " + name;
    }

    int32_t period = 0;
    uint8_t burst = 0;
    int64_t interval_us = 0;
    std::string token;
    while (ss >> token) {
      const auto eq = token.find('=');
      if (eq == std::string::npos) {
        return "ERROR bad key=value: " + token;
      }
      const auto key = token.substr(0, eq);
      const auto val = token.substr(eq + 1);
      if (key == "period") {
        period = std::atoi(val.c_str());
      }
      else if (key == "burst") {
        burst = static_cast<uint8_t>(std::atoi(val.c_str()));
      }
      else if (key == "interval_us") {
        interval_us = std::atoll(val.c_str());
      }
      else {
        return "ERROR unknown param: " + key;
      }
    }

    if (period > 0) {
      err_insertion_->set_counter(*ft, period, burst);
    }
    else if (interval_us > 0) {
      err_insertion_->set_time(*ft, interval_us, burst);
    }
    else {
      err_insertion_->set_fixed(*ft);
    }
    return "OK";
  }

  if (cmd == "clear-error") {
    if (!err_insertion_) {
      return "ERROR err_insertion not available";
    }
    std::string name = args;
    while (!name.empty() && name.back() == ' ') {
      name.pop_back();
    }
    if (name.empty()) {
      return "ERROR usage: clear-error <name>";
    }

    std::optional<FaultType> ft;
    if (name == "kAbortAfterBatchPhase2") {
      ft = FaultType::kAbortAfterBatchPhase2;
    }
    else if (name == "kAbortAfterSinglePhase2") {
      ft = FaultType::kAbortAfterSinglePhase2;
    }
    else if (name == "kAbortSweeperAfterPutGo") {
      ft = FaultType::kAbortSweeperAfterPutGo;
    }
    else if (name == "kAbortGcWorkerMidGroup") {
      ft = FaultType::kAbortGcWorkerMidGroup;
    }
    if (!ft) {
      return "ERROR unknown fault type: " + name;
    }

    err_insertion_->clear(*ft);
    return "OK";
  }

  return "ERROR unknown command: " + cmd;
}

bool AdminServer::run()
{
  const int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (listen_fd < 0) {
    std::cerr << "admin socket failed: " << std::strerror(errno) << std::endl;
    return false;
  }

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  if (socket_path_.size() >= sizeof(addr.sun_path)) {
    close(listen_fd);
    std::cerr << "admin socket path too long" << std::endl;
    return false;
  }
  std::strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);
  std::remove(socket_path_.c_str());
  if (bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(listen_fd);
    std::cerr << "admin bind failed: " << std::strerror(errno) << std::endl;
    return false;
  }
  if (listen(listen_fd, 16) < 0) {
    close(listen_fd);
    std::cerr << "admin listen failed: " << std::strerror(errno) << std::endl;
    return false;
  }

  while (!stop_flag_) {
    pollfd pfd{};
    pfd.fd = listen_fd;
    pfd.events = POLLIN;
    const int rc = poll(&pfd, 1, 200);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    if (rc == 0) {
      continue;
    }
    if (!(pfd.revents & POLLIN)) {
      continue;
    }

    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd < 0) {
      continue;
    }

    std::string request;
    char buf[512];
    while (true) {
      const ssize_t n = read(client_fd, buf, sizeof(buf));
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }
        break;
      }
      if (n == 0) {
        break;
      }
      request.append(buf, static_cast<size_t>(n));
      if (request.find('\n') != std::string::npos) {
        break;
      }
      if (request.size() > 4096) {
        break;
      }
    }

    const auto nl = request.find('\n');
    if (nl != std::string::npos) {
      request.resize(nl);
    }
    while (!request.empty() &&
           (request.back() == '\r' || request.back() == '\n')) {
      request.pop_back();
    }

    const std::string response = handle_line(request);
    std::string out = response;
    out.push_back('\n');
    (void)write(client_fd, out.data(), out.size());
    close(client_fd);
  }

  close(listen_fd);
  std::remove(socket_path_.c_str());
  return true;
}

} // namespace kvrgw
