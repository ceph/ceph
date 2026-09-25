// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/*
 * TestLogBudget - per-op debug log budget for the OSD backends.
 *
 * Debug level 10 is the "lean" level (doc/dev/osd_internals/
 * debug_log_levels.rst): about one line per IO per layer on the good path,
 * plus state changes and errors.  These tests run a small workload through
 * the in-process backend fixtures twice, once at debug_osd=20 and once at
 * debug_osd=10, capture the log entries written to a temporary log file and
 * check:
 *
 *   - level<=10 entries and bytes per client op (create, partial overwrite,
 *     read), summed over all shards/replicas of the PG,
 *   - the ratio of level<=10 bytes to level<=20 bytes for the same
 *     workload,
 *   - level<=10 entries and bytes for one OSD failure + recovery cycle.
 *
 * Only the backend (ECBackend/ECCommon, legacy EC, ReplicatedBackend) and
 * PeeringState/PGLog are exercised; OSD::dequeue_op, PrimaryLogPG and
 * BlueStore are not part of these fixtures.
 *
 * The budgets below are deliberately generous, but were written without
 * being run: no Linux build was available.  Each run prints one
 * "LOG_BUDGET" line per measurement with the actual values.  The test is
 * report-only (it always passes) until the budgets have been seen against
 * a real run and tightened; set CEPH_LOG_BUDGET_ENFORCE=1 in the
 * environment to fail on a budget that is exceeded.
 *
 * Bytes are message bytes: the "<timestamp> <thread> <level> " header that
 * every entry carries in a real log (~45 bytes) is not counted.
 */

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "log/Log.h"
#include "test/osd/ECPeeringTestFixture.h"
#include "test/osd/PGBackendTestFixture.h"
#include "test/osd/TestCommon.h"

namespace {

// ---------------------------------------------------------------------------
// Budgets.  Per-op values are summed over all shards/replicas of the PG.
// ---------------------------------------------------------------------------
constexpr int kNumOps = 16;
constexpr double kMaxL10EntriesPerCreate = 150;
constexpr double kMaxL10BytesPerCreate = 64 * 1024;
constexpr double kMaxL10EntriesPerOverwrite = 150;
constexpr double kMaxL10BytesPerOverwrite = 64 * 1024;
constexpr double kMaxL10EntriesPerRead = 100;
constexpr double kMaxL10BytesPerRead = 32 * 1024;
// Policy target is 0.20 (level 10 costs at most 20% of level 20).
constexpr double kMaxL10ToL20ByteRatio = 0.90;
constexpr uint64_t kMaxL10EntriesPerFailoverCycle = 20000;
constexpr uint64_t kMaxL10BytesPerFailoverCycle = 8 * 1024 * 1024;
// Level<=10 entries must be single lines; only reported for now.
constexpr bool kFailOnL10MultilineEntries = false;

bool report_only()
{
  // Report-only by default: the budgets below have not been checked
  // against a real run.  Set CEPH_LOG_BUDGET_ENFORCE=1 once they have been
  // tuned to make the test fail on a budget that is exceeded.
  return std::getenv("CEPH_LOG_BUDGET_ENFORCE") == nullptr;
}

struct LogStats {
  uint64_t entries = 0;           ///< log entries (header lines)
  uint64_t bytes = 0;             ///< message bytes incl. continuation lines
  uint64_t multiline_entries = 0; ///< entries that contain a '\n'
  /// template -> (entries, bytes), for diagnostics
  std::map<std::string, std::pair<uint64_t, uint64_t>> templates;

  std::string top_templates(size_t n) const {
    std::vector<std::pair<uint64_t, std::string>> v;
    for (const auto& [tmpl, eb] : templates) {
      v.emplace_back(eb.second, tmpl);
    }
    std::sort(v.begin(), v.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });
    std::ostringstream out;
    for (size_t i = 0; i < v.size() && i < n; ++i) {
      const auto& eb = templates.at(v[i].second);
      out << "    " << eb.second << "B " << eb.first << "x  " << v[i].second
          << "\n";
    }
    return out.str();
  }
};

/**
 * Parse a line written by ceph::logging::Log::_flush():
 *   "<timestamp> <thread-id or prefix-hook> <prio> <message>"
 * The prio is printed with "%2d", so single-digit levels are preceded by
 * two spaces.  Returns false for continuation lines of multi-line entries.
 */
bool parse_entry_header(const std::string& line, int& prio, std::string& msg)
{
  if (line.size() < 24 ||
      !std::isdigit(static_cast<unsigned char>(line[0])) ||
      line[4] != '-' || line[10] != 'T') {
    return false;
  }
  size_t p = line.find(' ');
  if (p == std::string::npos) {
    return false;
  }
  p = line.find(' ', p + 1);  // end of the thread id / prefix
  if (p == std::string::npos) {
    return false;
  }
  while (p < line.size() && line[p] == ' ') {
    ++p;
  }
  size_t q = p;
  if (q < line.size() && line[q] == '-') {
    ++q;
  }
  const size_t digits = q;
  while (q < line.size() && std::isdigit(static_cast<unsigned char>(line[q]))) {
    ++q;
  }
  if (q == digits || q >= line.size() || line[q] != ' ') {
    return false;
  }
  prio = std::stoi(line.substr(p, q - p));
  msg = line.substr(q + 1);
  return true;
}

/// Lines logged by the test harness itself (MockMessenger, EventLoop,
/// MockPeeringListener, MockPGLogEntryHandler) rather than by the
/// production code under test.
bool is_harness_line(const std::string& msg)
{
  return msg.rfind("MockMessenger:", 0) == 0 ||
         msg.rfind("EventLoop:", 0) == 0 ||
         msg.rfind("MockPGLogEntryHandler::", 0) == 0 ||
         msg.rfind("send_cluster_message to ", 0) == 0 ||
         msg.rfind("activate ", 0) == 0 ||
         msg == "on_activate_complete";
}

/// Message with the "pg[...]" prefix removed and digit runs replaced by N.
std::string make_template(const std::string& msg)
{
  std::string_view body(msg);
  if (body.substr(0, 3) == "pg[") {
    int depth = 0;
    size_t i = 0;
    for (; i < body.size(); ++i) {
      const char c = body[i];
      if (c == '[' || c == '(') {
        ++depth;
      } else if (c == ']' || c == ')') {
        --depth;
        if (depth == 0) {
          break;
        }
      }
    }
    body = (i + 1 < body.size()) ? body.substr(i + 1) : std::string_view();
  }
  std::string out;
  bool in_digits = false;
  for (const char c : body) {
    if (std::isdigit(static_cast<unsigned char>(c))) {
      if (!in_digits) {
        out.push_back('N');
      }
      in_digits = true;
    } else {
      out.push_back(c);
      in_digits = false;
    }
    if (out.size() >= 80) {
      break;
    }
  }
  return out;
}

/// Removes a file on construction and destruction.
struct FileRemover {
  std::string path;
  explicit FileRemover(const std::string& p) : path(p) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
  }
  ~FileRemover() {
    std::error_code ec;
    std::filesystem::remove(path, ec);
  }
};

/**
 * Redirects the log to a private file for the lifetime of the object and
 * measures what a piece of code writes to it.  Log to stderr is disabled
 * meanwhile so ctest output stays small.
 */
class LogCapture {
public:
  explicit LogCapture(const std::string& path)
    : remover_(path),
      path_(path),
      no_stderr_("log_to_stderr", "false"),
      to_file_("log_to_file", "true"),
      file_("log_file", path) {}

  /// Run fn() with debug_osd at `level` ("10/10", "20/20") and return the
  /// statistics of the entries it logged.
  template <typename F>
  LogStats measure(const std::string& level, F&& fn) {
    ScopedConfig debug_osd("debug_osd", level);
    g_ceph_context->_log->flush();
    std::error_code ec;
    uintmax_t start = std::filesystem::file_size(path_, ec);
    if (ec) {
      start = 0;
    }
    fn();
    g_ceph_context->_log->flush();
    return parse(start);
  }

private:
  LogStats parse(uintmax_t start) const {
    LogStats s;
    std::ifstream in(path_, std::ios::binary);
    if (!in) {
      return s;
    }
    in.seekg(static_cast<std::streamoff>(start));
    std::string line;
    std::string tmpl;
    bool counting = false;   // current entry is counted (not harness)
    bool continued = false;  // current entry already has a continuation
    while (std::getline(in, line)) {
      int prio = 0;
      std::string msg;
      if (parse_entry_header(line, prio, msg)) {
        continued = false;
        counting = !is_harness_line(msg);
        if (!counting) {
          continue;
        }
        tmpl = make_template(msg);
        ++s.entries;
        s.bytes += msg.size() + 1;
        auto& t = s.templates[tmpl];
        ++t.first;
        t.second += msg.size() + 1;
      } else if (counting) {
        if (!continued) {
          ++s.multiline_entries;
          continued = true;
        }
        s.bytes += line.size() + 1;
        s.templates[tmpl].second += line.size() + 1;
      }
    }
    return s;
  }

  // Declaration order matters: the ScopedConfigs are destroyed (restoring
  // the log settings and closing the file) before remover_ deletes it.
  FileRemover remover_;
  std::string path_;
  ScopedConfig no_stderr_;
  ScopedConfig to_file_;
  ScopedConfig file_;
};

struct PhaseStats {
  LogStats create;
  LogStats overwrite;
  LogStats read;
};

/**
 * kNumOps full-stripe creates, kNumOps 4KiB partial overwrites and kNumOps
 * full reads on objects "<prefix>_<i>", each phase measured at `level`.
 */
PhaseStats run_workload(PGBackendTestFixture& f, LogCapture& cap,
                        const std::string& level, const std::string& prefix)
{
  PhaseStats ps;
  const uint64_t object_size = f.get_stripe_width();
  const std::string full(object_size, 'A');
  const std::string partial(4096, 'B');
  auto name = [&prefix](int i) { return prefix + "_" + std::to_string(i); };

  ps.create = cap.measure(level, [&]() {
    for (int i = 0; i < kNumOps; ++i) {
      EXPECT_GE(f.create_and_write(name(i), full), 0) << name(i);
    }
  });
  ps.overwrite = cap.measure(level, [&]() {
    for (int i = 0; i < kNumOps; ++i) {
      EXPECT_GE(f.write(name(i), 4096, partial, object_size), 0) << name(i);
    }
  });
  ps.read = cap.measure(level, [&]() {
    for (int i = 0; i < kNumOps; ++i) {
      bufferlist bl;
      EXPECT_GE(f.read_object(name(i), 0, object_size, bl, object_size), 0)
        << name(i);
    }
  });
  return ps;
}

void report(const std::string& label, const std::string& phase,
            uint64_t ops, const LogStats& at20, const LogStats& at10)
{
  const double n = static_cast<double>(ops);
  std::cout << "LOG_BUDGET config=" << label << " phase=" << phase
            << " ops=" << ops
            << " l20_entries_per_op=" << at20.entries / n
            << " l20_bytes_per_op=" << at20.bytes / n
            << " l10_entries_per_op=" << at10.entries / n
            << " l10_bytes_per_op=" << at10.bytes / n
            << " l10_multiline_entries=" << at10.multiline_entries
            << std::endl;
}

void check_phase(const std::string& label, const std::string& phase,
                 const LogStats& at20, const LogStats& at10,
                 double max_entries, double max_bytes)
{
  report(label, phase, kNumOps, at20, at10);
  if (report_only()) {
    return;
  }
  const double entries = static_cast<double>(at10.entries) / kNumOps;
  const double bytes = static_cast<double>(at10.bytes) / kNumOps;
  EXPECT_LE(entries, max_entries)
    << label << " " << phase << ": level<=10 entries per op over budget;"
    << " top level<=10 templates:\n" << at10.top_templates(10);
  EXPECT_LE(bytes, max_bytes)
    << label << " " << phase << ": level<=10 bytes per op over budget;"
    << " top level<=10 templates:\n" << at10.top_templates(10);
  if (kFailOnL10MultilineEntries) {
    EXPECT_EQ(0u, at10.multiline_entries)
      << label << " " << phase << ": multi-line level<=10 entries";
  }
}

void check_workload(const std::string& label,
                    const PhaseStats& at20, const PhaseStats& at10)
{
  ASSERT_GT(at20.create.entries, 0u)
    << "no log entries captured at debug_osd=20: log capture is broken";
  check_phase(label, "create", at20.create, at10.create,
              kMaxL10EntriesPerCreate, kMaxL10BytesPerCreate);
  check_phase(label, "overwrite", at20.overwrite, at10.overwrite,
              kMaxL10EntriesPerOverwrite, kMaxL10BytesPerOverwrite);
  check_phase(label, "read", at20.read, at10.read,
              kMaxL10EntriesPerRead, kMaxL10BytesPerRead);

  const uint64_t b20 =
    at20.create.bytes + at20.overwrite.bytes + at20.read.bytes;
  const uint64_t b10 =
    at10.create.bytes + at10.overwrite.bytes + at10.read.bytes;
  const double ratio = b20 ? static_cast<double>(b10) / b20 : 0.0;
  std::cout << "LOG_BUDGET config=" << label << " phase=all"
            << " l20_bytes=" << b20 << " l10_bytes=" << b10
            << " l10_to_l20_ratio=" << ratio << std::endl;
  if (!report_only()) {
    EXPECT_LE(ratio, kMaxL10ToL20ByteRatio)
      << label << ": level<=10 output is too large a fraction of level 20";
  }
}

const std::vector<BackendConfig> kLogBudgetBackendConfigs = {
  {PGBackendTestFixture::REPLICATED, "", "", 0, 4096, 4, 2, "Replicated"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, "EC_Jerasure_Opt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES, 4096,  4, 2, "EC_Jerasure_NonOpt_k4m2_su4k"},
};

const std::vector<BackendConfig> kLogBudgetPeeringConfigs = {
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, "EC_Jerasure_Opt_k4m2_su4k"},
};

}  // namespace

// ---------------------------------------------------------------------------
// Backend only (no PeeringState): replicated, optimized EC, legacy EC.
// ---------------------------------------------------------------------------

class TestLogBudgetBackend : public PGBackendTestFixture,
                             public ::testing::WithParamInterface<BackendConfig> {
public:
  TestLogBudgetBackend() : PGBackendTestFixture() {
    const auto& config = GetParam();
    pool_type = config.pool_type;
    if (pool_type == EC) {
      k = config.k;
      m = config.m;
      stripe_unit = config.stripe_unit;
      ec_plugin = config.ec_plugin;
      ec_technique = config.ec_technique;
      pool_flags = config.pool_flags;
    } else {
      num_replicas = 3;
      min_size = 2;
    }
  }
};

TEST_P(TestLogBudgetBackend, WriteReadBudget) {
  LogCapture cap(data_dir + "_log_budget.log");
  const PhaseStats at20 = run_workload(*this, cap, "20/20", "lb20");
  const PhaseStats at10 = run_workload(*this, cap, "10/10", "lb10");
  check_workload(GetParam().label, at20, at10);
}

INSTANTIATE_TEST_SUITE_P(
  LogBudget,
  TestLogBudgetBackend,
  ::testing::ValuesIn(kLogBudgetBackendConfigs),
  [](const ::testing::TestParamInfo<BackendConfig>& info) {
    return info.param.label;
  }
);

// ---------------------------------------------------------------------------
// With PeeringState (pg log, state machine): optimized EC.
// ---------------------------------------------------------------------------

class TestLogBudgetPeering : public ECPeeringTestFixture,
                             public ::testing::WithParamInterface<BackendConfig> {
public:
  TestLogBudgetPeering() : ECPeeringTestFixture() {
    const auto& config = GetParam();
    k = config.k;
    m = config.m;
    stripe_unit = config.stripe_unit;
    ec_plugin = config.ec_plugin;
    ec_technique = config.ec_technique;
    pool_flags = config.pool_flags;
  }
};

TEST_P(TestLogBudgetPeering, WriteReadBudget) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  LogCapture cap(data_dir + "_log_budget.log");
  const PhaseStats at20 = run_workload(*this, cap, "20/20", "lb20");
  const PhaseStats at10 = run_workload(*this, cap, "10/10", "lb10");
  check_workload(GetParam().label + "_Peering", at20, at10);
}

TEST_P(TestLogBudgetPeering, FailoverCycleBudget) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  LogCapture cap(data_dir + "_log_budget.log");

  const std::string obj_name = "lb_failover";
  const size_t data_size = stripe_unit * k;  // one full stripe
  const std::string pattern_a(data_size, 'A');
  const std::string pattern_b(data_size, 'B');
  const int removed_osd = 1;                 // non-primary

  create_and_write_verify(obj_name, pattern_a);
  const LogStats cycle = cap.measure("10/10", [&]() {
    mark_osd_down(removed_osd);
    write_verify(obj_name, 0, pattern_b, data_size);
    mark_osd_up(removed_osd);
    run_recovery_and_verify_callbacks(obj_name, removed_osd, pattern_b);
  });

  std::cout << "LOG_BUDGET config=" << GetParam().label
            << " phase=failover_cycle l10_entries=" << cycle.entries
            << " l10_bytes=" << cycle.bytes
            << " l10_multiline_entries=" << cycle.multiline_entries
            << std::endl;
  if (!report_only()) {
    EXPECT_LE(cycle.entries, kMaxL10EntriesPerFailoverCycle)
      << "top level<=10 templates:\n" << cycle.top_templates(15);
    EXPECT_LE(cycle.bytes, kMaxL10BytesPerFailoverCycle)
      << "top level<=10 templates:\n" << cycle.top_templates(15);
  }
}

INSTANTIATE_TEST_SUITE_P(
  LogBudget,
  TestLogBudgetPeering,
  ::testing::ValuesIn(kLogBudgetPeeringConfigs),
  [](const ::testing::TestParamInfo<BackendConfig>& info) {
    return info.param.label;
  }
);
