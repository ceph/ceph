// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/osd_ofi.h"

#include <atomic>
#include <cstring>
#include <map>
#include <sstream>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/JSONFormatter.h"
#include "common/ceph_context.h"
#include "common/deleter.h"
#include "common/config.h"
#include "global/global_context.h"
#include "include/scope_guard.h"
#include "common/ofi_rma.h"
#include "common/oob_arena.h"
#include "common/oob_config.h"
#include "osd/oob_placement.h"

namespace {

/// a gather window, as the executor sizes them
constexpr size_t WINDOW = OSDOobExecutor::GATHER_WINDOW_SIZE;
/// the gather windows the executor of the cluster transport lends
constexpr size_t WINDOWS = OSDOobExecutor::GATHER_WINDOW_COUNT;
/// the blocks of the arena that the windows take, as reserve() rounds
/// them up
constexpr size_t WINDOW_BLOCKS =
  (WINDOWS * WINDOW + ceph::oob::Arena::MAX - 1) / ceph::oob::Arena::MAX;

/// a libfabric executor over tcp on the loopback, which needs no special
/// hardware, staging in an arena of blocks of the arena's largest buffer
std::shared_ptr<ceph::oob::Arena> arena(size_t blocks)
{
  std::string err;
  auto a = ceph::oob::Arena::create(blocks * ceph::oob::Arena::MAX, &err);
  EXPECT_TRUE(a) << err;
  return a;
}

ceph::oob::location_t loopback()
{
  ceph::oob::location_t loc;
  loc.addr = "127.0.0.1";
  loc.source = "oob_addr";
  return loc;
}

/// whether libfabric offers the tcp provider on the loopback. A test
/// skips only without it: the executor failing to start over it is a
/// failure of the test, not a reason to skip. The probe opens an
/// ofi-rma endpoint, as the executor does, so a test skips as well
/// should ofi-rma come to ask libfabric for more than tcp offers: only
/// ofi-rma's own tests, which a Ceph build leaves out, catch that
bool tcp_available()
{
  ofi_rma_config c;
  ofi_rma_config_init(&c);
  c.provider = "tcp";
  c.node = "127.0.0.1";
  ofi_rma_ep* ep = nullptr;
  char err[1024] = "";
  const int r = ofi_rma_ep_open(&c, &ep, err, sizeof(err));
  ofi_rma_ep_close(ep);
  // -ENODATA: fi_getinfo() found no tcp provider with what ofi-rma
  // asks for
  EXPECT_TRUE(r == 0 || r == -ENODATA) << err;
  return r != -ENODATA;
}

} // anonymous namespace

// Only the executor of the cluster transport lends the gather windows,
// and registers them. Another one pins no memory for windows, and lends
// none.
TEST(OSDOfi, RegistersWindowsOnlyWhenLending)
{
  if (!tcp_available()) {
    GTEST_SKIP() << "tcp is not available";
  }
  {
    auto a = arena(1);
    OSDOfi ofi(g_ceph_context, nullptr);
    ASSERT_EQ(0, ofi.init("tcp", loopback(), false, a.get()));
    EXPECT_TRUE(ofi.is_available());
    EXPECT_FALSE(ofi.acquire_window(4096));
    EXPECT_EQ(0u, a->stats().reserved);
  }
  {
    // the windows take a run of whole blocks of the arena, and leave
    // one block for staging
    auto a = arena(WINDOW_BLOCKS + 1);
    OSDOfi ofi(g_ceph_context, nullptr);
    ASSERT_EQ(0, ofi.init("tcp", loopback(), true, a.get()));
    EXPECT_EQ(WINDOW_BLOCKS * ceph::oob::Arena::MAX, a->stats().reserved);
    // more than a window holds is refused while every window is free
    EXPECT_FALSE(ofi.acquire_window(WINDOW + 1));
    std::vector<OSDOobExecutor::window_t> lent;
    while (auto w = ofi.acquire_window(4096)) {
      EXPECT_EQ(WINDOW, w->size);
      EXPECT_FALSE(w->token.empty());
      lent.push_back(*w);
    }
    EXPECT_EQ(WINDOWS, lent.size());
    for (const auto& w : lent) {
      ofi.release_window(w.id, 0);
    }
  }
  if (WINDOW_BLOCKS > 1) {
    // no room for the windows: the executor still delivers
    auto a = arena(WINDOW_BLOCKS - 1);
    OSDOfi ofi(g_ceph_context, nullptr);
    ASSERT_EQ(0, ofi.init("tcp", loopback(), true, a.get()));
    EXPECT_FALSE(ofi.acquire_window(4096));
    EXPECT_EQ(0u, a->stats().reserved);
  }
  {
    // the windows would take every block, and leave transfers nowhere
    // to stage: they stay inline, and the arena stays for staging
    auto a = arena(WINDOW_BLOCKS);
    OSDOfi ofi(g_ceph_context, nullptr);
    ASSERT_EQ(0, ofi.init("tcp", loopback(), true, a.get()));
    EXPECT_FALSE(ofi.acquire_window(4096));
    EXPECT_EQ(0u, a->stats().reserved);
    auto s = a->alloc(ceph::oob::Arena::MAX);
    EXPECT_TRUE(s);
    a->free(s);
  }
}

namespace {

/// a client that lends a window of data that peers may read, as an
/// OSD-direct PUT's client does
struct client_t {
  std::unique_ptr<ceph::ofi::Endpoint> ep;
  std::vector<char> mem;
  ceph::ofi::Endpoint::window_t w;
  std::string token;
};

client_t lend(size_t n, unsigned access, bool progress = true)
{
  client_t c;
  ceph::ofi::config_t cfg;
  cfg.provider = "tcp";
  cfg.node = "127.0.0.1";
  cfg.progress_thread = progress;  // tcp serves remote reads only when polled
  cfg.reads = true;
  std::string err;
  c.ep = ceph::ofi::Endpoint::open(cfg, &err);
  if (!c.ep) {
    return c;
  }
  c.mem.resize(n);
  for (size_t i = 0; i < n; i++) {
    c.mem[i] = static_cast<char>(i * 31 + 7);
  }
  if (c.ep->register_window(c.mem.data(), n, ceph::ofi::memory_t{}, access,
			    &c.w) == 0) {
    c.token = c.ep->window_token(c.w, 0, n);
  }
  return c;
}

/// a client that stops polling once the executor reached it: over tcp, a
/// write into its window then cannot complete, and is cut off
client_t stalled(OSDOfi& ofi, size_t n)
{
  auto c = lend(n, ceph::ofi::Endpoint::remote_write, false);
  if (!c.ep || c.token.empty()) {
    return c;
  }
  std::atomic<bool> stop{false};
  std::thread poller([&] { while (!stop) c.ep->progress(); });
  ceph::buffer::list first;
  first.append_zero(4096);
  const ssize_t r = ofi.execute_plan(
    "obj", c.token, first, ceph::osd::oob::linear_plan(0, 4096),
    std::chrono::seconds(5), nullptr);
  stop = true;
  poller.join();
  EXPECT_EQ(4096, r);
  if (r != 4096) {
    c.token.clear();
  }
  return c;
}

} // anonymous namespace

// An OSD-direct PUT: the primary pulls a write's payload out of the
// client's window, through its staging, into a buffer of its own.
TEST(OSDOfi, PullsAWritePayload)
{
  if (!tcp_available()) {
    GTEST_SKIP() << "tcp is not available";
  }
  auto a = arena(1);
  OSDOfi ofi(g_ceph_context, nullptr);
  ASSERT_EQ(0, ofi.init("tcp", loopback(), false, a.get()));
  auto c = lend(WINDOW, ceph::ofi::Endpoint::remote_read);
  ASSERT_TRUE(c.ep);
  ASSERT_FALSE(c.token.empty()) << c.ep->last_error();
  ASSERT_TRUE(ofi.pulls(c.token));

  // the second half of the window, as a stripe at offset WINDOW / 2
  const uint64_t n = WINDOW / 2;
  const auto plan = ceph::osd::oob::linear_plan(WINDOW / 2, n);
  ceph::buffer::list out;
  bool started = false;
  ASSERT_EQ(ssize_t(n), ofi.execute_pull("obj", c.token, plan, n, &out,
					 std::chrono::seconds(5), &started));
  EXPECT_TRUE(started);
  ASSERT_EQ(n, out.length());
  EXPECT_EQ(0, memcmp(out.c_str(), c.mem.data() + WINDOW / 2, n));

  // a window lent for writes only cannot be read, and a failed pull
  // hands nothing out
  auto wo = lend(WINDOW, ceph::ofi::Endpoint::remote_write);
  ASSERT_FALSE(wo.token.empty());
  ceph::buffer::list none;
  none.append("untouched");
  EXPECT_LT(ofi.execute_pull("obj", wo.token, ceph::osd::oob::linear_plan(0, n),
			     n, &none, std::chrono::seconds(2), nullptr), 0);
  EXPECT_EQ("untouched", none.to_str());
  // the endpoint is whole, and the executor goes on
  EXPECT_TRUE(ofi.is_available());

  // the staging buffer went back to the arena
  EXPECT_EQ(0u, a->stats().in_use);

  // a payload larger than the arena's largest buffer is not one it can
  // pull
  const size_t over = ceph::oob::Arena::MAX + 4096;
  auto large = lend(over, ceph::ofi::Endpoint::remote_read);
  ASSERT_FALSE(large.token.empty());
  ceph::buffer::list big;
  EXPECT_EQ(-E2BIG, ofi.execute_pull(
	      "obj", large.token, ceph::osd::oob::linear_plan(0, over),
	      over, &big, std::chrono::seconds(2), nullptr));

  // nor is one larger than a buffer can hold: it is refused before any
  // buffer is taken or any read is sent, whatever the window
  const uint64_t huge = (1ull << 32) + 4096;
  bool started_huge = true;
  EXPECT_EQ(-E2BIG, ofi.execute_pull(
	      "obj", c.token, ceph::osd::oob::linear_plan(0, huge),
	      huge, &big, std::chrono::seconds(2), &started_huge));
  EXPECT_FALSE(started_huge);
  EXPECT_EQ(0u, big.length());
  EXPECT_TRUE(ofi.is_available());
}

// The executor reports the bytes its plan placed, not the length of the
// data it was handed: the caller counts an op placed only when the two
// are the same, from the heap or from the arena.
TEST(OSDOfi, ReturnsWhatThePlanPlaced)
{
  if (!tcp_available()) {
    GTEST_SKIP() << "tcp is not available";
  }
  auto a = arena(1);
  OSDOfi ofi(g_ceph_context, nullptr);
  ASSERT_EQ(0, ofi.init("tcp", loopback(), false, a.get()));
  auto c = lend(WINDOW, ceph::ofi::Endpoint::remote_write);
  ASSERT_TRUE(c.ep);
  ASSERT_FALSE(c.token.empty()) << c.ep->last_error();
  std::fill(c.mem.begin(), c.mem.end(), 0);

  ceph::buffer::list heap;
  heap.append(std::string(8192, 'h'));
  ASSERT_EQ(4096, ofi.execute_plan(
    "obj", c.token, heap, ceph::osd::oob::linear_plan(0, 4096),
    std::chrono::seconds(5), nullptr));
  c.ep->sync();
  EXPECT_EQ(std::string(4096, 'h'), std::string(c.mem.data(), 4096));
  EXPECT_EQ(std::string(4096, '\0'), std::string(c.mem.data() + 4096, 4096));

  // a buffer of the arena, which the bufferlist gives back
  const auto s = a->alloc(8192);
  ASSERT_TRUE(s);
  std::fill(a->ptr(s), a->ptr(s) + 8192, 'a');
  ceph::buffer::list arena_data;
  {
    auto* ar = a.get();
    arena_data.append(ceph::buffer::ptr(ceph::buffer::claim_buffer(
      8192, a->ptr(s), make_deleter([ar, s] { ar->free(s); }))));
  }
  ASSERT_EQ(1000, ofi.execute_plan(
    "obj", c.token, arena_data, ceph::osd::oob::linear_plan(16384, 1000),
    std::chrono::seconds(5), nullptr));
  c.ep->sync();
  EXPECT_EQ(std::string(1000, 'a'), std::string(c.mem.data() + 16384, 1000));
  EXPECT_EQ('\0', c.mem[16384 + 1000]);
  arena_data.clear();
  EXPECT_EQ(0u, a->stats().in_use);
}

// The executor reads osd_oob_cutoff_policy when it opens its endpoint,
// which judges a late cut-off by it from then on: a change at runtime
// would apply only in part, so a running daemon refuses one. One stored
// in the monitors applies when the daemon next starts.
TEST(OSDOfi, CutoffPolicyIsSetAtStartup)
{
  auto& conf = g_ceph_context->_conf;
  EXPECT_EQ(-EPERM, conf.set_val("osd_oob_cutoff_policy", "disable"));
  EXPECT_EQ("warn", conf.get_val<std::string>("osd_oob_cutoff_policy"));
}

// A cut-off that fails leaves writes that may still land. The executor
// stops, and raises OOB_DELIVERY_UNSAFE alone: not OOB_DELIVERY_DOWN too,
// although its endpoint is broken as well.
TEST(OSDOfi, FailedCutOffRaisesUnsafe)
{
  if (!tcp_available()) {
    GTEST_SKIP() << "tcp is not available";
  }
  auto a = arena(1);
  OSDOfi ofi(g_ceph_context, nullptr);
  ASSERT_EQ(0, ofi.init("tcp", loopback(), false, a.get()));
  const size_t n = 4 << 20;
  auto c = stalled(ofi, n);
  ASSERT_TRUE(c.ep);
  ASSERT_FALSE(c.token.empty()) << c.ep->last_error();
  std::map<std::string, std::string> alerts;
  ofi.get_alerts(alerts);
  EXPECT_TRUE(alerts.empty());

  // the provider fails to close the endpoint in the cut-off
  auto& conf = g_ceph_context->_conf;
  ASSERT_EQ(0, conf.set_val("osd_ofi_inject_cutoff_failure", "true"));
  ceph::buffer::list data;
  data.append_zero(n);
  const ssize_t r = ofi.execute_plan(
    "obj", c.token, data, ceph::osd::oob::linear_plan(0, n),
    std::chrono::milliseconds(600), nullptr);
  conf.set_val("osd_ofi_inject_cutoff_failure", "false");
  EXPECT_EQ(-ENOTRECOVERABLE, r);
  EXPECT_FALSE(ofi.is_available());
  EXPECT_FALSE(ofi.handles(c.token));
  ofi.get_alerts(alerts);
  ASSERT_EQ(1u, alerts.size());
  EXPECT_EQ("OOB_DELIVERY_UNSAFE", alerts.begin()->first);
}

// The executor acts on osd_oob_cutoff_policy as it read it when it
// opened its endpoint. A policy that changed after that - here to abort,
// past the check that refuses a change at runtime - does not apply: the
// executor stops, and the OSD goes on.
TEST(OSDOfi, CutoffPolicyIsReadAtInit)
{
  if (!tcp_available()) {
    GTEST_SKIP() << "tcp is not available";
  }
  auto a = arena(1);
  OSDOfi ofi(g_ceph_context, nullptr);
  ASSERT_EQ(0, ofi.init("tcp", loopback(), false, a.get()));
  const size_t n = 4 << 20;
  auto c = stalled(ofi, n);
  ASSERT_TRUE(c.ep);
  ASSERT_FALSE(c.token.empty()) << c.ep->last_error();

  auto& conf = g_ceph_context->_conf;
  auto set_policy = [&conf](const char* policy) {
    conf._clear_safe_to_start_threads();
    conf.set_val_or_die("osd_oob_cutoff_policy", policy);
    conf.set_safe_to_start_threads();
  };
  set_policy("abort");
  auto restore = make_scope_guard([&] {
    set_policy("warn");
    conf.set_val("osd_ofi_inject_cutoff_failure", "false");
  });
  ASSERT_EQ(0, conf.set_val("osd_ofi_inject_cutoff_failure", "true"));
  ceph::buffer::list data;
  data.append_zero(n);
  EXPECT_EQ(-ENOTRECOVERABLE, ofi.execute_plan(
    "obj", c.token, data, ceph::osd::oob::linear_plan(0, n),
    std::chrono::milliseconds(600), nullptr));
  EXPECT_FALSE(ofi.is_available());
}

// A cut-off whose close takes longer than the tolerance, as with a
// provider that drains its writes on close, ends past the tolerance too.
// The executor stops, and raises OOB_DELIVERY_UNSAFE, which says how late
// it ended: not OOB_CUTOFF_LATE, which would say within the tolerance.
TEST(OSDOfi, SlowCutOffRaisesUnsafeNotLate)
{
  if (!tcp_available()) {
    GTEST_SKIP() << "tcp is not available";
  }
  auto a = arena(1);
  OSDOfi ofi(g_ceph_context, nullptr);
  ASSERT_EQ(0, ofi.init("tcp", loopback(), false, a.get()));
  const size_t n = 4 << 20;
  auto c = stalled(ofi, n);
  ASSERT_TRUE(c.ep);
  ASSERT_FALSE(c.token.empty()) << c.ep->last_error();

  auto& conf = g_ceph_context->_conf;
  const auto tolerance = conf.get_val<std::chrono::milliseconds>(
    "osd_oob_cutoff_late_tolerance");
  ASSERT_EQ(0, conf.set_val("osd_ofi_inject_cutoff_delay",
			    std::to_string(tolerance.count() + 500)));
  ceph::buffer::list data;
  data.append_zero(n);
  const ssize_t r = ofi.execute_plan(
    "obj", c.token, data, ceph::osd::oob::linear_plan(0, n),
    std::chrono::milliseconds(600), nullptr);
  conf.set_val("osd_ofi_inject_cutoff_delay", "0");
  // it ended after the write's budget, so it was counted late as well
  EXPECT_EQ(-ENOTRECOVERABLE, r);
  EXPECT_FALSE(ofi.is_available());
  std::map<std::string, std::string> alerts;
  ofi.get_alerts(alerts);
  ASSERT_EQ(1u, alerts.count("OOB_DELIVERY_UNSAFE"));
  EXPECT_NE(std::string::npos, alerts["OOB_DELIVERY_UNSAFE"].find(
	      "took longer than the tolerance"));
  EXPECT_EQ(1u, alerts.size());
}
