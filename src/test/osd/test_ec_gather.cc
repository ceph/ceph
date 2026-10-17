// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd/ec_gather.h"
#include "osd/ECMsgTypes.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/JSONFormatter.h"
#include "global/global_context.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "osd/oob_executor.h"
#include "osd/oob_read_delivery.h"
#include "osd/oob_xfer.h"

using namespace std::chrono_literals;
using ceph::osd::oob::bounds_t;
using ceph::osd::oob::sub_read_push;
using ceph::osd::oob::sub_read_push_job;
using ceph::osd::oob::take_pushed;
using ceph::osd::oob::Transfers;
using ceph::osd::oob::xfer_job;
using pushed_t =
  std::map<std::string, std::list<std::pair<uint64_t, uint64_t>>>;
using out_t =
  std::map<std::string, std::list<std::pair<uint64_t, ceph::buffer::list>>>;

namespace {

/// A shard's sub-read result, as it pushes it: the extents' data back to
/// back in reply order, gathered from buffers of its own (bluestore hands
/// back many), and the crc32c of the whole.
struct shard_push_t {
  pushed_t pushed;
  std::string data;
  uint32_t crc = 0;

  void add(const std::string& obj, uint64_t offset, const std::string& d) {
    pushed[obj].emplace_back(offset, d.size());
    data += d;
  }
  void seal() {
    ceph::buffer::list all;
    for (size_t o = 0; o < data.size(); o += 1000) {
      all.append(ceph::buffer::copy(data.data() + o,
				    std::min<size_t>(1000, data.size() - o)));
    }
    crc = all.crc32c(-1);
  }
};

std::string pattern(char c, size_t n)
{
  std::string s(n, c);
  for (size_t i = 0; i < n; i += 7) {
    s[i] = static_cast<char>(i);
  }
  return s;
}

shard_push_t two_objects()
{
  shard_push_t p;
  p.add("a", 0, pattern('a', 8192));
  p.add("a", 65536, pattern('b', 4096));
  p.add("c", 4096, pattern('c', 12288));
  p.seal();
  return p;
}

std::string flatten(const out_t& out, const std::string& obj)
{
  std::string s;
  for (const auto& [ofs, bl] : out.at(obj)) {
    s += bl.to_str();
  }
  return s;
}

} // anonymous namespace

TEST(ECGather, TakesWhatTheShardPushed)
{
  auto p = two_objects();
  std::vector<char> win(64 << 10, 'x');
  std::memcpy(win.data(), p.data.data(), p.data.size());
  out_t out;
  auto t = take_pushed(win.data(), win.size(), p.pushed, p.crc, out);
  ASSERT_TRUE(t.ok());
  EXPECT_EQ(p.data.size(), t.bytes);
  EXPECT_EQ(p.crc, t.crc);
  ASSERT_EQ(2u, out.size());
  ASSERT_EQ(2u, out.at("a").size());
  EXPECT_EQ(0u, out.at("a").front().first);
  EXPECT_EQ(65536u, out.at("a").back().first);
  EXPECT_EQ(pattern('a', 8192) + pattern('b', 4096), flatten(out, "a"));
  EXPECT_EQ(4096u, out.at("c").front().first);
  EXPECT_EQ(pattern('c', 12288), flatten(out, "c"));
}

TEST(ECGather, RefusesAWindowThatIsNotThePush)
{
  // a byte of a stray write, the tail of a push that completed short,
  // and another gather's data, in turn: each is found, and nothing of
  // the window is taken
  auto p = two_objects();
  std::vector<char> good(64 << 10, 'x');
  std::memcpy(good.data(), p.data.data(), p.data.size());

  auto stray = good;
  stray[9000] ^= 0x40;
  auto short_push = good;
  std::memset(short_push.data() + 16384, 0, p.data.size() - 16384);
  auto other = good;
  const auto o = pattern('z', p.data.size());
  std::memcpy(other.data(), o.data(), o.size());

  for (auto* w : {&stray, &short_push, &other}) {
    out_t out;
    auto t = take_pushed(w->data(), w->size(), p.pushed, p.crc, out);
    EXPECT_TRUE(t.mismatch);
    EXPECT_FALSE(t.ok());
    EXPECT_NE(p.crc, t.crc);
    EXPECT_TRUE(out.empty());
  }
}

TEST(ECGather, RefusesExtentsPastTheWindow)
{
  auto p = two_objects();
  std::vector<char> win(p.data.size() - 1);
  std::memcpy(win.data(), p.data.data(), win.size());
  out_t out;
  auto t = take_pushed(win.data(), win.size(), p.pushed, p.crc, out);
  EXPECT_TRUE(t.overrun);
  EXPECT_FALSE(t.ok());
  EXPECT_TRUE(out.empty());
}

TEST(ECGather, AppendsToWhatIsThere)
{
  auto p = two_objects();
  std::vector<char> win(64 << 10);
  std::memcpy(win.data(), p.data.data(), p.data.size());
  out_t out;
  ceph::buffer::list keep;
  keep.append("k");
  out["d"].emplace_back(7, keep);
  auto t = take_pushed(win.data(), win.size(), p.pushed, p.crc, out);
  ASSERT_TRUE(t.ok());
  EXPECT_EQ(3u, out.size());
  EXPECT_EQ("k", flatten(out, "d"));
}

// A read whose result goes out of band takes each extent into a buffer
// that make() hands out, one of the OSD's arena, and into the heap when
// make() has none to give
TEST(ECGather, TakesIntoTheBuffersMakeHandsOut)
{
  auto p = two_objects();
  std::vector<char> win(64 << 10);
  std::memcpy(win.data(), p.data.data(), p.data.size());
  std::vector<const char*> handed;
  int calls = 0;
  auto make = [&](uint64_t len) {
    if (++calls == 2) {
      return ceph::buffer::ptr();
    }
    ceph::buffer::ptr bp(ceph::buffer::create(len));
    handed.push_back(bp.c_str());
    return bp;
  };
  out_t out;
  auto t = take_pushed(win.data(), win.size(), p.pushed, p.crc, out, make);
  ASSERT_TRUE(t.ok());
  // in reply order: a at 0, a at 65536 (none handed out), c at 4096
  EXPECT_EQ(3, calls);
  ASSERT_EQ(2u, handed.size());
  EXPECT_EQ(handed[0], out.at("a").front().second.front().c_str());
  EXPECT_NE(handed[1], out.at("a").back().second.front().c_str());
  EXPECT_EQ(handed[1], out.at("c").front().second.front().c_str());
  EXPECT_EQ(pattern('a', 8192) + pattern('b', 4096), flatten(out, "a"));
  EXPECT_EQ(pattern('c', 12288), flatten(out, "c"));

  // a window that is not the push is still refused, whatever make() gave
  auto stray = win;
  stray[9000] ^= 0x40;
  out_t none;
  EXPECT_TRUE(take_pushed(stray.data(), stray.size(), p.pushed, p.crc, none,
			  make).mismatch);
  EXPECT_TRUE(none.empty());
}

// a buffer that make() rounds up holds the extent, and no more: the
// extent's bufferlist is its length
TEST(ECGather, TrimsTheBuffersMakeRoundsUp)
{
  auto p = two_objects();
  std::vector<char> win(64 << 10);
  std::memcpy(win.data(), p.data.data(), p.data.size());
  std::vector<const char*> handed;
  auto make = [&](uint64_t len) {
    ceph::buffer::ptr bp(ceph::buffer::create(len + 4096));
    handed.push_back(bp.c_str());
    return bp;
  };
  out_t out;
  auto t = take_pushed(win.data(), win.size(), p.pushed, p.crc, out, make);
  ASSERT_TRUE(t.ok());
  ASSERT_EQ(3u, handed.size());
  const auto& a = out.at("a");
  ASSERT_EQ(2u, a.size());
  EXPECT_EQ(8192u, a.front().second.length());
  EXPECT_EQ(4096u, a.back().second.length());
  EXPECT_EQ(handed[0], a.front().second.front().c_str());
  EXPECT_EQ(12288u, out.at("c").front().second.length());
  EXPECT_EQ(pattern('a', 8192) + pattern('b', 4096), flatten(out, "a"));
  EXPECT_EQ(pattern('c', 12288), flatten(out, "c"));
}

TEST(ECGather, TakesExtentsOfManySlices)
{
  // an extent is copied and checked a slice at a time: one that ends
  // inside its third slice comes out whole, in one buffer, as does one
  // of no bytes, and a byte changed in any of its slices is found
  using ceph::osd::oob::pushed_slice;
  shard_push_t p;
  const auto big = pattern('a', 2 * pushed_slice + 4099);
  p.add("a", 0, big);
  p.add("a", 1 << 20, "");
  p.add("c", 8192, pattern('c', 5000));
  p.seal();
  std::vector<char> win(4 * pushed_slice, 'x');
  std::memcpy(win.data(), p.data.data(), p.data.size());

  out_t out;
  auto t = take_pushed(win.data(), win.size(), p.pushed, p.crc, out);
  ASSERT_TRUE(t.ok());
  EXPECT_EQ(p.data.size(), t.bytes);
  EXPECT_EQ(p.crc, t.crc);
  ASSERT_EQ(2u, out.at("a").size());
  EXPECT_EQ(1u, out.at("a").front().second.get_num_buffers());
  EXPECT_EQ(big, out.at("a").front().second.to_str());
  EXPECT_EQ(1u << 20, out.at("a").back().first);
  EXPECT_EQ(0u, out.at("a").back().second.length());
  EXPECT_EQ(pattern('c', 5000), flatten(out, "c"));

  for (uint64_t at : {uint64_t(0), pushed_slice - 1, pushed_slice,
		      2 * pushed_slice + 4098}) {
    SCOPED_TRACE("byte " + std::to_string(at));
    auto bad = win;
    bad[at] ^= 0x40;
    out_t o;
    auto b = take_pushed(bad.data(), bad.size(), p.pushed, p.crc, o);
    EXPECT_TRUE(b.mismatch);
    EXPECT_EQ(p.data.size(), b.bytes);
    EXPECT_TRUE(o.empty());
  }
}

TEST(ECGather, RefusesAnExtentNoBufferlistHolds)
{
  // an extent of 4 GiB is refused before any of it is read, even from
  // a window said to hold it
  char win[1] = {};
  pushed_t pushed;
  pushed["a"].emplace_back(0, uint64_t(1) << 32);
  out_t out;
  auto t = take_pushed(win, uint64_t(1) << 33, pushed, 0, out);
  EXPECT_TRUE(t.overrun);
  EXPECT_FALSE(t.ok());
  EXPECT_EQ(0u, t.bytes);
  EXPECT_TRUE(out.empty());
}

TEST(ECGather, TheReplyCarriesTheChecksum)
{
  ECSubReadReply r;
  r.from = pg_shard_t(1, shard_id_t(1));
  r.tid = 7;
  const hobject_t hoid(sobject_t("obj", CEPH_NOSNAP));
  r.pushed[hoid].emplace_back(0, 4096);
  r.pushed_crc = 0xdeadbeef;

  ceph::buffer::list p, d;
  r.encode(p, d, CEPH_FEATURES_ALL);
  p.claim_append(d);
  ECSubReadReply got;
  auto it = std::as_const(p).begin();
  got.decode(it);
  EXPECT_EQ(r.pushed, got.pushed);
  EXPECT_EQ(0xdeadbeefu, got.pushed_crc);

  // to a peer from before pushes, neither goes out
  ceph::buffer::list op, od;
  r.encode(op, od, 0);
  op.claim_append(od);
  ECSubReadReply old;
  old.pushed_crc = 1;
  auto oit = std::as_const(op).begin();
  old.decode(oit);
  EXPECT_TRUE(old.pushed.empty());
  EXPECT_EQ(0u, old.pushed_crc);
}

TEST(ECGather, TheReplySaysWhenNothingWasSent)
{
  // a shard whose executor refused the push before sending anything
  // replies inline and says so, and the primary lends the window again
  // at once
  ECSubReadReply r;
  r.from = pg_shard_t(1, shard_id_t(1));
  r.tid = 8;
  const hobject_t hoid(sobject_t("obj", CEPH_NOSNAP));
  ceph::buffer::list bl;
  bl.append(std::string(4096, 'i'));
  r.buffers_read[hoid].emplace_back(0, bl);
  r.push_declined = true;

  ceph::buffer::list p, d;
  r.encode(p, d, CEPH_FEATURES_ALL);
  ECSubReadReply got;
  auto pit = std::as_const(p).begin();
  auto dit = std::as_const(d).begin();
  got.decode(pit, dit);
  EXPECT_TRUE(got.push_declined);
  EXPECT_TRUE(got.pushed.empty());
  ASSERT_EQ(1u, got.buffers_read[hoid].size());
  EXPECT_TRUE(got.buffers_read[hoid].front().second.contents_equal(bl));

  // a shard that may have written says nothing of the kind
  r.push_declined = false;
  ceph::buffer::list p2, d2;
  r.encode(p2, d2, CEPH_FEATURES_ALL);
  ECSubReadReply wrote;
  wrote.push_declined = true;
  auto p2it = std::as_const(p2).begin();
  auto d2it = std::as_const(d2).begin();
  wrote.decode(p2it, d2it);
  EXPECT_FALSE(wrote.push_declined);

  // to a peer from before pushes, it does not go out
  r.push_declined = true;
  ceph::buffer::list op, od;
  r.encode(op, od, 0);
  op.claim_append(od);
  ECSubReadReply old;
  old.push_declined = true;
  auto oit = std::as_const(op).begin();
  old.decode(oit);
  EXPECT_FALSE(old.push_declined);
}

namespace {

/// what a reply holds up to v3, as Tentacle encodes it, with nothing read
void encode_v3_fields(ceph::buffer::list& p, ceph_tid_t tid)
{
  encode(pg_shard_t(1, shard_id_t(1)), p);
  encode(tid, p);
  encode(__u32(0), p);  // buffers_read, encoded by hand from v2
  encode(std::map<hobject_t,
		  std::map<std::string, ceph::buffer::list, std::less<>>>(),
	 p);  // attrs_read
  encode(std::map<hobject_t, int>(), p);  // errors
  encode(std::map<hobject_t, ceph::buffer::list>(), p);  // omap headers
  encode(std::map<hobject_t, std::map<std::string, ceph::buffer::list>>(),
	 p);  // omap entries
  encode(std::map<hobject_t, bool>(), p);  // omaps_complete
}

} // anonymous namespace

TEST(ECGather, AV3ReplyPushedNothing)
{
  // a shard from before pushes (v3) neither pushed nor declined: its
  // reply decodes so, whatever the reply decoded into held before
  ceph::buffer::list p;
  ENCODE_START(3, 2, p);
  encode_v3_fields(p, 9);
  ENCODE_FINISH(p);

  ECSubReadReply got;
  got.pushed[hobject_t(sobject_t("obj", CEPH_NOSNAP))].emplace_back(0, 1);
  got.pushed_crc = 1;
  got.push_declined = true;
  ceph::buffer::list d;
  auto pit = std::as_const(p).begin();
  auto dit = std::as_const(d).begin();
  got.decode(pit, dit);
  EXPECT_EQ(9u, got.tid);
  EXPECT_TRUE(got.pushed.empty());
  EXPECT_EQ(0u, got.pushed_crc);
  EXPECT_FALSE(got.push_declined);
}

TEST(ECGather, V4AddsThePushFields)
{
  // v4 follows v3 with the pushed extents, their crc32c and whether the
  // push was declined, in that order. Any change here is a wire format
  // change and needs a version bump
  const hobject_t hoid(sobject_t("obj", CEPH_NOSNAP));
  ECSubReadReply r;
  r.from = pg_shard_t(1, shard_id_t(1));
  r.tid = 9;
  r.pushed[hoid].emplace_back(0, 4096);
  r.pushed_crc = 0xfeedu;
  r.push_declined = true;
  ceph::buffer::list p, d;
  r.encode(p, d, CEPH_FEATURES_ALL);

  ceph::buffer::list want;
  ENCODE_START(4, 2, want);
  encode_v3_fields(want, 9);
  encode(r.pushed, want);
  encode(uint32_t(0xfeedu), want);
  encode(true, want);
  ENCODE_FINISH(want);
  EXPECT_TRUE(p.contents_equal(want))
    << "ECSubReadReply layout changed; this is a wire format break";

  ECSubReadReply got;
  auto pit = std::as_const(p).begin();
  auto dit = std::as_const(d).begin();
  got.decode(pit, dit);
  EXPECT_EQ(r.pushed, got.pushed);
  EXPECT_EQ(0xfeedu, got.pushed_crc);
  EXPECT_TRUE(got.push_declined);
}

// the log line of a sub-read says whether it lends a gather window, and
// which one; an ordinary sub-read prints as it always has
TEST(ECGather, ASubReadPrintsItsWindow)
{
  ECSubRead r;
  r.tid = 7;
  std::ostringstream inline_read;
  inline_read << r;
  EXPECT_EQ(std::string::npos, inline_read.str().find("push_token"))
    << inline_read.str();
  r.push_token = "gather-window-token";
  std::ostringstream pushed;
  pushed << r;
  EXPECT_NE(std::string::npos,
	    pushed.str().find(", push_token=gather-window-token)"))
    << pushed.str();
}

/*
 * The shard's end of a gather on the OSD's transfer threads
 * (sub_read_push_job): the reply leaves only once the push is over, and
 * says what the push did, as the shard's op thread said it before the
 * push moved off it. A push that starts nothing - refused, expired,
 * shed, or failed before a write went out - leaves the data inline and
 * says so; one that fails after a write went out leaves the data inline
 * and does not.
 */
namespace {

/// what a shard's executor does with a push
enum class outcome {
  placed,          ///< every byte reached the window
  failed_started,  ///< failed after a write went out
  failed_unstarted ///< failed before anything went out
};

/// a shard's executor, pushing into a gather window it keeps; with
/// hold, each push waits in it until release()
struct shard_exec : OSDOobExecutor {
  outcome how = outcome::placed;
  bool hold = false;
  std::string window = std::string(64 << 10, 'x');
  std::atomic<int> calls{0};
  std::atomic<int64_t> budget_ms{-1};

  std::mutex m;
  std::condition_variable cv;
  bool entered = false;
  bool released = false;
  std::thread::id ran_on;

  bool is_available() const override { return true; }
  bool handles(const std::string&) const override { return true; }
  void dump_stats(ceph::Formatter*) const override {}

  ssize_t execute_plan(const std::string& key, const std::string& token,
		       const ceph::buffer::list& data,
		       const ceph::osd::oob::placement_plan& plan,
		       std::chrono::milliseconds budget,
		       bool* started) override {
    calls++;
    budget_ms = budget.count();
    std::unique_lock l(m);
    ran_on = std::this_thread::get_id();
    entered = true;
    cv.notify_all();
    cv.wait(l, [this] { return !hold || released; });
    if (how == outcome::failed_unstarted) {
      *started = false;
      return -ETIMEDOUT;
    }
    *started = true;
    ssize_t total = 0;
    for (const auto& t : plan) {
      ceph_assert(t.client_ofs + t.len <= window.size());
      // a failed push got half of its first range out
      const uint64_t len = how == outcome::placed ? t.len : t.len / 2;
      ceph::buffer::list part;
      part.substr_of(data, t.local_ofs, len);
      part.begin().copy(len, window.data() + t.client_ofs);
      if (how != outcome::placed) {
	return -EIO;
      }
      total += len;
    }
    return total;
  }

  /// up to 10 s for a push to reach the executor
  bool wait_entered() {
    std::unique_lock l(m);
    return cv.wait_for(l, 10s, [this] { return entered; });
  }
  void release() {
    std::lock_guard l(m);
    released = true;
    cv.notify_all();
  }
};

hobject_t hobj(const std::string& name)
{
  return hobject_t(sobject_t(name, CEPH_NOSNAP));
}

ceph::buffer::list bl_of(const std::string& s)
{
  ceph::buffer::list bl;
  bl.append(s);
  return bl;
}

using buffers_t =
  std::map<hobject_t, std::list<std::pair<uint64_t, ceph::buffer::list>>>;

/// what a shard read for a sub-read of two objects, as handle_sub_read()
/// leaves it in the reply
buffers_t shard_read()
{
  buffers_t b;
  b[hobj("a")].emplace_back(0, bl_of(pattern('a', 8192)));
  b[hobj("a")].emplace_back(65536, bl_of(pattern('b', 4096)));
  b[hobj("c")].emplace_back(4096, bl_of(pattern('c', 12288)));
  return b;
}

/// the reply to sub-read tid, complete but for the push
MOSDECSubOpReadReply* sub_read_reply(ceph_tid_t tid)
{
  auto* r = new MOSDECSubOpReadReply;
  r->pgid = spg_t(pg_t(1, 2), shard_id_t(0));
  r->map_epoch = 17;
  r->op.from = pg_shard_t(1, shard_id_t(1));
  r->op.tid = tid;
  r->op.buffers_read = shard_read();
  // as ECBackend::prepare_sub_read_push() leaves it
  r->op.push_declined = true;
  return r;
}

/// the push ECBackend::prepare_sub_read_push() takes of r
sub_read_push push_of(const ECSubReadReply& r, OSDOobExecutor* exec,
		      const bounds_t& b)
{
  sub_read_push p;
  for (const auto& [hoid, extents] : r.buffers_read) {
    for (const auto& [offset, bl] : extents) {
      p.pushed[hoid].emplace_back(offset, bl.length());
      p.all.append(bl);
    }
  }
  p.exec = exec;
  p.token = "window";
  p.plan = ceph::osd::oob::linear_plan(0, p.all.length());
  p.bounds = b;
  return p;
}

/// may start for 5 s, and land for 8
bounds_t far_bounds()
{
  const auto now = ceph::mono_clock::now();
  return {now + 5s, now + 8s};
}

/// could start until a second ago
bounds_t past_bounds()
{
  const auto now = ceph::mono_clock::now();
  return {now - 1s, now + 3s};
}

/// the replies a job sent
struct sent_t {
  std::mutex m;
  std::vector<boost::intrusive_ptr<MOSDECSubOpReadReply>> replies;
  std::thread::id on;

  size_t count() {
    std::lock_guard l(m);
    return replies.size();
  }
  /// up to 10 s for a reply
  bool wait() {
    for (int i = 0; i < 10000 && !count(); i++) {
      std::this_thread::sleep_for(1ms);
    }
    return count() > 0;
  }
  ECSubReadReply& op() {
    std::lock_guard l(m);
    ceph_assert(replies.size() == 1);
    return replies.front()->op;
  }
};

sub_read_push_job::send_fn sender(sent_t& sent)
{
  return [&sent](MOSDECSubOpReadReply* r) {
    std::lock_guard l(sent.m);
    sent.replies.emplace_back(r, false);
    sent.on = std::this_thread::get_id();
  };
}

std::unique_ptr<xfer_job> new_job(shard_exec& e, ceph_tid_t tid,
				  const bounds_t& b, sent_t& sent,
				  bool here = false)
{
  auto* r = sub_read_reply(tid);
  auto push = push_of(r->op, &e, b);
  return std::make_unique<sub_read_push_job>(g_ceph_context, r,
					     std::move(push), sender(sent),
					     here);
}

std::unique_ptr<Transfers> service(unsigned threads, unsigned max_jobs = 64)
{
  auto x = std::make_unique<Transfers>(g_ceph_context, threads, max_jobs,
				       1 << 30, 30s, [] {});
  x->start();
  return x;
}

bool same_buffers(const buffers_t& a, const buffers_t& b)
{
  if (a.size() != b.size()) {
    return false;
  }
  for (auto i = a.begin(), j = b.begin(); i != a.end(); ++i, ++j) {
    if (i->first != j->first || i->second.size() != j->second.size()) {
      return false;
    }
    for (auto x = i->second.begin(), y = j->second.begin();
	 x != i->second.end(); ++x, ++y) {
      if (x->first != y->first || !x->second.contents_equal(y->second)) {
	return false;
      }
    }
  }
  return true;
}

/// the reply carries what the shard read inline, and pushed nothing
void expect_inline(ECSubReadReply& op)
{
  EXPECT_TRUE(same_buffers(shard_read(), op.buffers_read));
  EXPECT_TRUE(op.pushed.empty());
}

} // anonymous namespace

// a push that placed everything returns only the extents and their
// checksum, which is what the primary then finds in its window, from a
// transfer thread when there are some, and before the submit returns
// when there are none
TEST(ECGatherPush, APlacedPushReturnsTheExtents)
{
  const auto me = std::this_thread::get_id();
  for (unsigned threads : {0u, 2u}) {
    SCOPED_TRACE("threads " + std::to_string(threads));
    shard_exec e;
    sent_t sent;
    auto x = service(threads);
    auto job = new_job(e, 7, far_bounds(), sent);
    ASSERT_EQ(0, x->submit(job));
    if (threads == 0) {
      EXPECT_EQ(1u, sent.count());
    }
    ASSERT_TRUE(sent.wait());
    x->stop();
    ASSERT_EQ(1u, sent.count());
    EXPECT_EQ(1, e.calls);
    EXPECT_EQ(threads == 0, sent.on == me);
    EXPECT_EQ(threads == 0, e.ran_on == me);
    // the budget runs to land_by
    EXPECT_GT(e.budget_ms, 7000);
    EXPECT_LE(e.budget_ms, 8000);

    auto& op = sent.op();
    EXPECT_EQ(7u, op.tid);
    EXPECT_FALSE(op.push_declined);
    EXPECT_TRUE(op.buffers_read.empty());
    std::map<hobject_t, std::list<std::pair<uint64_t, uint64_t>>> extents;
    ceph::buffer::list all;
    for (const auto& [hoid, ext] : shard_read()) {
      for (const auto& [offset, bl] : ext) {
	extents[hoid].emplace_back(offset, bl.length());
	all.append(bl);
      }
    }
    EXPECT_EQ(extents, op.pushed);
    EXPECT_EQ(all.crc32c(-1), op.pushed_crc);

    // and the primary takes the shard's buffers back out of its window
    buffers_t out;
    auto t = take_pushed(e.window.data(), e.window.size(), op.pushed,
			 op.pushed_crc, out);
    EXPECT_TRUE(t.ok());
    EXPECT_EQ(all.length(), t.bytes);
    EXPECT_TRUE(same_buffers(shard_read(), out));
  }
}

// a push that failed leaves what the shard read in the reply, as read,
// and says it sent nothing into the window only when the executor says
// so: after a write went out, the primary keeps the window out of use
TEST(ECGatherPush, AFailedPushLeavesTheDataInline)
{
  for (unsigned threads : {0u, 2u}) {
    for (auto how : {outcome::failed_started, outcome::failed_unstarted}) {
      const bool started = how == outcome::failed_started;
      SCOPED_TRACE("threads " + std::to_string(threads) +
		   (started ? " started" : " not started"));
      shard_exec e;
      e.how = how;
      sent_t sent;
      auto x = service(threads);
      auto job = new_job(e, 8, far_bounds(), sent);
      ASSERT_EQ(0, x->submit(job));
      ASSERT_TRUE(sent.wait());
      x->stop();
      ASSERT_EQ(1u, sent.count());
      EXPECT_EQ(1, e.calls);
      auto& op = sent.op();
      EXPECT_EQ(!started, op.push_declined);
      expect_inline(op);
    }
  }
}

// a push that a transfer thread takes past its post_by expires: it never
// reaches the executor, and the reply goes inline, saying so
TEST(ECGatherPush, AnExpiredPushNeverStarts)
{
  shard_exec e;
  sent_t sent;
  auto x = service(1);
  const auto expired = x->get_logger()->get(
    ceph::osd::oob::l_oob_xfer_expired);
  auto job = new_job(e, 9, past_bounds(), sent);
  ASSERT_EQ(0, x->submit(job));
  ASSERT_TRUE(sent.wait());
  x->stop();
  ASSERT_EQ(1u, sent.count());
  EXPECT_EQ(0, e.calls);
  EXPECT_EQ(expired + 1,
	    x->get_logger()->get(ceph::osd::oob::l_oob_xfer_expired));
  auto& op = sent.op();
  EXPECT_TRUE(op.push_declined);
  expect_inline(op);
}

// handed to a transfer thread, the reply waits for the push: the primary
// reads its window once the reply is there, and not before
TEST(ECGatherPush, TheReplyWaitsForThePush)
{
  const auto me = std::this_thread::get_id();
  shard_exec e;
  e.hold = true;
  sent_t sent;
  auto x = service(1);
  auto job = new_job(e, 10, far_bounds(), sent);
  ASSERT_EQ(0, x->submit(job));
  ASSERT_TRUE(e.wait_entered());
  std::this_thread::sleep_for(100ms);
  EXPECT_EQ(0u, sent.count());
  e.release();
  ASSERT_TRUE(sent.wait());
  x->stop();
  EXPECT_NE(me, e.ran_on);
  EXPECT_NE(me, sent.on);
  auto& op = sent.op();
  EXPECT_FALSE(op.push_declined);
  EXPECT_TRUE(op.buffers_read.empty());
  EXPECT_FALSE(op.pushed.empty());
}

// an executor whose transfers stay on the op threads pushes on the
// thread that submits, with transfer threads too, and the reply leaves
// before the submit returns
TEST(ECGatherPush, AnInlineExecutorPushesOnTheOpThread)
{
  const auto me = std::this_thread::get_id();
  shard_exec e;
  sent_t sent;
  auto x = service(2);
  auto job = new_job(e, 11, far_bounds(), sent, true);
  ASSERT_EQ(0, x->submit(job));
  EXPECT_EQ(1u, sent.count());
  x->stop();
  EXPECT_EQ(me, e.ran_on);
  EXPECT_EQ(me, sent.on);
  EXPECT_FALSE(sent.op().push_declined);
}

// a push the service refuses, sheds when it stops, or that is dropped
// without running started nothing: each reply goes inline, saying so,
// and the push that was running still ends with its own reply
TEST(ECGatherPush, ARefusedOrShedPushIsDeclined)
{
  shard_exec e;
  e.hold = true;
  sent_t running, queued, refused;
  auto x = service(1, 2);
  auto j1 = new_job(e, 12, far_bounds(), running);
  ASSERT_EQ(0, x->submit(j1));
  ASSERT_TRUE(e.wait_entered());
  auto j2 = new_job(e, 13, far_bounds(), queued);
  ASSERT_EQ(0, x->submit(j2));

  // over the cap: the job comes back untouched, and the caller answers
  // it inline, as ECBackend does
  auto j3 = new_job(e, 14, far_bounds(), refused);
  const int r = x->submit(j3);
  ASSERT_EQ(-EBUSY, r);
  ASSERT_TRUE(j3);
  EXPECT_EQ(0u, refused.count());
  j3->expire(r);
  ASSERT_EQ(1u, refused.count());
  EXPECT_TRUE(refused.op().push_declined);
  expect_inline(refused.op());
  j3.reset();
  EXPECT_EQ(1u, refused.count());

  // stopping sheds the queued job, then waits for the running one
  std::thread stopper([&x] { x->stop(); });
  ASSERT_TRUE(queued.wait());
  EXPECT_EQ(0u, running.count());
  e.release();
  stopper.join();
  ASSERT_EQ(1u, running.count());
  EXPECT_EQ(1, e.calls);
  EXPECT_EQ(12u, running.op().tid);
  EXPECT_FALSE(running.op().push_declined);
  EXPECT_EQ(13u, queued.op().tid);
  EXPECT_TRUE(queued.op().push_declined);
  expect_inline(queued.op());

  // a job dropped without running sends its reply too
  sent_t dropped;
  new_job(e, 15, far_bounds(), dropped).reset();
  ASSERT_EQ(1u, dropped.count());
  EXPECT_EQ(1, e.calls);
  EXPECT_TRUE(dropped.op().push_declined);
  expect_inline(dropped.op());
}

// dump_oob_transfers names the PG instance of the shard that runs the
// push, 2.1s1, not that of the primary the reply goes to, 2.1s0
TEST(ECGatherPush, TheJobNamesTheShardsPG)
{
  shard_exec e;
  sent_t sent;
  auto job = new_job(e, 16, far_bounds(), sent);
  ceph::JSONFormatter f;
  f.open_object_section("job");
  job->dump(&f);
  f.close_section();
  std::ostringstream ss;
  f.flush(ss);
  EXPECT_NE(std::string::npos, ss.str().find("\"pgid\":\"2.1s1\""))
    << ss.str();
  job.reset();
  ASSERT_EQ(1u, sent.count());
  EXPECT_EQ(0, e.calls);
}
