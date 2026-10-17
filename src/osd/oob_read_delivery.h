// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <sys/types.h>
#include <utility>
#include <vector>

#include "common/ceph_mutex.h"
#include "common/rdma_token.h"
#include "include/buffer.h"
#include "include/common_fwd.h"
#include "osd/DynamicPerfStats.h"
#include "osd/oob_placement.h"
#include "osd/oob_xfer.h"
#include "osd/osd_types.h"

class MOSDECSubOpReadReply;
class MOSDOpReply;
class OSDOobExecutor;
namespace ceph { class Formatter; }

/**
 * The read push: out-of-band delivery of a read reply's data into the
 * client windows its ops' descriptors name, apart from what needs the
 * PG.
 *
 * Under the PG lock, PrimaryLogPG::deliver_oob_prepare() checks the
 * request, plans each descriptor-bearing op's push (plan_read_push())
 * and fixes the bounds of its transfer. Each push then runs as a
 * push_job, and the reply waits in an oob_reply until every push is
 * over: the last one to finish sends it. With transfer threads the
 * pushes run there, and the op thread lets go of the object and the
 * request once it handed them off (submit_pushes(), hand_off()); the
 * reply leaves only after that, from the transfer thread whose push
 * ends last, or from the op thread when every push ended first. Until
 * it left, an op that may change the object waits (read_fences).
 * Without them, or for an executor whose transfers stay on the op
 * threads, each push runs on the thread that submits it, and the reply
 * leaves before the op thread lets go of the object.
 */
namespace ceph::osd::oob {

/// the stripe geometry the plan of an EC shard-direct read needs; known
/// only when the pool allows direct reads
struct ec_direct_geometry {
  uint64_t chunk_size = 0;
  uint32_t k = 0;
  uint32_t raw_shard = 0;  ///< this OSD's raw (data-order) shard index
};

/**
 * One descriptor-bearing op of a read, ready to push.
 * plan_read_push() fills in the plan; the PG adds the executor, the
 * token and the bounds.
 */
struct read_push {
  size_t idx = 0;  ///< the op's index in the request
  placement_plan plan;
  ceph::buffer::list payload;  ///< the bytes the plan indexes
  /// a SPARSE_READ: a placed one keeps its extent map in the reply
  bool sparse = false;
  std::map<uint64_t, uint64_t> sparse_extents;
  bool want64 = false;   ///< delivery_t::FLAG_CRC64NVME
  bool want32c = false;  ///< delivery_t::FLAG_CRC32C

  OSDOobExecutor* exec = nullptr;
  std::string token;
  bounds_t bounds;
};

/**
 * Whether plan_read_push() can push an op of op_code with descriptor d,
 * from what is known before the op reads: d has no flags we do not
 * implement, the op is a read that carries data (READ, SYNC_READ or
 * SPARSE_READ), and an EC shard-direct read is not sparse and has its
 * geometry (has_ec). An op that passes may still stay inline: when it
 * fails, reads nothing, or its plan places nothing.
 */
bool read_push_possible(int op_code, const ceph::rdma::delivery_t& d,
			bool ec_direct, bool has_ec);

/**
 * The push of read op idx of a request to the window that descriptor d
 * names, or nullopt when the op stays inline: read_push_possible() says
 * no, the op carries no read data, its extent map does not decode, or
 * its plan places nothing, or places other than exactly its payload: all
 * of a placed op's data leaves the reply, so an op whose plan leaves
 * bytes out (a sparse read whose extent map covers less than its data,
 * say) goes inline. ec_direct says the request reads one shard of an EC
 * pool directly.
 */
std::optional<read_push> plan_read_push(
  size_t idx, const OSDOp& op, const ceph::rdma::delivery_t& d,
  bool ec_direct, const std::optional<ec_direct_geometry>& ec);

/// what the push of one op did
struct push_result {
  ssize_t r = -ECANCELED;  ///< the bytes placed, or a negative errno
  /// a write of it was handed to the transport
  bool started = false;
  /// OSDOobExecutor::delivery_complete() of the executor that placed it
  bool delivery_complete = false;
};

/**
 * Put the result of an op's push in the op and its oob result. The op
 * is placed only when the executor placed every byte of the payload. A
 * placed op's data leaves the reply (a sparse read keeps its extent map,
 * with an empty data blob); res gets the bytes placed, landed when the
 * executor is delivery-complete, and the checksums of the placed
 * ranges that p wants. An op that was not placed keeps its data inline,
 * and is declined when nothing of it was started.
 */
void finish_op(OSDOp& op, ceph::rdma::oob_result_t& res,
	       const read_push& p, const push_result& r);

/**
 * The reply to a read whose ops are being pushed. It holds the reply,
 * with its ops and oob results swapped out, until every push is over,
 * and then sends it, exactly once.
 *
 * It counts references: one for the thread that builds it (the
 * guard), and one for each push_job. A job's finish() puts its result
 * in the op's slot, then drops its reference; whichever drop is the
 * last, on whichever thread, puts the ops and results back in the reply
 * and hands it to send. A push touches only its own op's slot, and the
 * builder touches no slot once a job holds a reference.
 */
class oob_reply {
public:
  /// sends the finished reply; consumes the message reference
  using send_fn = std::function<void(MOSDOpReply*)>;

  oob_reply(MOSDOpReply* reply, std::vector<OSDOp>&& rops,
	    std::vector<ceph::rdma::oob_result_t>&& oob, send_fn send);
  ~oob_reply();

  oob_reply(const oob_reply&) = delete;
  oob_reply& operator=(const oob_reply&) = delete;

  /// one more reference, for a push that will finish(); only while
  /// another reference is held, since the last put() sends the reply
  void get();
  /// the push p is over: finish_op() on its op, then put()
  void finish(const read_push& p, const push_result& r);
  /// drop a reference; the last one sends the reply
  void put();

private:
  void send_reply();

  MOSDOpReply* reply;
  std::vector<OSDOp> rops;
  std::vector<ceph::rdma::oob_result_t> oob;
  send_fn send;
  /// the guard, and one for each push not yet over
  std::atomic<unsigned> pending{1};
};

/**
 * The push of one op of a read: execute_plan() into the op's window,
 * then finish() it in the reply. A job that never runs (refused,
 * expired, shed, or dropped) starts nothing, and its op is declined.
 */
class push_job final : public xfer_job {
public:
  /// here: run on the thread that submits it (see inline_only())
  push_job(CephContext* cct, std::shared_ptr<oob_reply> join,
	   std::string key, read_push push, bool here);
  ~push_job() override;

  uint64_t pinned_bytes() const override { return push.payload.length(); }
  ceph::mono_clock::time_point post_by() const override {
    return push.bounds.post_by;
  }
  bool inline_only() const override { return here; }
  void run() override;
  void expire(int r) override;
  void dump(ceph::Formatter* f) const override;

private:
  void finish(const push_result& r);

  CephContext* const cct;
  std::shared_ptr<oob_reply> join;
  const std::string key;  ///< the object's name, for the executor
  const read_push push;
  const bool here;
  bool finished = false;
};

/**
 * Submit a push_job to x for each of the pushes of the reply join waits
 * for; with no x, refuse them all. A push whose executor's transfers
 * stay on the op threads (runs_inline) runs on this thread, and so does
 * every push when x has no threads. A push x refuses is declined, since
 * nothing of it started. Returns true when any push was handed to a
 * transfer thread: the caller then drops its guard with hand_off().
 */
bool submit_pushes(CephContext* cct, Transfers* x,
		   const std::shared_ptr<oob_reply>& join,
		   const std::string& key, std::vector<read_push>&& pushes,
		   const std::function<bool(const OSDOobExecutor*)>& runs_inline);

/**
 * The op thread's end of a read whose pushes submit_pushes() handed
 * off: let_go() releases what the request holds (PrimaryLogPG closes
 * its op context there, and with it the object's lock), and only then
 * is the guard on join dropped. So the reply never leaves before
 * let_go() returned, however soon the pushes end: it leaves from the
 * transfer thread whose push ends last, or from here, when every push
 * ended first.
 */
void hand_off(const std::shared_ptr<oob_reply>& join,
	      const std::function<void()>& let_go);

/**
 * The reply of a read whose pushes submit_pushes() handed off, as the
 * fence of its object sees it (read_fences): out until the reply left.
 * The thread that sends the reply settles it once the messenger has the
 * reply, and then kicks the PG when the PG fenced the object with it:
 * that thread takes no PG lock (PrimaryLogPG::oob_read_kick()).
 */
class read_out {
public:
  /// the PG fenced the object with it: the send of its reply kicks the PG
  void fence() { fenced.store(true, std::memory_order_release); }
  /// the reply left; true when the PG fenced the object with it, and
  /// waits for the kick
  bool settle() {
    done.store(true, std::memory_order_release);
    return fenced.load(std::memory_order_acquire);
  }
  bool settled() const { return done.load(std::memory_order_acquire); }

private:
  std::atomic<bool> fenced{false};
  std::atomic<bool> done{false};
};

/**
 * Whether a client request with ops and flags may change its object, as
 * far as its message says: the client marked it a write
 * (CEPH_OSD_FLAG_WRITE), or one of its ops writes, calls a class method,
 * which can write, or is a cache op.
 */
bool may_change_object(const std::vector<OSDOp>& ops, int flags);

/**
 * The fences of a PG's objects whose reads handed their pushes off. Such
 * a read lets go of its object's lock at the handoff, and its reply
 * leaves only once the pushes are over. A client op that may change the
 * object waits until the reply of each such read of the object left, and
 * so does every op on the object behind one that waits. So the replies
 * leave in the order of the ops, as they do when a read replies before
 * it lets go of the lock. A client that sends the read again, because
 * the PG split or its primary changed while the reply was on its way,
 * gets the object as it was before the ops it sent after the read: the
 * PG dropped those ops from the fence, and the client sends them again
 * after the read.
 *
 * A fence holds ops and the replies they wait for only: no object
 * context and no lock of the object, so that it outlives an interval
 * change, which takes its ops to run them again (take_waiters()). An op
 * that runs again waits again while a reply is out. Op is the PG's
 * OpRequestRef.
 */
template <typename Op>
class read_fences {
public:
  struct fence {
    /// the reads of the object whose replies may not have left
    std::vector<std::shared_ptr<read_out>> out;
    /// the ops that wait, in the order they came
    std::list<Op> waiters;
    utime_t since;  ///< when the first of them came
  };

  bool empty() const { return fences.empty(); }
  const std::map<hobject_t, fence>& get() const { return fences; }

  /// a read of head handed its pushes off; r settles once its reply left
  void add(const hobject_t& head, std::shared_ptr<read_out> r) {
    r->fence();
    fences[head].out.push_back(std::move(r));
  }

  /**
   * Whether op, on head, waits; if so, it waits in the fence of head.
   * It waits when other ops wait there, or when it may change the object
   * (changes) and the reply of a read of head is out.
   */
  bool wait(const hobject_t& head, Op op, bool changes, utime_t now) {
    auto p = fences.find(head);
    if (p == fences.end()) {
      return false;
    }
    auto& f = p->second;
    if (f.waiters.empty()) {
      if (!changes) {
	return false;
      }
      drop_settled(f);
      if (f.out.empty()) {
	// every reply left, and the kicks find no fence
	fences.erase(p);
	return false;
      }
      f.since = now;
    }
    f.waiters.push_back(std::move(op));
    return true;
  }

  /**
   * A reply of a read of head left. Once no reply of a read of head is
   * out, the fence ends, and returns its ops, in order, to run again.
   */
  std::list<Op> kick(const hobject_t& head) {
    std::list<Op> ls;
    auto p = fences.find(head);
    if (p == fences.end()) {
      return ls;
    }
    drop_settled(p->second);
    if (p->second.out.empty()) {
      ls.swap(p->second.waiters);
      fences.erase(p);
    }
    return ls;
  }

  /// the ops of every fence, each fence's in order, to run again; the
  /// fences keep the replies that are out
  std::list<Op> take_waiters() {
    std::list<Op> ls;
    for (auto p = fences.begin(); p != fences.end();) {
      ls.splice(ls.end(), p->second.waiters);
      drop_settled(p->second);
      if (p->second.out.empty()) {
	p = fences.erase(p);
      } else {
	++p;
      }
    }
    return ls;
  }

  void clear() { fences.clear(); }

private:
  static void drop_settled(fence& f) {
    std::erase_if(f.out, [](const auto& r) { return r->settled(); });
  }

  std::map<hobject_t, fence> fences;
};

/**
 * A PG's dynamic perf stats of its pushed reads, with the latency at
 * the reply. A transfer thread may send such a reply, and holds no PG
 * lock, so add() takes the read under a lock of its own. The PG takes
 * what add() took into its own stats at each report of the OSD's perf
 * query results (take()). A reply on its way holds them, so they
 * outlive the PG.
 *
 * They follow the queries of the PG's own stats, which the OSD sets in
 * two ways: on each PG it has when the queries change
 * (set_queries()), and at each report, which hands each PG fresh stats
 * with the OSD's queries, in place of its own. A PG that a split made,
 * or that this OSD made as a replica and that became primary, gets its
 * queries only the second way.
 */
class pg_perf_stats {
public:
  explicit pg_perf_stats(spg_t pgid) : pgid(pgid) {}

  pg_perf_stats(const pg_perf_stats&) = delete;
  pg_perf_stats& operator=(const pg_perf_stats&) = delete;

  void set_queries(const std::list<OSDPerfMetricQuery>& queries);

  /// a pushed read, whose reply left latency after its request arrived
  template <typename Op>
  void add(int osd, const Op& op, uint64_t inb, uint64_t outb,
	   utime_t latency) {
    std::lock_guard l(lock);
    if (stats.is_enabled()) {
      // all that DynamicPerfStats::add() reads of it
      pg_info_t info;
      info.pgid = pgid;
      stats.add(osd, info, op, inb, outb, latency);
      added = true;
    }
  }

  /**
   * At a report: what add() took since the last report, for the PG to
   * merge into its own stats. fresh is the stats that the report hands
   * the PG, with no counts and the OSD's queries, and is not changed.
   * The stats take its queries when add() took a read since the last
   * report, or when one of the two has queries and the other has none.
   * Stats that took nothing keep their queries, and cost the report no
   * copy of them.
   */
  DynamicPerfStats take(DynamicPerfStats& fresh);

private:
  ceph::mutex lock = ceph::make_mutex("pg_perf_stats::lock");
  const spg_t pgid;
  DynamicPerfStats stats;
  /// add() took a read since the last take()
  bool added = false;
};

/**
 * The EC shard push: a shard that the primary lent a gather window
 * (ECSubRead::push_token) writes the data of its sub-read there, rather
 * than return it in its reply.
 *
 * Under the shard's PG lock, ECBackend::prepare_sub_read_push() checks
 * the sub-read, finds the executor, and takes the data read, its plan
 * and the bounds of the transfer; the reply is complete then, but for
 * what the push changes in it. A sub_read_push_job runs the push, puts
 * its outcome in the reply, and only then sends it: the primary reads
 * the window once it has the reply, and not before. With transfer
 * threads the push and the reply leave from one of them, and the op
 * thread goes on at once. Without them, or for an executor whose
 * transfers stay on the op threads, both happen before the submit
 * returns, as they always have.
 */
struct sub_read_push {
  OSDOobExecutor* exec = nullptr;
  std::string token;  ///< the primary's window
  /// what the sub-read read, back to back in reply order: what the
  /// window gets. It shares its buffers with the reply's buffers_read
  ceph::buffer::list all;
  placement_plan plan;
  /// the extents of all, by object, for the reply's pushed
  std::map<hobject_t, std::list<std::pair<uint64_t, uint64_t>>> pushed;
  bounds_t bounds;
};

/**
 * The push of a shard's sub-read into the primary's gather window, then
 * its reply. A push that places every byte returns only the extents and
 * their crc32c; any other outcome leaves the data in buffers_read, as
 * read. The reply says that nothing went into the window
 * (push_declined) when the executor says so, and when the job never ran
 * (refused, expired, shed or dropped): the primary then lends the
 * window again at once.
 */
class sub_read_push_job final : public xfer_job {
public:
  /// sends the finished reply; consumes the message reference
  using send_fn = std::function<void(MOSDECSubOpReadReply*)>;

  /// reply: the sub-read's reply, its reference now the job's, complete
  /// but for the push; here: run on the thread that submits it (see
  /// inline_only())
  sub_read_push_job(CephContext* cct, MOSDECSubOpReadReply* reply,
		    sub_read_push push, send_fn send, bool here);
  ~sub_read_push_job() override;

  sub_read_push_job(const sub_read_push_job&) = delete;
  sub_read_push_job& operator=(const sub_read_push_job&) = delete;

  uint64_t pinned_bytes() const override { return bytes; }
  ceph::mono_clock::time_point post_by() const override {
    return push.bounds.post_by;
  }
  bool inline_only() const override { return here; }
  void run() override;
  void expire(int r) override;
  void dump(ceph::Formatter* f) const override;

private:
  /// the data stays inline, and nothing went into the window
  void decline();
  void send_reply();

  CephContext* const cct;
  MOSDECSubOpReadReply* reply;
  sub_read_push push;
  const send_fn send;
  const bool here;
  /// fixed before the job runs, for the log and dump_oob_transfers: the
  /// PG instance of this shard, which runs the push, where the reply's
  /// pgid names the primary's
  const spg_t pgid;
  const ceph_tid_t tid;
  const uint64_t bytes;
};

} // namespace ceph::osd::oob
