// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <array>
#include <cerrno>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/ceph_mutex.h"
#include "common/rdma_token.h"
#include "include/buffer.h"
#include "include/common_fwd.h"
#include "osd/oob_xfer.h"
#include "osd/osd_types.h"

class OSDOobExecutor;
class Throttle;
struct OpRequest;
namespace ceph { class Formatter; }

/**
 * The pulled write: the primary takes the payload of each write that
 * carries a pull descriptor (delivery_t::FLAG_PULL) out of the client's
 * window, and the write then runs as if the client had sent the bytes.
 *
 * Under the PG lock, with the object's lock held, pull_payloads()
 * checks every pull of the request before any of them starts, charges
 * their bytes to the client message throttle once, and creates the
 * request's pull_set (OpRequest::oob_pull). A pull_job runs the pulls
 * one after another, with the checksums their descriptors ask for, and
 * settles the set; the op thread then takes the payloads into the ops.
 * The set stays with the request: a run of the request after the pulls
 * started finds it, and neither pulls nor charges the throttle again.
 *
 * Without transfer threads the job runs on the thread that submits it,
 * and a pull holds its op thread, the PG and the object's lock until it
 * settles; so does a pull of a request that a coroutine runs, and one
 * whose executor's transfers stay on the op threads. With them the job runs on a transfer thread, and the request
 * parks: the PG lets go of the object's lock and of the request's
 * context, and keeps only a hold on the object, where the client ops on
 * it wait behind the request (PrimaryLogPG::oob_pull_holds). The
 * transfer thread that settles the set queues a kick to the PG
 * (PGOobPullKick), and takes no PG lock; the PG then runs the request
 * again, with the ops behind it, and that run consumes the set.
 */
namespace ceph::osd::oob {

/// the pull of one write, checked and fixed under the PG lock
struct pull_op {
  size_t idx = 0;  ///< the op's index in the request
  uint64_t len = 0;  ///< the payload's bytes
  uint64_t base_offset = 0;  ///< where the payload starts in the window
  bool want64 = false;   ///< delivery_t::FLAG_CRC64NVME
  bool want32c = false;  ///< delivery_t::FLAG_CRC32C
  OSDOobExecutor* exec = nullptr;
  std::string token;
};

/// what the pull of one write did
struct pull_result {
  size_t idx = 0;  ///< the op's index in the request
  /// 0 when the payload is here, else the errno that fails the request
  int r = -ECANCELED;
  /// a read of it was handed to the transport
  bool started = false;
  /// the payload, when r is 0
  ceph::buffer::list payload;
  /// its byte count and checksums, for the reply (OpRequest::oob_pulled)
  ceph::rdma::oob_result_t oob;
};

/// where a request waits for its pulls: the PG to kick once they
/// settled, so that it runs the request again
struct kick_target {
  spg_t pgid;
  /// the head of the request's object, where it holds the client ops
  hobject_t head;
  /// the PG's map epoch when the request parked
  epoch_t epoch = 0;
};

/**
 * The pulls of one request, and what they did. Created when a run of
 * the request starts them, kept with the request (OpRequest::oob_pull)
 * and never replaced. The job that runs the pulls settles it; the op
 * thread then takes the payloads (consume()), under the PG lock.
 *
 * A run of the request that waits for the set parks it (park()), under
 * the PG lock, and the settle reads where it parked under the set's
 * mutex: either the set settled first, and the run goes on with it, or
 * the settle names the PG to kick, once. So no kick is lost, and none
 * comes for a run that did not wait. A later attempt of the request
 * may wait for the set too, in its own PG (pulls_out): each attempt
 * that parks keeps its own place, and the settle kicks each.
 */
class pull_set {
public:
  /// pulled: the index of each op that the request's pulls are for, in
  /// op order
  explicit pull_set(std::vector<size_t> pulled);
  pull_set(const pull_set&) = delete;
  pull_set& operator=(const pull_set&) = delete;

  /// the job is over: every pull completed, failed, or never started
  bool settled() const;
  /**
   * Under the PG lock, by a run of who, an attempt of the request, that
   * is to wait for the set: its settle kicks target. A park by a later
   * run of who, after an interval change, replaces who's target, and
   * leaves those of other attempts as they are. Returns false, and sets
   * nothing, when the set settled already: the run goes on.
   */
  bool park(const OpRequest* who, const kick_target& target);
  /**
   * The request that owns the set went away (OpRequest::_unregistered()):
   * nothing can run or answer it any more, whatever dropped it. A pull
   * of the job that has not started never will, and the settle does not
   * kick where who waited; another attempt that waits for the set still
   * gets its kick. Nothing consumes the payloads any more: they go now,
   * or at the settle, and not with the last attempt that waits for the
   * set, which no throttle or cap counts them against. Returns true when
   * the set had not settled. Inline, as the request's teardown is in the
   * common library
   */
  bool abandon(const OpRequest* who) {
    std::lock_guard l(lock);
    dropped = true;
    std::erase_if(targets, [who](const auto& t) { return t.first == who; });
    if (state == state_t::settled) {
      drop_payloads();
    }
    return state == state_t::in_flight;
  }
  /**
   * who, a later attempt of the request that waits for the set
   * (wait_for_earlier()), waits for it no more: it went away
   * (OpRequest::_unregistered()), or waits for another set. Its settle
   * kicks nothing for who, and keeps no place of it, which a new
   * request could take the address of. Inline, as abandon()
   */
  void leave(const OpRequest* who) {
    std::lock_guard l(lock);
    std::erase_if(targets, [who](const auto& t) { return t.first == who; });
  }
  bool abandoned() const;
  /// the ops took the payloads: a later run of the request takes the
  /// ops as they are, which the request may have changed since
  bool consumed() const;
  /**
   * A reply that fails the request says nothing of a pull that reached
   * the transport, as it would not of a push cut off: clear the
   * declined flag of each such op in oob, the request's oob results.
   * Nothing of the others read the window, and they stay declined.
   * Until the set settles, any of its pulls may be reading a window:
   * none of its ops stays declined.
   */
  void clear_declined(std::vector<ceph::rdma::oob_result_t>& oob) const;
  /// the job's end, exactly once: the result of each pull, in op order.
  /// Returns the PGs to kick: where each attempt of the request that
  /// parked to wait for the set waits, unless it went away since
  std::vector<kick_target> settle(std::vector<pull_result>&& r);
  /**
   * Under the PG lock, once settled; all or nothing. When every pull
   * completed, move each payload into its op's indata and its result
   * into pulled, which is aligned with ops, put the record of the
   * result in the op's outdata when it takes no more than max_record
   * bytes (encode_pull_record()), and return 0. Else return the errno
   * of the first pull that failed, and leave ops and pulled as they
   * are.
   */
  int consume(std::vector<OSDOp>& ops,
	      std::vector<ceph::rdma::oob_result_t>& pulled,
	      uint64_t max_record);

private:
  enum class state_t {
    in_flight,
    settled,
    consumed,
  };

  /// under lock, once settled, for a set whose request went away: free
  /// the payloads, and fail a consume() as a pull of the set that
  /// starts then fails
  void drop_payloads() {
    for (auto& r : results) {
      r.payload.clear();
      if (r.r == 0) {
	r.r = -ESHUTDOWN;
      }
    }
  }

  mutable ceph::mutex lock = ceph::make_mutex("oob::pull_set::lock");
  const std::vector<size_t> pulled;
  state_t state = state_t::in_flight;
  std::vector<pull_result> results;
  /// where each attempt of the request that waits for the set waits; an
  /// attempt is never dereferenced, only told apart from the others
  std::vector<std::pair<const OpRequest*, kick_target>> targets;
  bool dropped = false;
};

/// queues the kick of a PG where a request waits for its pulls, from
/// the thread that settled them; holds the request
using kick_fn = std::function<void(const kick_target&)>;

/**
 * Under the PG lock: op, a later attempt of a request, waits at target
 * for earlier, the pulls of an earlier attempt of it that are out
 * (pulls_out), as pull_set::park() says. It leaves earlier once it goes
 * away (OpRequest::oob_pull_waits). A run of op waits nowhere else: it
 * leaves the set it waited for before, if another. Returns false, and
 * parks nothing, when earlier settled already.
 */
bool wait_for_earlier(OpRequest& op, const std::shared_ptr<pull_set>& earlier,
		      const kick_target& target);

/**
 * The sets whose pulls run on a transfer thread, by the request they
 * are for, from just before the job is submitted until it settled; one
 * for all the PGs of the OSD. A PG may drop an attempt of a request
 * while its pulls still read the client's window (an interval change,
 * a split, a reset session), and the client then sends the request
 * again, to the same PG or another of the OSD. Until those pulls
 * settled the later attempt waits for them: no other pull of the window
 * starts, and nothing answers the request
 * (PrimaryLogPG::maybe_wait_for_oob_pull()). Every pulled write of every
 * op shard looks here, and every transfer thread, so the sets are spread
 * over shards by their request, each behind its own lock.
 */
class pulls_out {
public:
  void add(const osd_reqid_t& reqid, std::shared_ptr<pull_set> set);
  /// set, which add() took for reqid, settled
  void remove(const osd_reqid_t& reqid, const pull_set* set);
  /// a set of reqid that its job had not removed, or null. It may
  /// settle at any time, after which it parks nothing (pull_set::park())
  std::shared_ptr<pull_set> find(const osd_reqid_t& reqid) const;
  size_t size() const;

private:
  struct shard_t {
    mutable ceph::mutex lock = ceph::make_mutex("oob::pulls_out::lock");
    std::multimap<osd_reqid_t, std::shared_ptr<pull_set>> sets;
  };
  static constexpr size_t SHARDS = 16;
  shard_t& shard_of(const osd_reqid_t& reqid) const {
    return shards[std::hash<osd_reqid_t>{}(reqid) % SHARDS];
  }
  mutable std::array<shard_t, SHARDS> shards;
};

/**
 * The pulls of one request: execute_pull() of each in op order, out of
 * its window, then the checksums its descriptor asks for, and at last
 * the set settles. All or nothing: after a pull that failed, or one
 * that could not land by post_by any more, no later pull starts. A job
 * that never runs (refused, expired, shed, or dropped) starts nothing,
 * and fails the request with -EOPNOTSUPP, on which the client sends the
 * payloads inline; so does a pull that would start past post_by, the
 * pool's lease since the request's receipt. Nor does a pull of a
 * set whose request went away start: it fails with -ESHUTDOWN. Once the
 * set settled, the job kicks each PG where an attempt of the request
 * waits for it.
 */
class pull_job final : public xfer_job {
public:
  /// pulls: the request's, in op order; bounds: from the request's
  /// receipt, as those of a read's pushes; here: run on the thread that
  /// submits it (see inline_only()); kick: what the settle does with
  /// where each attempt of the request waits; out: where set was added
  /// for reqid, which the settle removes it from, or null
  pull_job(CephContext* cct, std::shared_ptr<pull_set> set, std::string key,
	   std::vector<pull_op> pulls, bounds_t bounds, bool here,
	   kick_fn kick = nullptr, pulls_out* out = nullptr,
	   osd_reqid_t reqid = {});
  ~pull_job() override;

  pull_job(const pull_job&) = delete;
  pull_job& operator=(const pull_job&) = delete;

  uint64_t pinned_bytes() const override { return bytes; }
  ceph::mono_clock::time_point post_by() const override {
    return bounds.post_by;
  }
  bool pulls() const override { return true; }
  bool inline_only() const override { return here; }
  void run() override;
  void expire(int r) override;
  void dump(ceph::Formatter* f) const override;

private:
  /// the pull of p, into res
  void pull(const pull_op& p, pull_result& res);
  /// nothing started: every pull fails with r
  void fail(int r);
  void settle(std::vector<pull_result>&& r);

  CephContext* const cct;
  const std::shared_ptr<pull_set> set;
  const std::string key;  ///< the object's name, for the executor
  const std::vector<pull_op> ops;
  const bounds_t bounds;
  const bool here;
  const kick_fn kick;
  pulls_out* const out;
  const osd_reqid_t reqid;
  const uint64_t bytes;  ///< the payloads, together
  bool settled = false;
};

/// what the pull step of a request takes from the PG and the OSD
struct pull_env {
  CephContext* cct = nullptr;
  /// the transfer service, or null, which refuses the pulls
  Transfers* xfer = nullptr;
  /// the executor that can pull from a token's window, or null
  std::function<OSDOobExecutor*(const std::string&)> puller_for;
  /// the throttle of the client's messages, which the payloads are
  /// charged to, or null; asked only of a request that pulls
  std::function<Throttle*()> client_throttle;
  /// osd_max_write_op_reply_len: the most an op's outdata may hold, as
  /// the record of its pull does when it fits (pull_set::consume())
  uint64_t max_reply = 0;
  /// the pool's delivery lease and drain, which bound the pulls from
  /// the request's receipt (OpRequest::get_mono_recv_stamp()), as they
  /// bound a read's pushes
  double lease = 0;
  double drain = 0;
  /// true when an executor's transfers stay on the thread that submits
  /// them (OSDService::oob_runs_inline()), or null for none
  std::function<bool(const OSDOobExecutor*)> runs_inline;
  /// the pulls run on this thread, whatever threads the service has: a
  /// request that a coroutine runs
  bool here = false;
  /// where the request waits when its pulls run on a transfer thread
  kick_target target;
  /// queues the kick of target, once the pulls the request waits for
  /// settled; null when nothing waits for them
  kick_fn kick;
  /// the OSD's sets whose pulls run on a transfer thread, which a set
  /// of the request joins while its pulls run there; null for none
  pulls_out* out = nullptr;
};

/// pull_payloads(): the pulls run on a transfer thread, and the request
/// waits for them; pull_env::kick runs once they settled
inline constexpr int PULLS_PARKED = 1;

/**
 * The pull step of a write request, under the PG lock, with the
 * object's lock held, before the request runs. For each op in ops, the
 * ops the request runs, that carries a pull descriptor: take its
 * payload out of the client's window into its indata, and its byte
 * count and checksums into OpRequest::oob_pulled, for the reply. The
 * first run of the request checks every pull, and fails the request
 * with -EOPNOTSUPP, pulling nothing, past the pool's lease since its
 * receipt, so that the client can send the payloads inline; then it
 * charges the payloads to the client throttle, then starts them. A
 * later run finds what they did in OpRequest::oob_pull, and pulls
 * nothing. Returns 0, or a negative errno that fails the whole request,
 * with nothing written, or PULLS_PARKED when the pulls run on a
 * transfer thread: the request then waits at env.target, holding
 * nothing of the object, and runs again once env.kick kicked the PG. A
 * run of a request whose pulls are still out does not come here: it
 * waits for them first (pull_set::park()), as does a run of a later
 * attempt of the request while those of an earlier one are
 * (pulls_out).
 */
int pull_payloads(const pull_env& env, OpRequest& op,
		  std::vector<OSDOp>& ops);

/**
 * The record of what the pull of a write reported (pull_result::oob),
 * which the op keeps in its outdata once it took its payload. The PG
 * log keeps it with the request's entry (pg_log_op_return_item_t): with
 * the return data of every op when the request asks for it
 * (CEPH_OSD_FLAG_RETURNVEC), which the client library does for a
 * request that pulls when no other op of it returns any
 * (MOSDOp::wants_pull_records()), else alone (keep_pull_records()). The
 * reply to a resend that the PG answers as a dup then reports the pull
 * of the attempt that did the write (dup_pull_results()), not a
 * declined one. Its byte count, flags and checksums; a range, when the
 * result has one, is the whole payload, at the base_offset of the
 * descriptor, which a resend carries unchanged.
 */
void encode_pull_record(const ceph::rdma::oob_result_t& r,
			ceph::buffer::list& bl);
/// the result encode_pull_record() kept in bl, for the op whose
/// descriptor is d; false when bl holds no such record
bool decode_pull_record(const ceph::buffer::list& bl,
			const ceph::rdma::delivery_t& d,
			ceph::rdma::oob_result_t* r);

/**
 * The return data that the PG log keeps with the write of a request
 * without CEPH_OSD_FLAG_RETURNVEC, which keeps none of its own: the
 * record of each pull in ops (encode_pull_record()), aligned with ops,
 * and nothing of what the other ops returned, as a CALL or a read
 * does. No reply carries it: the reply to the write carries no return
 * data, nor does the reply to a dup of it, whose results report the
 * pulls from it (OSDService::reply_op_error()). Returns false, and
 * leaves op_returns as it is, when no op of deliveries, aligned with
 * ops, kept a record.
 */
bool keep_pull_records(
  const std::vector<ceph::rdma::delivery_t>& deliveries,
  const std::vector<OSDOp>& ops,
  std::vector<pg_log_op_return_item_t>& op_returns);

/**
 * The results of the pulled writes of a request that the PG answers as
 * a dup (OSDService::reply_op_error()), from the op returns the PG log
 * kept for the attempt that did it, and r, its result: as the reply to
 * that attempt said, an op whose pull completed reports that pull when
 * the request succeeded. Any other pulled op, one that failed or whose
 * record the log did not keep, is not declined, and reports no bytes:
 * that attempt may have read its window. oob is aligned with
 * deliveries; the ops that do not pull stay as they are.
 */
void dup_pull_results(
  const std::vector<ceph::rdma::delivery_t>& deliveries,
  const std::vector<pg_log_op_return_item_t>& op_returns, int r,
  std::vector<ceph::rdma::oob_result_t>& oob);

/**
 * The results of a reply that OSDService::reply_op_error() sends, with
 * r its result: an error that fails the request before it ran, or, when
 * dup, the result of the attempt that did a write the PG answers as a
 * dup, with op_returns the return data the PG log kept for that
 * attempt. An error and a dup decline the ops that carry a descriptor,
 * and a dup declines no pulled op, as reply_error_results() says: no
 * transfer of the request started from this reply path, and the attempt
 * that did a write pushed nothing, but may have pulled. Nor does either
 * decline a pull of pulls (the request's pull set, or NULL) that may
 * have read the client's window. A dup reports the pulls that the PG
 * log kept (dup_pull_results()). oob is aligned with deliveries.
 */
void op_error_results(
  const std::vector<ceph::rdma::delivery_t>& deliveries,
  const std::vector<pg_log_op_return_item_t>& op_returns, int r, bool dup,
  const pull_set* pulls, std::vector<ceph::rdma::oob_result_t>& oob);

} // namespace ceph::osd::oob
