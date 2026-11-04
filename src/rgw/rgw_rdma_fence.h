// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <ostream>
#include <string_view>

#include "common/ceph_time.h"
#include "common/rdma_token.h"
#include "include/rados/librados.hpp"
#include "osd/osd_types.h"

namespace rgw::rdma {

/// the largest osd_oob_cutoff_late_tolerance that an OSD takes: the
/// option's maximum in osd.yaml.in, which a unit test checks. An OSD
/// rejects a larger value, from the monitor or from its own
/// configuration
inline constexpr std::chrono::milliseconds MAX_CUTOFF_LATE_TOLERANCE{2000};

/**
 * The fence, in seconds: how long after its descriptor-bearing reads
 * completed a GET waits, when a write of theirs may still land, before
 * anything else writes the window and before it answers. It is the
 * largest delivery lease plus the largest drain that any pool can have
 * (pg_pool_t's caps), and the largest osd_oob_cutoff_late_tolerance that
 * an OSD takes on top. The caps, because the OSD that serves a read may
 * use the options of another pool (the read tier) or of a newer map than
 * the gateway has. The tolerance, because a libfabric cut-off can end
 * that much after the drain, and set_rdma_delivery() asks a caller to
 * wait it out for a resent read and for a read that got no reply, whose
 * cut-off no reply follows. Its largest value, because each OSD reads
 * the option from its own configuration, which the gateway's need not
 * match, and no reply says the value that an OSD used.
 */
inline constexpr double delivery_fence()
{
  return pg_pool_t::MAX_RDMA_DELIVERY_LEASE +
    pg_pool_t::MAX_RDMA_DELIVERY_DRAIN +
    std::chrono::duration<double>(MAX_CUTOFF_LATE_TOLERANCE).count();
}

/**
 * Whether a write of OSD-direct reads, every one of them completed, may
 * still land in the window: the fence (delivery_fence()) that a fallback
 * and a response wait out before anything else writes the window or the
 * client hears back, or a relay window's quarantine in its place.
 *
 * Not when every descriptor-bearing read came back, without having been
 * resent, saying either that its OSD started no transfer for it
 * (declined) or that every byte was in the window before the OSD replied
 * (landed): then no write of the request can land any more. A read that
 * was resent may have started a transfer in an earlier attempt, on
 * another OSD, that its result knows nothing of. A read without a result,
 * as one that timed out, or one from an OSD that does not report either,
 * counts as having started a transfer. The same after a GET that
 * delivered everything as after one that failed: a reply without
 * rdma_delivery_result::LANDED may come before the bytes it reports
 * placed are in the window, as from an OSD whose transport does not
 * promise delivery-complete writes (a libfabric provider without
 * FI_DELIVERY_COMPLETE).
 */
template <typename Results>
bool fence_needed(const Results& results)
{
  using R = librados::ObjectReadOperation::rdma_delivery_result;
  for (const auto& r : results) {
    if (r.flags & R::RESENT) {
      return true;
    }
    if (!(r.flags & (R::DECLINED | R::LANDED))) {
      return true;
    }
  }
  return false;
}

/**
 * What passthrough GETs found out about the OSDs' transport for one kind
 * of token. An OSD whose transport does not promise delivery-complete
 * writes (a libfabric provider without FI_DELIVERY_COMPLETE) places a
 * stripe without saying that it landed, and its reply can come before
 * the bytes. Every passthrough GET that it serves then waits out the
 * fence before its response, success or not (fence_needed()). Rather
 * than have every GET wait, the gateway declines passthrough for HOLD
 * once a GET found such a stripe, and serves GETs staged or over HTTP.
 * Once HOLD has passed, one GET tries passthrough again, the probe. It
 * waits out the fence if a stripe still does not land, and starts the
 * next HOLD. The other GETs keep declining passthrough until the probe
 * found its stripes landed, or until PROBE has passed.
 */
class landing_reports {
 public:
  using clock = ceph::mono_clock;
  /// how long passthrough is declined after a stripe did not land
  static constexpr std::chrono::seconds HOLD{600};
  /// how long the probe has before another GET probes in its place
  static constexpr std::chrono::seconds PROBE{60};

  /// whether a GET may pass through at now: when no hold is on, or when
  /// one ran out and this GET claims the probe
  bool allow(clock::time_point now) {
    auto u = until.load();
    if (u == 0) {
      return true;
    }
    const auto n = ticks(now);
    if (n < u) {
      return false;
    }
    // the hold or the last probe ran out: this GET probes, unless another
    // claimed the probe first
    return until.compare_exchange_strong(u, n + ticks(PROBE));
  }

  /// what the stripes that the OSDs of a passthrough GET placed said at
  /// now: some did not land, or every one landed. Returns true when this
  /// starts a hold where none was
  bool note(clock::time_point now, bool unlanded, bool landed) {
    const auto n = ticks(now);
    if (unlanded) {
      return until.exchange(n + ticks(HOLD)) == 0;
    }
    if (landed) {
      // a probe found the OSDs' writes landed: passthrough goes on. Not a
      // GET that went out before a hold started, with more than PROBE of
      // it left: it may have met other OSDs
      auto u = until.load();
      if (u != 0 && u - n <= ticks(PROBE)) {
        until.compare_exchange_strong(u, 0);
      }
    }
    return false;
  }

  /// forget any hold, as if every stripe had landed
  void clear() { until = 0; }

 private:
  using rep = clock::duration::rep;
  static rep ticks(clock::time_point t) {
    return t.time_since_epoch().count();
  }
  static rep ticks(std::chrono::seconds d) {
    return std::chrono::duration_cast<clock::duration>(d).count();
  }
  /// the end of the hold or of the probe, in ticks of clock; 0 when
  /// passthrough is not declined
  std::atomic<rep> until{0};
};

/// the landing_reports of passthrough GETs with tokens of token's kind:
/// libfabric tokens, or cuObject descriptors, which other executors of
/// the OSDs serve
inline landing_reports& passthrough_landing(std::string_view token)
{
  static landing_reports ofi;
  static landing_reports cuobj;
  return ceph::rdma::is_ofi_token(token) ? ofi : cuobj;
}

/// what a passthrough GET makes of one stripe read's reply
enum class stripe_reply {
  failed,  ///< the read failed: the GET fails with its error
  inline_data,  ///< the OSD returned the data: the GET falls back to HTTP
  placed,  ///< the OSD placed the data in the window
};

/**
 * A stripe read that failed fails the GET. One whose OSD returned its data
 * inline sends the GET back to HTTP, whatever its delivery result says:
 * the OSD declined to push, or its push was cut off - per plan or with the
 * endpoint - and it answered with the data instead, so the window may
 * hold part of the stripe, and only the fallback rewrites it, behind the
 * fence when one is needed. Anything else was placed.
 */
inline stripe_reply classify_stripe(int rval, uint64_t inline_bytes)
{
  if (rval < 0) {
    return stripe_reply::failed;
  }
  return inline_bytes > 0 ? stripe_reply::inline_data : stripe_reply::placed;
}

/// the delivery results of a GET's stripe reads, counted for a log line
struct results_summary {
  size_t reads = 0;
  size_t declined = 0;
  size_t landed = 0;
  size_t resent = 0;
  /// neither declined nor landed: a transfer may have started
  size_t open = 0;
  uint64_t bytes = 0;
};

template <typename Results>
results_summary summarize(const Results& results)
{
  using R = librados::ObjectReadOperation::rdma_delivery_result;
  results_summary s;
  for (const auto& r : results) {
    s.reads++;
    s.bytes += r.bytes;
    if (r.flags & R::RESENT) {
      s.resent++;
    }
    if (r.flags & R::DECLINED) {
      s.declined++;
    } else if (r.flags & R::LANDED) {
      s.landed++;
    } else {
      s.open++;
    }
  }
  return s;
}

inline std::ostream& operator<<(std::ostream& o, const results_summary& s)
{
  return o << s.reads << " reads (" << s.declined << " declined, "
           << s.landed << " landed, " << s.open << " open, " << s.resent
           << " resent), " << s.bytes << " bytes placed";
}

} // namespace rgw::rdma
