// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

/**
 * Plans that stopped waiting for their writes, for an executor whose
 * transport cannot cancel a posted write (cuObject).
 *
 * Every write carries a handle that is unique in the process: a plan
 * reserves count handles from first, and its write i carries
 * first + i. A plan that stops waiting while writes are still posted
 * keeps the staging buffer that they read, and records it here, with
 * which of its writes are still posted. Their completions come back to
 * whoever polls the channel next: credit() counts each one against the
 * plan that posted it, once, should the transport return one twice,
 * and hands that plan's buffer back with the last one. drop() hands
 * every buffer back, for when the transport flushed what was posted.
 *
 * Free of the transport, so that the bookkeeping can be tested without
 * one.
 */
namespace ceph::osd::oob {

/// true when h is one of the count handles from first
inline bool owns_handle(uint64_t first, uint64_t count, uint64_t h)
{
  return h - first < count;  // unsigned: an h below first wraps high
}

/// true when h is the handle of a write that is still posted, of the
/// writes from first: posted[i] tells whether write first + i still is
inline bool still_posted(uint64_t first, const std::vector<bool>& posted,
			 uint64_t h)
{
  return owns_handle(first, posted.size(), h) && posted[h - first];
}

template <typename Buf>
class abandoned_plans {
public:
  bool empty() const { return plans.empty(); }
  size_t size() const { return plans.size(); }

  /// a plan whose writes carry the handles from first stopped waiting:
  /// posted[i] tells whether its write i was still posted then, as one
  /// at least was
  void abandon(uint64_t first, std::vector<bool> posted, Buf buf)
  {
    const auto outstanding =
      static_cast<uint64_t>(std::count(posted.begin(), posted.end(), true));
    plans.push_back({first, std::move(posted), outstanding, std::move(buf)});
  }

  enum class credit_t {
    unknown,   ///< no plan here posted it, or drop() gave its plan up
    repeated,  ///< its write had completed already: counted once only
    counted,   ///< its plan still has writes posted
    last,      ///< its plan's last write: *buf is that plan's buffer
  };

  /// the completion of the write that carried handle h
  credit_t credit(uint64_t h, Buf* buf)
  {
    auto p = std::find_if(plans.begin(), plans.end(),
			  [h](const plan_t& a) {
			    return owns_handle(a.first, a.posted.size(), h);
			  });
    if (p == plans.end()) {
      return credit_t::unknown;
    }
    if (!p->posted[h - p->first]) {
      return credit_t::repeated;
    }
    p->posted[h - p->first] = false;
    if (--p->outstanding > 0) {
      return credit_t::counted;
    }
    *buf = std::move(p->buf);
    plans.erase(p);
    return credit_t::last;
  }

  /// the transport flushed every posted write: all buffers come back
  std::vector<Buf> drop()
  {
    std::vector<Buf> bufs;
    bufs.reserve(plans.size());
    for (auto& a : plans) {
      bufs.push_back(std::move(a.buf));
    }
    plans.clear();
    return bufs;
  }

private:
  struct plan_t {
    uint64_t first;             // the plan's first handle
    std::vector<bool> posted;   // which of its writes are still posted
    uint64_t outstanding;       // how many are
    Buf buf;
  };
  std::vector<plan_t> plans;
};

} // namespace ceph::osd::oob
