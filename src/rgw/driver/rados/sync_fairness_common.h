// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <cstdint>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>
#include <boost/system/error_code.hpp>

#include "include/buffer.h"
#include "include/encoding.h"

namespace rgw::sync_fairness::detail {
namespace buffer = ceph::buffer;
namespace container = boost::container;
namespace sys = boost::system;

using bid_value = std::uint16_t;
using bid_vector = std::vector<bid_value>; // bid per replication log shard

using notifier_id = uint64_t;
using bidder_map = container::flat_map<notifier_id, bid_vector>;

struct BidRequest {
  bid_vector bids;

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bids, bl);
    ENCODE_FINISH(bl);
  }
  void decode(buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(bids, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(BidRequest);

struct BidResponse {
  bid_vector bids;

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bids, bl);
    ENCODE_FINISH(bl);
  }
  void decode(buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(bids, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(BidResponse);


static void encode_notify_request(const bid_vector& bids, buffer::list& bl)
{
  BidRequest request;
  request.bids = bids; // copy the vector
  encode(request, bl);
}

inline sys::error_code apply_notify_responses(
  const container::flat_map<std::pair<std::uint64_t, std::uint64_t>,
                      buffer::list>& replies,
  const container::flat_set<std::pair<std::uint64_t, std::uint64_t>>& timeouts,
  bidder_map& bidders)
{
  try {
    // add peers that replied
    for (const auto& peer : replies) {
      auto q = peer.second.cbegin();
      BidResponse response;
      decode(response, q);

      uint64_t peer_id = peer.first.first;
      bidders[peer_id] = std::move(response.bids);
    }

    // remove peers that timed out
    for (const auto& peer : timeouts) {
      uint64_t peer_id = peer.first;
      bidders.erase(peer_id);
    }
  } catch (const buffer::error& e) {
    return e.code();
  }
  return {};
}

inline sys::error_code apply_notify_responses(const bufferlist& bl,
					      bidder_map& bidders)
{
  container::flat_map<std::pair<std::uint64_t, std::uint64_t>,
		      buffer::list> replies;
  container::flat_set<std::pair<std::uint64_t, std::uint64_t>> timeouts;
  try {
    // decode notify responses
    auto p = bl.cbegin();

    using ceph::decode;
    decode(replies, p);
    decode(timeouts, p);
    return apply_notify_responses(replies, timeouts, bidders);
  } catch (const buffer::error& e) {
    return e.code();
  }
  return {};
}
}
