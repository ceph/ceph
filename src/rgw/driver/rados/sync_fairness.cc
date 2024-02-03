// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <mutex>
#include <random>
#include <vector>
#include <boost/container/flat_map.hpp>
#include "include/encoding.h"
#include "include/rados/librados.hpp"
#include "rgw_sal_rados.h"
#include "rgw_cr_rados.h"
#include "sync_fairness.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

namespace rgw::sync_fairness {

using bid_value = uint16_t;
using bid_vector = std::vector<bid_value>; // bid per replication log shard

using notifier_id = uint64_t;
using bidder_map = boost::container::flat_map<notifier_id, bid_vector>;

struct BidRequest {
  bid_vector bids;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bids, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    decode(bids, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(BidRequest);

struct BidResponse {
  bid_vector bids;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(bids, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    decode(bids, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(BidResponse);


static void encode_notify_request(const bid_vector& bids, bufferlist& bl)
{
  BidRequest request;
  request.bids = bids; // copy the vector
  encode(request, bl);
}

static int apply_notify_responses(const bufferlist& bl, bidder_map& bidders)
{
  bc::flat_map<std::pair<uint64_t, uint64_t>, bufferlist> replies;
  std::vector<std::pair<uint64_t, uint64_t>> timeouts;
  try {
    // decode notify responses
    auto p = bl.cbegin();

    using ceph::decode;
    decode(replies, p);
    decode(timeouts, p);

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
    return -EIO;
  }
  return 0;
}


// server interface to handle bid notifications from peers
struct Server {
  virtual ~Server() = default;

  virtual void on_peer_bid(uint64_t peer_id, bid_vector peer_bids,
                           bid_vector& my_bids) = 0;
};


// rados watcher for sync fairness notifications
class Watcher : public librados::WatchCtx2 {
  const DoutPrefixProvider* dpp;
  sal::RadosStore* const store;
  rgw_raw_obj obj;
  Server* server;
  rgw_rados_ref ref;
  uint64_t handle = 0;

 public:
  Watcher(const DoutPrefixProvider* dpp, sal::RadosStore* store,
          const rgw_raw_obj& obj, Server* server)
    : dpp(dpp), store(store), obj(obj), server(server)
  {}
  ~Watcher()
  {
    stop();
  }

  int start()
  {
    int r = store->getRados()->get_raw_obj_ref(dpp, obj, &ref);
    if (r < 0) {
      return r;
    }

    // register a watch on the control object
    r = ref.ioctx.watch2(ref.obj.oid, &handle, this);
    if (r == -ENOENT) {
      constexpr bool exclusive = true;
      r = ref.ioctx.create(ref.obj.oid, exclusive);
      if (r == -EEXIST || r == 0) {
        r = ref.ioctx.watch2(ref.obj.oid, &handle, this);
      }
    }
    if (r < 0) {
      ldpp_dout(dpp, -1) << "Failed to watch " << ref.obj
          << " with " << cpp_strerror(-r) << dendl;
      ref.ioctx.close();
      return r;
    }

    ldpp_dout(dpp, 10) << "Watching " << ref.obj.oid << dendl;
    return 0;
  }

  int restart()
  {
    int r = ref.ioctx.unwatch2(handle);
    if (r < 0) {
      ldpp_dout(dpp, -1) << "Failed to unwatch on " << ref.obj
          << " with " << cpp_strerror(-r) << dendl;
    }
    r = ref.ioctx.watch2(ref.obj.oid, &handle, this);
    if (r < 0) {
      ldpp_dout(dpp, -1) << "Failed to restart watch on " << ref.obj
          << " with " << cpp_strerror(-r) << dendl;
      ref.ioctx.close();
    }
    return r;
  }

  void stop()
  {
    if (handle) {
      ref.ioctx.unwatch2(handle);
      ref.ioctx.close();
    }
  }

  // respond to bid notifications
  void handle_notify(uint64_t notify_id, uint64_t cookie,
                     uint64_t notifier_id, bufferlist& bl)
  {
    if (cookie != handle) {
      return;
    }

    BidRequest request;
    try {
      auto p = bl.cbegin();
      decode(request, p);
    } catch (const buffer::error& e) {
      ldpp_dout(dpp, -1) << "Failed to decode notification: " << e.what() << dendl;
      return;
    }

    BidResponse response;
    server->on_peer_bid(notifier_id, std::move(request.bids), response.bids);

    bufferlist reply;
    encode(response, reply);

    ref.ioctx.notify_ack(ref.obj.oid, notify_id, cookie, reply);
  }

  // reestablish the watch if it gets disconnected
  void handle_error(uint64_t cookie, int err)
  {
    if (cookie != handle) {
      return;
    }
    if (err == -ENOTCONN) {
      ldpp_dout(dpp, 4) << "Disconnected watch on " << ref.obj << dendl;
      restart();
    }
  }
}; // Watcher


class RadosBidManager;

// RGWRadosNotifyCR wrapper coroutine
class NotifyCR : public RGWCoroutine {
  rgw::sal::RadosStore* store;
  RadosBidManager* mgr;
  rgw_raw_obj obj;
  bufferlist request;
  bufferlist response;
 public:
  NotifyCR(rgw::sal::RadosStore* store, RadosBidManager* mgr,
           const rgw_raw_obj& obj, const bid_vector& my_bids)
      : RGWCoroutine(store->ctx()), store(store), mgr(mgr), obj(obj)
  {
    encode_notify_request(my_bids, request);
  }

  int operate(const DoutPrefixProvider* dpp) override;
};


class RadosBidManager : public BidManager, public Server, public DoutPrefix {
  sal::RadosStore* store;
  rgw_raw_obj obj;
  Watcher watcher;

  std::mutex mutex;
  bid_vector my_bids;
  bidder_map all_bids;

 public:
  RadosBidManager(sal::RadosStore* store, const rgw_raw_obj& watch_obj,
                 std::size_t num_shards)
    : DoutPrefix(store->ctx(), dout_subsys, "sync fairness: "),
      store(store), obj(watch_obj), watcher(this, store, watch_obj, this)
  {
    // fill my_bids with random values
    std::random_device rd;
    std::default_random_engine rng{rd()};

    my_bids.resize(num_shards);
    for(bid_value i = 0; i < num_shards; ++i) {
      my_bids[i] = i;
    }
    std::shuffle(my_bids.begin(), my_bids.end(), rng);
  }

  int start() override
  {
    return watcher.start();
  }

  void on_peer_bid(uint64_t peer_id, bid_vector peer_bids,
                   bid_vector& my_bids) override
  {
    ldpp_dout(this, 10) << "received bids from peer " << peer_id << dendl;

    auto lock = std::scoped_lock{mutex};
    all_bids[peer_id] = std::move(peer_bids);
    my_bids = this->my_bids;
  }

  bool is_highest_bidder(std::size_t index) override
  {
    auto lock = std::scoped_lock{mutex};
    const bid_value my_bid = my_bids.at(index); // may throw

    for (const auto& peer_bids : all_bids) {
      const bid_value peer_bid = peer_bids.second.at(index); // may throw
      if (peer_bid > my_bid) {
        return false;
      }
    }
    return true;
  }

  RGWCoroutine* notify_cr() override
  {
    auto lock = std::scoped_lock{mutex};
    return new NotifyCR(store, this, obj, my_bids);
  }

  void notify_response(const bufferlist& bl)
  {
    ldpp_dout(this, 10) << "received notify response from peers" << dendl;

    auto lock = std::scoped_lock{mutex};

    // clear existing bids in case any peers went away. note that this may
    // remove newer bids from peer notifications that raced with ours
    all_bids.clear();

    apply_notify_responses(bl, all_bids);
  }
};


int NotifyCR::operate(const DoutPrefixProvider* dpp)
{
  static constexpr uint64_t timeout_ms = 15'000;
  reenter(this) {
    yield call(new RGWRadosNotifyCR(store, obj, request,
                                    timeout_ms, &response));
    if (retcode < 0) {
      return set_cr_error(retcode);
    }
    mgr->notify_response(response);
    return set_cr_done();
  }
  return 0;
}


auto create_rados_bid_manager(sal::RadosStore* store,
                              const rgw_raw_obj& watch_obj,
                              std::size_t num_shards)
  -> std::unique_ptr<BidManager>
{
  return std::make_unique<RadosBidManager>(store, watch_obj, num_shards);
}

} // namespace rgw::sync_fairness
