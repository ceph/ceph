// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "sync_fairness_asio.h"

#include <chrono>
#include <coroutine>
#include <memory>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "include/neorados/RADOS.hpp"

#include "common/async/async_call.h"

#include "rgw_neorados.h"
#include "sync_fairness_common.h"


namespace rgw::sync_fairness {

namespace async = ceph::async;
using namespace std::literals;

using asio::use_awaitable;
using asio::awaitable;

using neorados::IOContext;
using neorados::Object;

using namespace detail;

class NeoRadosBidManager : public BidManagerAsio, public DoutPrefix {
  sal::RadosStore* const store;
  asio::any_io_executor executor;
  asio::strand<asio::any_io_executor> strand{executor};
  const Object watch_obj;
  const IOContext ioc;

  bid_vector my_bids;
  bidder_map all_bids;

  asio::cancellation_signal watcher_cancellation;
  std::uint64_t watch_handle = 0;

 public:
  NeoRadosBidManager(sal::RadosStore* store, asio::any_io_executor executor,
		     Object watch_obj, IOContext ioc, std::size_t num_shards)
    : DoutPrefix(store->ctx(), ceph_subsys_rgw_sync, "sync fairness: "),
      store(store), executor(executor), watch_obj(std::move(watch_obj)),
      ioc(std::move(ioc))
  {
    // fill my_bids with random values
    std::random_device rd;
    std::default_random_engine rng{rd()};

    my_bids.resize(num_shards);
    for (bid_value i = 0; i < num_shards; ++i) {
      my_bids[i] = i;
    }
    std::shuffle(my_bids.begin(), my_bids.end(), rng);
  }

  ~NeoRadosBidManager() {
    co_spawn(executor, stop(), asio::detached);
  }

  awaitable<void> start_watcher() {
    watch_handle = co_await store->get_neorados().watch(watch_obj, ioc, use_awaitable);
    co_spawn(store->get_io_context(),
	     [this]() -> awaitable<void> {
	       for (;;) {
		 auto [notify_id, cookie, notifier_id, bl] = co_await
		   store->get_neorados().next_notification(watch_handle,
							   use_awaitable);
		 BidRequest request;
		 try {
		   auto p = bl.cbegin();
		   decode(request, p);
		 } catch (const buffer::error& e) {
		   ldpp_dout(this, -1) << "Failed to decode notification: "
				       << e.what() << dendl;
		   continue;
		 }

		 BidResponse response;
		 co_await on_peer_bid(notifier_id, std::move(request.bids),
				      response.bids);

		 buffer::list reply;
		 encode(response, reply);

		 co_await store->get_neorados().notify_ack(watch_obj, ioc, notify_id,
							   cookie, reply,
							   use_awaitable);

	       }
	       co_return;
	     }, asio::bind_cancellation_slot(watcher_cancellation.slot(),
					     asio::detached));
  }

  awaitable<void> stop_watcher() {
    watcher_cancellation.emit(asio::cancellation_type::terminal);
    co_await store->get_neorados().unwatch(watch_handle, ioc, use_awaitable);
    watch_handle = 0;
    co_return;
  }

  awaitable<void> stop() override {
    if (watch_handle) {
      co_await stop_watcher();
    }
    co_return;
  }

  awaitable<void> on_peer_bid(uint64_t peer_id, bid_vector peer_bids,
			      bid_vector& my_bids)
  {
    ldpp_dout(this, 10) << "received bids from peer " << peer_id << dendl;

    co_await async::async_dispatch(
      strand,
      [this, peer_id, &peer_bids, &my_bids]() {
	all_bids[peer_id] = std::move(peer_bids);
	my_bids = this->my_bids;
      }, use_awaitable);
  }

  awaitable<bool> is_highest_bidder(std::size_t index) override
  {
    co_return co_await async::async_dispatch(
      strand,
      [this, index]() {
	const bid_value my_bid = my_bids.at(index); // may throw
	for (const auto& peer_bids : all_bids) {
	  const bid_value peer_bid = peer_bids.second.at(index); // may throw
	  if (peer_bid > my_bid) {
	    return false;
	  }
	}
	return true;
      }, use_awaitable);
  }

  asio::awaitable<void> notify() override
  {
    static constexpr auto timeout = 15s;
    buffer::list request;
    co_await async::async_dispatch(
      strand,
      [this, &request]() {
	encode_notify_request(my_bids, request);
      }, asio::use_awaitable);
    auto [reply_map, missed_set] = co_await store->get_neorados().notify(
      watch_obj, ioc, std::move(request), timeout, use_awaitable);

    ldpp_dout(this, 10) << "received notify response from peers" << dendl;

    co_await async::async_dispatch(
      strand,
      [this, &reply_map, &missed_set]() {
	// clear existing bids in case any peers went away. note that this may
	// remove newer bids from peer notifications that raced with ours
	all_bids.clear();
	apply_notify_responses(reply_map, missed_set, all_bids);
      }, asio::use_awaitable);
    co_return;
  }
};

asio::awaitable<std::unique_ptr<BidManagerAsio>> create_neorados_bid_manager(
  sal::RadosStore* store, asio::any_io_executor executor,
  std::string_view obj, const rgw_pool& pool, std::size_t num_shards);

awaitable<std::unique_ptr<BidManagerAsio>> create_neorados_bid_manager(
  const DoutPrefixProvider* dpp, sal::RadosStore* store,
  asio::any_io_executor executor, std::string_view obj, const rgw_pool& pool,
  std::size_t num_shards)
{
  auto ioc = co_await neorados::init_iocontext(dpp, store->get_neorados(), pool,
					       neorados::create);
  auto p = std::make_unique<NeoRadosBidManager>(store, executor, obj,
						std::move(ioc), num_shards);
  co_await p->start_watcher();
  co_return std::move(p);
}
}
