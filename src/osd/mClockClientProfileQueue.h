// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#pragma once

#include <ostream>

#include "boost/variant.hpp"

#include "common/config.h"
#include "common/ceph_context.h"
#include "common/mClockPriorityQueue.h"
#include "osd/OpQueueItem.h"
#include "osd/mClockOpClassSupport.h"
#include "common/mClockClientInfoMgr.h"


namespace ceph {

  using Request = OpQueueItem;
  using Client = uint64_t;
  using MClockProfile = uint64_t;

  // This class exists to bridge the ceph code, which treats the class
  // as the client, and the queue, where the class is
  // osd_op_type_t. So this adapter class will transform calls
  // appropriately.
  class mClockClientProfileQueue : public OpQueue<Request, Client> {

  public:

    // either wraps a request or acts as a proxy for a client request,
    // the actual request of which is in client_queue.
    struct RequestProxyWrapper {
      std::unique_ptr<Request> request;
      bool is_proxy;

      // construct a proxy
      RequestProxyWrapper() :
	is_proxy(true)
      {}

      // construct a request wrapper
      explicit RequestProxyWrapper(Request&& _request) :
	request(new Request(std::move(_request))),
	is_proxy(false)
      {}

      RequestProxyWrapper(RequestProxyWrapper&& _wrapper) {
	std::swap(request, _wrapper.request);
	std::swap(is_proxy, _wrapper.is_proxy);
      }

      RequestProxyWrapper& operator=(RequestProxyWrapper&& rhs) {
	is_proxy = rhs.is_proxy;
	std::swap(request, rhs.request);
	return *this;
      }
    }; // RequestProxyWrapper

  protected:

    using osd_op_type_t = ceph::mclock::osd_op_type_t;
    using ClientProfile = std::pair<Client,MClockProfile>;
    using client_queue_t = dmc::PullPriorityQueue<ClientProfile, Request, true>;
    using top_queue_t = mClockQueue<RequestProxyWrapper, osd_op_type_t>;
    using client_profile_mgr_t =
      ceph::mclock::MClockClientInfoMgr<ClientProfile>;

    client_profile_mgr_t client_profile_mgr;
    ceph::mclock::OpClassClientInfoMgr op_class_mgr;
    client_queue_t client_queue;
    top_queue_t top_queue;

  public:

    mClockClientProfileQueue(CephContext *cct);

    inline unsigned length() const override final {
      return top_queue.length();
    }

    // Ops of this priority should be deleted immediately
    inline void remove_by_class(Client cl,
				std::list<Request> *out) override final {
      top_queue.remove_by_filter(
	[&cl, out] (RequestProxyWrapper&& r) -> bool {
	  if (!r.is_proxy && cl == r.request->get_owner()) {
	    out->push_front(std::move(*r.request));
	    return true;
	  } else {
	    return false;
	  }
	});
    }

    void enqueue_strict(Client cl,
			unsigned priority,
			Request&& item) override final;

    // Enqueue op in the front of the strict queue
    void enqueue_strict_front(Client cl,
			      unsigned priority,
			      Request&& item) override final;

    // Enqueue op in the back of the regular queue
    void enqueue(Client cl,
		 unsigned priority,
		 unsigned cost,
		 Request&& item) override final;

    // Enqueue the op in the front of the regular queue
    void enqueue_front(Client cl,
		       unsigned priority,
		       unsigned cost,
		       Request&& item) override final;

    // Return an op to be dispatch
    Request dequeue() override final;

    // Returns if the queue is empty
    inline bool empty() const override final {
      return top_queue.empty();
    }

    // Formatted output of the queue
    void dump(ceph::Formatter *f) const override final;

#if 0
  protected:

    InnerClient get_inner_client(const Client& cl, const Request& request);
#endif
  }; // class mClockClientProfileQueue

} // namespace ceph
