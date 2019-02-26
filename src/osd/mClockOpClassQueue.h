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
#include "boost/container/flat_set.hpp"

#include "common/config.h"
#include "common/ceph_context.h"
#include "common/mClockPriorityQueue.h"
#include "osd/OpQueueItem.h"
#include "osd/mClockOpClassSupport.h"


namespace ceph {

  using Request = OpQueueItem;
  using Client = uint64_t;

  // This class exists to bridge the ceph code, which treats the class
  // as the client, and the queue, where the class is
  // osd_op_type_t. So this adapter class will transform calls
  // appropriately.
  class mClockOpClassQueue : public OpQueue<Request, Client> {

    using osd_op_type_t = ceph::mclock::osd_op_type_t;

    using queue_t = mClockQueue<Request, osd_op_type_t>;
    queue_t queue;

    ceph::mclock::OpClassClientInfoMgr client_info_mgr;

  public:

    mClockOpClassQueue(CephContext *cct);

    const crimson::dmclock::ClientInfo*
    op_class_client_info_f(const osd_op_type_t& op_type);

    inline unsigned get_size_slow() const {
      return queue.get_size_slow();
    }

    // Ops of this priority should be deleted immediately
    inline void remove_by_class(Client cl,
				std::list<Request> *out) override final {
      queue.remove_by_filter(
	[&cl, out] (Request&& r) -> bool {
	  if (cl == r.get_owner()) {
	    out->push_front(std::move(r));
	    return true;
	  } else {
	    return false;
	  }
	});
    }

    inline void enqueue_strict(Client cl,
			       unsigned priority,
			       Request&& item) override final {
      queue.enqueue_strict(client_info_mgr.osd_op_type(item),
			   priority,
			   std::move(item));
    }

    // Enqueue op in the front of the strict queue
    inline void enqueue_strict_front(Client cl,
				     unsigned priority,
				     Request&& item) override final {
      queue.enqueue_strict_front(client_info_mgr.osd_op_type(item),
				 priority,
				 std::move(item));
    }

    // Enqueue op in the back of the regular queue
    inline void enqueue(Client cl,
			unsigned priority,
			unsigned cost,
			Request&& item) override final {
      queue.enqueue(client_info_mgr.osd_op_type(item),
		    priority,
		    1u,
		    std::move(item));
    }

    // Enqueue the op in the front of the regular queue
    inline void enqueue_front(Client cl,
			      unsigned priority,
			      unsigned cost,
			      Request&& item) override final {
      queue.enqueue_front(client_info_mgr.osd_op_type(item),
			  priority,
			  1u,
			  std::move(item));
    }

    // Returns if the queue is empty
    inline bool empty() const override final {
      return queue.empty();
    }

    // Return an op to be dispatch
    inline Request dequeue() override final {
      return queue.dequeue();
    }

    // Formatted output of the queue
    void dump(ceph::Formatter *f) const override final;
  }; // class mClockOpClassAdapter
} // namespace ceph
