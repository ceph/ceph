// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#pragma once

#include <memory>
#include <mutex>
#include <deque>
#include <functional>
#include <variant>

#include "ssched_recs.h"

#ifdef PROFILE
#include "profile.h"
#endif

namespace crimson {

  namespace simple_scheduler {

    template<typename C, typename R, typename Time>
    class SimpleQueue {

    public:

      using RequestRef = std::unique_ptr<R>;

      // a function to see whether the server can handle another request
      using CanHandleRequestFunc = std::function<bool(void)>;

      // a function to submit a request to the server; the second
      // parameter is a callback when it's completed
      using HandleRequestFunc =
	std::function<void(const C&,RequestRef,NullData,uint64_t)>;

      struct PullReq {
	enum class Type { returning, none };

	struct Retn {
	  C           client;
	  RequestRef  request;
	};

	Type                 type;
	std::variant<Retn> data;
      };

    protected:

      enum class Mechanism { push, pull };

      struct QRequest {
	C          client;
	RequestRef request;
      };

      bool finishing = false;
      Mechanism mechanism;

      CanHandleRequestFunc can_handle_f;
      HandleRequestFunc handle_f;

      mutable std::mutex queue_mtx;
      using DataGuard = std::lock_guard<decltype(queue_mtx)>;

      std::deque<QRequest> queue;

#ifdef PROFILE
    public:
      ProfileTimer<std::chrono::nanoseconds> pull_request_timer;
      ProfileTimer<std::chrono::nanoseconds> add_request_timer;
      ProfileTimer<std::chrono::nanoseconds> request_complete_timer;
    protected:
#endif

    public:

      // push full constructor
      SimpleQueue(CanHandleRequestFunc _can_handle_f,
		  HandleRequestFunc _handle_f) :
	mechanism(Mechanism::push),
	can_handle_f(_can_handle_f),
	handle_f(_handle_f)
      {
	// empty
      }

      SimpleQueue() :
	mechanism(Mechanism::pull)
      {
	// empty
      }

      ~SimpleQueue() {
	finishing = true;
      }

      void add_request(R&& request,
		       const C& client_id,
		       const ReqParams& req_params,
		       uint64_t request_cost) {
	add_request(RequestRef(new R(std::move(request))),
		    client_id, req_params, request_cost);
      }

      void add_request(RequestRef&& request,
		       const C& client_id,
		       const ReqParams& req_params,
		       uint64_t request_cost) {
	DataGuard g(queue_mtx);

#ifdef PROFILE
	add_request_timer.start();
#endif
	queue.emplace_back(QRequest{client_id, std::move(request)});

	if (Mechanism::push == mechanism) {
	  schedule_request();
	}

#ifdef PROFILE
	add_request_timer.stop();
#endif
      } // add_request

      void request_completed() {
	assert(Mechanism::push == mechanism);
	DataGuard g(queue_mtx);

#ifdef PROFILE
	request_complete_timer.start();
#endif
	schedule_request();

#ifdef PROFILE
	request_complete_timer.stop();
#endif
      } // request_completed

      PullReq pull_request() {
	assert(Mechanism::pull == mechanism);
	PullReq result;
	DataGuard g(queue_mtx);

#ifdef PROFILE
	pull_request_timer.start();
#endif

	if (queue.empty()) {
	  result.type = PullReq::Type::none;
	} else {
	  auto front = queue.front();
	  result.type = PullReq::Type::returning;
	  result.data =
	    typename PullReq::Retn{front.client, std::move(front.request)};
	  queue.pop();
	}

#ifdef PROFILE
	pull_request_timer.stop();
#endif

	return result;
      }

    protected:

      // queue_mtx should be held when called; should only be called
      // when mechanism is push
      void schedule_request() {
	if (!queue.empty() && can_handle_f()) {
	  auto& front = queue.front();
	  static NullData null_data;
	  handle_f(front.client, std::move(front.request), null_data, 1u);
	  queue.pop_front();
	}
      }
    };
  };
};
