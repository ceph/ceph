// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */

#pragma once


#include "queue_ifc.h"

#include "test_recs.h"
#include "simple_recs.h"


namespace crimson {
  namespace simple_scheduler {

    template<typename C, typename R>
    class SimpleQueue {

    public:

      using RequestRef = std::unique_ptr<R>;

      using ClientRecRef = std::shared_ptr<ClientRec>;

      // a function to see whether the server can handle another request
      using CanHandleRequestFunc = std::function<bool(void)>;

      // a function to submit a request to the server; the second
      // parameter is a callback when it's completed
      using HandleRequestFunc = std::function<void(const C&,RequestRef)>;


      struct PullReq {
	enum class Type { returning, none };

	struct Retn {
	  C           client;
	  RequestRef  request;
	};

	Type                 type;
	boost::variant<Retn> data;
      };

    protected:

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

    public:

      // push full constructor
      Queue(CanHandleRequestFunc _can_handle_f,
	    HandleRequestFunc _handle_f) :
	mechanism(Mechanism::push),
	can_handle_f(_can_handle_f),
	handle_f(_handle_f)
      {
	// empty
      }

      Queue() :
	mechanism(Mechanism::pull),
      {
	// empty
      }

      ~Queue() {
	finishing = true;
      }

      void add_request(const R& request,
		       const ReqParams<C>& req_params) {
	add_request(RequestRef(new R(request)), req_params, get_time());
      }

      void add_request(const R& request,
		       const ReqParams<C>& req_params,
		       const Time time) {
	add_request(RequestRef(new R(request)), req_params, time);
      }

      void add_request(RequestRef&& request,
		       const ReqParams<C>& req_params) {
	add_request(request, req_params, get_time());
      }

      void add_request(RequestRef&& request,
		       const ReqParams<C>& req_params,
		       const Time time) {
	const C& client_id = req_params.client;
	DataGuard g(queue_mtx);
	queue.enqueue_back(QRequest{client_id, std::move(request)});

	if (Mechanism::push == mechanism) {
	  schedule_request();
	}
      }

      void request_completed() {
	if (Mechanism::push == mechanism) {
	  DataGuard g(queue_mtx);
	  schedule_request();
	}
      }


      PullReq pull_request() {
	assert(Mechanism::pull == mechanism);
	PullReq result;
	DataGuard g(queue_mtx);
	if (queue.empty()) {
	  result.type = NextReqType::none;
	  return result;
	} else {
	  auto front = queue.front();
	  result.type = NextReqType::returning;
	  result.data = PullReq::Ret{front.client, std::move(front.request)};
	  queue.pop();
	  return result;
	}
      };

    protected:

      void schedule_request() {
	DataGuard g(queue_mtx);
	if (!queue.empty() && can_handle_f()) {
	  auto front = queue.front();
	  queue.pop_front();
	  handle_f(front);
	}
      }
    };
  };
};
