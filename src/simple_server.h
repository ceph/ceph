// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */

#pragma once


namespace crimson {
  namespace simple_scheduler {
    struct ClientInfo {
      // empty
    };

    template<typename C, typename R>
    class Queue {

    public:

      using RequestRef = std::unique_ptr<R>;

      enum class Mechanism { push, pull };

      enum class NextReqType { returning, future, none };

      struct PullReq {
	struct Retn {
	  C           client;
	  RequestRef  request;
	};

	NextReqType               type;
	boost::variant<Retn,Time> data;
      };

    protected:

      bool finishing = false;

      mutable std::mutex queue_mtx;
      using DataGuard = std::lock_guard<decltype(queue_mtx)>;

      std::deque<RequestRef> queue;
      Mechanism mechanism;

    public:

      // push full constructor
      Queue(ClientInfoFunc _client_info_f,
	    CanHandleRequestFunc _can_handle_f,
	    HandleRequestFunc _handle_f) {
      }

      Queue(ClientInfoFunc _client_info_f) {
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
	queue.enqueue_back(request);

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
	  result.data = PullReq::Ret{client, std::move(front)};
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
