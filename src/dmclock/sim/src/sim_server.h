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


#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <deque>

#include "sim_recs.h"


namespace crimson {
  namespace qos_simulation {

    template<typename Q, typename ReqPm, typename RespPm, typename Accum>
    class SimulatedServer {

      struct QueueItem {
	ClientId                     client;
	std::unique_ptr<TestRequest> request;
	RespPm                       additional;
	Cost                         request_cost;

	QueueItem(const ClientId&                _client,
		  std::unique_ptr<TestRequest>&& _request,
		  const RespPm&                  _additional,
		  const Cost                     _request_cost) :
	  client(_client),
	  request(std::move(_request)),
	  additional(_additional),
	  request_cost(_request_cost)
	{
	  // empty
	}
      }; // QueueItem

    public:

      struct InternalStats {
	std::mutex mtx;
	std::chrono::nanoseconds add_request_time;
	std::chrono::nanoseconds request_complete_time;
	uint32_t add_request_count;
	uint32_t request_complete_count;

	InternalStats() :
	  add_request_time(0),
	  request_complete_time(0),
	  add_request_count(0),
	  request_complete_count(0)
	{
	  // empty
	}
      };

      using ClientRespFunc = std::function<void(ClientId,
						const TestResponse&,
						const ServerId&,
						const RespPm&,
						const Cost)>;

      using ServerAccumFunc = std::function<void(Accum& accumulator,
						 const RespPm& additional)>;

    protected:

      const ServerId                 id;
      Q*                             priority_queue;
      ClientRespFunc                 client_resp_f;
      int                            iops;
      size_t                         thread_pool_size;

      bool                           finishing;
      std::chrono::microseconds      op_time;

      std::mutex                     inner_queue_mtx;
      std::condition_variable        inner_queue_cv;
      std::deque<QueueItem>          inner_queue;

      std::thread*                   threads;

      using InnerQGuard = std::lock_guard<decltype(inner_queue_mtx)>;
      using Lock = std::unique_lock<std::mutex>;

      // data collection

      ServerAccumFunc accum_f;
      Accum accumulator;

      InternalStats internal_stats;

    public:

      using CanHandleRequestFunc = std::function<bool(void)>;
      using HandleRequestFunc =
	std::function<void(const ClientId&,std::unique_ptr<TestRequest>,const RespPm&, uint64_t)>;
      using CreateQueueF = std::function<Q*(CanHandleRequestFunc,HandleRequestFunc)>;
					

      SimulatedServer(ServerId _id,
		      int _iops,
		      size_t _thread_pool_size,
		      const ClientRespFunc& _client_resp_f,
		      const ServerAccumFunc& _accum_f,
		      CreateQueueF _create_queue_f) :
	id(_id),
	priority_queue(_create_queue_f(std::bind(&SimulatedServer::has_avail_thread,
						 this),
				       std::bind(&SimulatedServer::inner_post,
						 this,
						 std::placeholders::_1,
						 std::placeholders::_2,
						 std::placeholders::_3,
						 std::placeholders::_4))),
	client_resp_f(_client_resp_f),
	iops(_iops),
	thread_pool_size(_thread_pool_size),
	finishing(false),
	accum_f(_accum_f)
      {
	op_time =
	  std::chrono::microseconds((int) (0.5 +
					   thread_pool_size * 1000000.0 / iops));
	std::chrono::milliseconds finishing_check_period(1000);
	threads = new std::thread[thread_pool_size];
	for (size_t i = 0; i < thread_pool_size; ++i) {
	  threads[i] = std::thread(&SimulatedServer::run, this, finishing_check_period);
	}
      }

      virtual ~SimulatedServer() {
	Lock l(inner_queue_mtx);
	finishing = true;
	inner_queue_cv.notify_all();
	l.unlock();

	for (size_t i = 0; i < thread_pool_size; ++i) {
	  threads[i].join();
	}

	delete[] threads;

	delete priority_queue;
      }

      void post(TestRequest&& request,
		const ClientId& client_id,
		const ReqPm& req_params,
		const Cost request_cost)
      {
	time_stats(internal_stats.mtx,
		   internal_stats.add_request_time,
		   [&](){
		     priority_queue->add_request(std::move(request),
						 client_id,
						 req_params,
						 request_cost);
		   });
	count_stats(internal_stats.mtx,
		    internal_stats.add_request_count);
      }

      bool has_avail_thread() {
	InnerQGuard g(inner_queue_mtx);
	return inner_queue.size() <= thread_pool_size;
      }

      const Accum& get_accumulator() const { return accumulator; }
      const Q& get_priority_queue() const { return *priority_queue; }
      const InternalStats& get_internal_stats() const { return internal_stats; }

    protected:

      void inner_post(const ClientId& client,
		      std::unique_ptr<TestRequest> request,
		      const RespPm& additional,
		      const Cost request_cost) {
	Lock l(inner_queue_mtx);
	assert(!finishing);
	accum_f(accumulator, additional);
	inner_queue.emplace_back(QueueItem(client,
					   std::move(request),
					   additional,
					   request_cost));
	inner_queue_cv.notify_one();
      }

      void run(std::chrono::milliseconds check_period) {
	Lock l(inner_queue_mtx);
	while(true) {
	  while(inner_queue.empty() && !finishing) {
	    inner_queue_cv.wait_for(l, check_period);
	  }
	  if (!inner_queue.empty()) {
	    auto& front = inner_queue.front();
	    auto client = front.client;
	    auto req = std::move(front.request);
	    auto additional = front.additional;
	    auto request_cost = front.request_cost;
	    inner_queue.pop_front();

	    l.unlock();

	    // simulation operation by sleeping; then call function to
	    // notify server of completion
	    std::this_thread::sleep_for(op_time * request_cost);

	    // TODO: rather than assuming this constructor exists, perhaps
	    // pass in a function that does this mapping?
	    client_resp_f(client, TestResponse{req->epoch}, id, additional, request_cost);

	    time_stats(internal_stats.mtx,
		       internal_stats.request_complete_time,
		       [&](){
			 priority_queue->request_completed();
		       });
	    count_stats(internal_stats.mtx,
			internal_stats.request_complete_count);

	    l.lock(); // in prep for next iteration of loop
	  } else {
	    break;
	  }
	}
      }
    }; // class SimulatedServer

  }; // namespace qos_simulation
}; // namespace crimson
