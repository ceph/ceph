// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#define DEBUGGER


#pragma once


#include <assert.h>

#include <memory>
#include <map>
#include <deque>
#include <queue>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <iostream>

#include "crimson/heap.h"
#include "dmclock_util.h"
#include "dmclock_recs.h"


namespace c = crimson;


namespace crimson {

  namespace dmclock {

    struct ClientInfo {
      const double reservation;  // minimum
      const double weight;       // proportional
      const double limit;        // maximum

      // multiplicative inverses of above, which we use in calculations
      // and don't want to recalculate repeatedlu
      const double reservation_inv;
      const double weight_inv;
      const double limit_inv;

      ClientInfo(double _weight, double _reservation, double _limit) :
	reservation(_reservation),
	weight(_weight),
	limit(_limit),
	reservation_inv(0.0 == reservation ? 0.0 : 1.0 / reservation),
	weight_inv(     0.0 == weight      ? 0.0 : 1.0 / weight),
	limit_inv(      0.0 == limit       ? 0.0 : 1.0 / limit)
      {
	// empty
      }

      friend std::ostream& operator<<(std::ostream&, const ClientInfo&);
    }; // class ClientInfo


    std::ostream& operator<<(std::ostream& out,
			     const crimson::dmclock::ClientInfo& client);

    struct RequestTag {
      double reservation;
      double proportion;
      double limit;

      template<typename I>
      RequestTag(const RequestTag& prev_tag,
		 const ClientInfo& client,
		 const ReqParams<I>& req_params,
		 const Time& time) :
	reservation(tag_calc(time,
			     prev_tag.reservation,
			     client.reservation_inv,
			     req_params.rho)),
	proportion(tag_calc(time,
			    prev_tag.proportion,
			    client.weight_inv,
			    req_params.delta)),
	limit(tag_calc(time,
		       prev_tag.limit,
		       client.limit_inv,
		       req_params.delta))
      {
	// empty
      }

      RequestTag(double _res, double _prop, double _lim) :
	reservation(_res),
	proportion(_prop),
	limit(_lim)
      {
	// empty
      }

      RequestTag(const RequestTag& other) :
	reservation(other.reservation),
	proportion(other.proportion),
	limit(other.limit)
      {
	// empty
      }

    private:

      static double tag_calc(Time time,
			     double prev,
			     double increment,
			     uint32_t dist_req_val) {
	if (0 != dist_req_val) {
	  increment *= dist_req_val;
	}
	if (0.0 == increment) {
	  return 0.0;
	} else {
	  return std::max(time, prev + increment);
	}
      }

      friend std::ostream& operator<<(std::ostream&, const RequestTag&);
    }; // class RequestTag


    std::ostream& operator<<(std::ostream& out,
			     const crimson::dmclock::RequestTag& tag);


    // C is client identifier type, R is request type
    template<typename C, typename R>
    class PriorityQueue {

    public:

      using RequestRef = std::unique_ptr<R>;

    protected:

      class ClientRec {
	friend PriorityQueue<C,R>;

	ClientInfo         info;
	RequestTag         prev_tag;
	bool               idle;
	Counter            last_tick;

	ClientRec(const ClientInfo& _info, Counter current_tick) :
	  info(_info),
	  prev_tag(0.0, 0.0, 0.0),
	  idle(true),
	  last_tick(current_tick)
	{
	  // empty
	}
      }; // class ClientRec


      class Entry {
	friend PriorityQueue<C,R>;

	C          client;
	RequestTag tag;
	RequestRef request;
	bool       handled;

      public:

	Entry(C _client, RequestTag _tag, RequestRef&& _request) :
	  client(_client),
	  tag(_tag),
	  request(std::move(_request)),
	  handled(false)
	{
	  // empty
	}

	friend
	std::ostream& operator<<(std::ostream& out,
				 const typename PriorityQueue<C,R>::Entry& e) {
	  out << "{ client:" << e.client <<
	    ", tag:" << e.tag <<
	    ", handled:" << (e.handled ? "T" : "f") << " }";
	  return out;
	}
      }; // struct Entry


      using EntryRef = std::shared_ptr<Entry>;

      // if you try to display an EntryRef (shared pointer to an
      // Entry), dereference the shared pointer so we get data, not
      // addresses
      friend
      std::ostream& operator<<(std::ostream& out,
			       const typename PriorityQueue<C,R>::EntryRef& e) {
	out << *e;
	return out;
      }

    public:

      // a function that can be called to look up client information
      using ClientInfoFunc = std::function<ClientInfo(C)>;

      // a function to see whether the server can handle another request
      using CanHandleRequestFunc = std::function<bool(void)>;

      // a function to submit a request to the server; the second
      // parameter is a callback when it's completed
      using HandleRequestFunc =
	       std::function<void(const C&,RequestRef,PhaseType)>;

    protected:

      struct ReservationCompare {
	bool operator()(const EntryRef& n1, const EntryRef& n2) const {
	  assert(n1->tag.reservation > 0 && n2->tag.reservation > 0);
	  return n1->tag.reservation < n2->tag.reservation;
	}
      };

      struct ProportionCompare {
	bool operator()(const EntryRef& n1, const EntryRef& n2) const {
	  assert(n1->tag.proportion > 0 && n2->tag.proportion > 0);
	  return n1->tag.proportion < n2->tag.proportion;
	}
      };

      struct LimitCompare {
	bool operator()(const EntryRef& n1, const EntryRef& n2) const {
	  assert(n1->tag.limit > 0 && n2->tag.limit > 0);
	  return n1->tag.limit < n2->tag.limit;
	}
      };


      ClientInfoFunc       client_info_f;
      CanHandleRequestFunc can_handle_f;
      HandleRequestFunc    handle_f;

      mutable std::mutex data_mutex;
      using DataGuard = std::lock_guard<decltype(data_mutex)>;

      // stable mappiing between client ids and client queues
      std::map<C,ClientRec> client_map;

      // four heaps that maintain the earliest request by each of the
      // tag components
      c::Heap<EntryRef, ReservationCompare> res_q;
      c::Heap<EntryRef, ProportionCompare> prop_q;

      // AKA not-ready queue
      c::Heap<EntryRef, LimitCompare> lim_q;

      // for entries whose limit is passed and that'll be sorted by
      // their proportion tag
      c::Heap<EntryRef, ProportionCompare> ready_q;

      // if all reservations are met and all other requestes are under
      // limit, this will allow the request next in terms of
      // proportion to still get issued
      bool allowLimitBreak;

      std::atomic_bool finishing = false;

      // for handling timed scheduling
      std::mutex  sched_ahead_mtx;
      std::condition_variable sched_ahead_cv;
      std::thread sched_ahead_thd;
      Time sched_ahead_when = TimeZero;

      // every request creates a tick
      Counter tick = 0;

      // performance data collection
      size_t res_sched_count = 0;
      size_t prop_sched_count = 0;
      size_t limit_break_sched_count = 0;

    public:

      PriorityQueue(ClientInfoFunc _client_info_f,
		    CanHandleRequestFunc _can_handle_f,
		    HandleRequestFunc _handle_f,
		    bool _allowLimitBreak = false) :
	client_info_f(_client_info_f),
	can_handle_f(_can_handle_f),
	handle_f(_handle_f),
	allowLimitBreak(_allowLimitBreak)
      {
	sched_ahead_thd = std::thread(&PriorityQueue::run_sched_ahead, this);
      }


      ~PriorityQueue() {
	finishing = true;
	sched_ahead_cv.notify_one();
	sched_ahead_thd.join();
      }


      void mark_as_idle(const C& client_id) {
	auto client_it = client_map.find(client_id);
	if (client_map.end() != client_it) {
	  client_it->second.idle = true;
	}
      }


      void add_request(const R& request,
		      const ReqParams<C>& req_params,
		      const Time& time) {
	add_request(RequestRef(new R(request)), req_params, time);
      }


      void add_request(RequestRef&& request,
		      const ReqParams<C>& req_params,
		      const Time& time) {
#if 0
	{
	  static std::atomic_ulong counter(0);
	  ++counter;
	  uint32_t counter2 = counter.load();
	  if (counter2 >= 200 && counter2 < 220) {
	    std::cout << req_params << std::endl;
	  }
	}
#endif
	const C& client_id = req_params.client;
	DataGuard g(data_mutex);
	++tick;

	auto client_it = client_map.find(client_id);
	if (client_map.end() == client_it) {
	  ClientInfo ci = client_info_f(client_id);
	  client_map.emplace(client_id, ClientRec(ci));
	  client_it = client_map.find(client_id);
	}

	if (client_it->second.idle) {
	  while (!prop_q.empty() && prop_q.top()->handled) {
	    prop_q.pop();
	  }
	  if (!prop_q.empty()) {
	    double min_prop_tag = prop_q.top()->tag.proportion;
	    double reduction = min_prop_tag - time;
	    for (auto i = prop_q.begin(); i != prop_q.end(); ++i) {
	      (*i)->tag.proportion -= reduction;
	    }
	  }
	  client_it->second.idle = false;
	}

	EntryRef entry =
	  std::make_shared<Entry>(client_id,
				  RequestTag(client_it->second.prev_tag,
					     client_it->second.info,
					     req_params,
					     time),
				  std::move(request));

	// copy tag to previous tag for client
	client_it->second.prev_tag = entry->tag;

	if (0.0 != entry->tag.reservation) {
	  res_q.push(entry);
	}

	if (0.0 != entry->tag.proportion) {
	  prop_q.push(entry);

	  if (0.0 == entry->tag.limit) {
	    ready_q.push(entry);
	  } else {
	    lim_q.push(entry);
	  }
	}

#if 0
	{
	  static uint count = 0;
	  ++count;
	  if (50 <= count && count < 55) {
	    std::cout << "add_request:" << std::endl;
	    std::cout << "time:" << format_time(time) << std::endl;
	    displayQueues();
	    std::cout << std::endl;
	    debugger();
	  }
	}
#endif

	schedule_request();
      }


      void request_completed() {
	DataGuard g(data_mutex);
	schedule_request();
      }


    protected:

      // for debugging
      void display_queues() {
	auto filter = [](const EntryRef& e)->bool { return !e->handled; };
	res_q.displaySorted(std::cout << "RESER:", filter) << std::endl;
	lim_q.displaySorted(std::cout << "LIMIT:", filter) << std::endl;
	ready_q.displaySorted(std::cout << "READY:", filter) << std::endl;
	prop_q.displaySorted(std::cout << "PROPO:", filter) << std::endl;
      }


      // data_mutex should be held when called
      void reduce_reservation_tags(C client_id) {
	auto client_it = client_map.find(client_id);
	assert(client_map.end() != client_it);
	double reduction = client_it->second.info.reservation_inv;
	for (auto i = res_q.begin(); i != res_q.end(); ++i) {
	  if ((*i)->client == client_id) {
	    (*i)->tag.reservation -= reduction;
	    i.increase(); // since tag goes down, priority increases
	  }
	}
      }


      // data_mutex should be held when called; furthermore, the heap
      // should not be empty and the top element of the heap should
      // not be already handled
      template<typename K>
      C submit_top_request(Heap<EntryRef, K>& heap, PhaseType phase) {
	EntryRef& top = heap.top();
	top->handled = true;
	handle_f(top->client, std::move(top->request), phase);
	C client_result = top->client;
	heap.pop();
	return client_result;
      }


      // data_mutex should be held when called
      template<typename K>
      void prepare_queue(Heap<EntryRef, K>& heap) {
	while (!heap.empty() && heap.top()->handled) {
	  heap.pop();
	}
      }


      // data_mutex should be held when called
      void schedule_request() {
	if (!can_handle_f()) {
	  return;
	}

	Time now = get_time();

	// so queue management is handled incrementally, remove
	// handled items from each of the queues
	prepare_queue(res_q);
	prepare_queue(ready_q);
	prepare_queue(lim_q);
	prepare_queue(prop_q);

	// try constraint (reservation) based scheduling

#if 0
	{
	  static uint count = 0;
	  ++count;
	  if (50 <= count && count <= 55) {
	    std::cout << "schedule_request A:" << std::endl;
	    std::cout << "now:" << format_time(now) << std::endl;
	    display_queues();
	    std::cout << std::endl;
	    debugger();
	  }
	}
#endif

	if (!res_q.empty() && res_q.top()->tag.reservation <= now) {
	  (void) submit_top_request(res_q, PhaseType::reservation);
	  ++res_sched_count;
	  return;
	}

	// no existing reservations before now, so try weight-based
	// scheduling

	// all items that are within limit are eligible based on
	// priority
	while (!lim_q.empty()) {
	  auto top = lim_q.top();
	  if (top->handled) {
	    lim_q.pop();
	  } else if (top->tag.limit <= now) {
	    ready_q.push(top);
	    lim_q.pop();
	  } else {
	    break;
	  }
	}

#if 0
	{
	  static uint count = 0;
	  ++count;
	  if (2000 <= count && count < 2002) {
	    std::cout << "schedule_request B:" << std::endl;
	    std::cout << "now:" << format_time(now) << std::endl;
	    display_queues();
	    std::cout << std::endl;
	    debugger();
	  }
	}
#endif

	if (!ready_q.empty()) {
	  C client = submit_top_request(ready_q, PhaseType::priority);
	  reduce_reservation_tags(client);
	  ++prop_sched_count;
	  return;
	}

	if (allowLimitBreak) {
	  if (!prop_q.empty()) {
	    C client = submit_top_request(prop_q, PhaseType::priority);
	    reduce_reservation_tags(client);
	    ++limit_break_sched_count;
	    return;
	  }
	}

	// nothing scheduled; make sure we re-run when next
	// reservation item or next limited item comes up

	Time next_call = TimeMax;
	if (!res_q.empty()) {
	  next_call = min_not_0_time(next_call, res_q.top()->tag.reservation);
	}
	if (!lim_q.empty()) {
	  next_call = min_not_0_time(next_call, lim_q.top()->tag.limit);
	}
	if (next_call < TimeMax) {
	  sched_at(next_call);
	}
      } // schedule_request


      // if possible is not zero and less than current then return it;
      // otherwise return current; the idea is we're trying to find
      // the minimal time but ignoring zero
      static inline const Time& min_not_0_time(const Time& current,
					       const Time& possible) {
	return TimeZero == possible ? current : std::min(current, possible);
      }


      // this is the thread that handles running schedule_request at
      // future times when nothing can be scheduled immediately
      void run_sched_ahead() {
	std::unique_lock<std::mutex> l(sched_ahead_mtx);

	while (!finishing) {
	  if (TimeZero == sched_ahead_when) {
	    sched_ahead_cv.wait(l);
	  } else {
	    Time now;
	    while (!finishing.load() && (now = get_time()) < sched_ahead_when) {
	      long microseconds_l = long(1 + 1000000 * (sched_ahead_when - now));
	      auto microseconds = std::chrono::microseconds(microseconds_l);
	      sched_ahead_cv.wait_for(l, microseconds);
	    }
	    sched_ahead_when = TimeZero;
	    l.unlock();
	    if (!finishing) {
	      DataGuard g(data_mutex);
	      schedule_request();
	    }
	    l.lock();
	  }
	}
      }


      void sched_at(Time when) {
	std::lock_guard<std::mutex> l(sched_ahead_mtx);
	if (TimeZero == sched_ahead_when || when < sched_ahead_when) {
	  sched_ahead_when = when;
	  sched_ahead_cv.notify_one();
	}
      }
    }; // class PriorityQueue
  } // namespace dmclock
} // namespace crimson
