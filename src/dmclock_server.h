// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
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

#include "heap.h"
#include "indirect_intrusive_heap.h"
#include "run_every.h"
#include "dmclock_util.h"
#include "dmclock_recs.h"

#include "gtest/gtest_prod.h"


namespace crimson {

  namespace dmclock {

    namespace c = crimson;

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

      static double tag_calc(const Time& time,
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
      FRIEND_TEST(dmclock_server, client_idle_erase);

    public:

      using RequestRef = std::unique_ptr<R>;

    protected:

      using TimePoint = decltype(std::chrono::steady_clock::now());
      using Duration = std::chrono::milliseconds;
      using MarkPoint = std::pair<TimePoint,Counter>;


      class Entry {
	friend PriorityQueue<C,R>;

	C                   client;
	RequestTag          tag;
	RequestRef          request;
	bool                handled;

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


      class ClientEntry; // forward decl for friend decls


      class ClientReq {
	friend ClientEntry;

	RequestTag          tag;
	RequestRef          request;

      public:

	ClientReq(const RequestTag& _tag,
		  RequestRef&&      _request) :
	  tag(_tag),
	  request(_request)
	{
	  // empty
	}

	friend std::ostream& operator<<(std::ostream& out, const ClientReq& c) {
	  out << c.tag;
	  return out;
	}
      };


      class ClientEntry {
	friend PriorityQueue<C,R>;

	C                     client;
	std::deque<ClientReq> requests;
	c::IndIntruHeapData   reserv_heap_data;
	c::IndIntruHeapData   lim_heap_data;
	c::IndIntruHeapData   ready_heap_data;
	c::IndIntruHeapData   prop_heap_data;

      public:

	ClientEntry(C _client) :
	  client(_client)
	{
	  // empty
	}


	inline void add_request(const RequestTag& tag, RequestRef&& request) {
	  requests.emplace_back(ClientReq(tag, request));
	}


	inline const ClientReq& next_request() const {
	  return requests.front();
	}


	inline void pop_request() {
	  requests.pop_front();
	}


	inline bool has_next_request() const {
	  return !requests.empty();
	}


	friend
	std::ostream& operator<<(std::ostream& out,
				 const typename PriorityQueue<C,R>::ClientEntry& e) {
	  out << "{ client:" << e.client << " top req: " <<
	    (e.has_requests() ? e.next_request() : "none") << " }";
	  return out;
	}
      }; // struct ClientEntry


      using EntryRef = std::shared_ptr<Entry>;
      using ClientEntryRef = std::shared_ptr<ClientEntry>;

      // if you try to display an EntryRef (shared pointer to an
      // Entry), dereference the shared pointer so we get data, not
      // addresses
      friend
      std::ostream& operator<<(std::ostream& out,
			       const typename PriorityQueue<C,R>::EntryRef& e) {
	out << *e;
	return out;
      }

      class ClientRec {
	// we're keeping this private to force callers to use
	// update_req_tag, to make sure the tick gets updated
	RequestTag         prev_tag;

      public:

	ClientInfo         info;
	bool               idle;
	Counter            last_tick;
	// TODO consider merging ClientEntry and ClientRec
	ClientEntryRef     client_entry;

	ClientRec(const ClientInfo& _info,
		  Counter current_tick,
		  const ClientEntryRef& _client_entry) :
	  prev_tag(0.0, 0.0, 0.0),
	  info(_info),
	  idle(true),
	  last_tick(current_tick),
	  client_entry(_client_entry)
	{
	  // empty
	}

	inline const RequestTag& get_req_tag() const {
	  return prev_tag;
	}

	inline void update_req_tag(const RequestTag& _prev,
				   const Counter& _tick) {
	  prev_tag = _prev;
	  last_tick = _tick;
	}

	inline double get_prev_prop_tag() const {
	  return prev_tag.proportion;
	}

	inline void set_prev_prop_tag(double value,
				      bool adjust_by_inc = false) {
	  prev_tag.proportion = value - (adjust_by_inc ? info.weight_inv : 0.0);
	}
      }; // class ClientRec


    public:

      enum class Mechanism { push, pull };

      // when we try to get the next request, we'll be in one of three
      // situations -- we'll have one to return, have one that can
      // fire in the future, or not have any
      enum class NextReqStat { returning, future, none };

      struct PullReq {
	NextReqStat status;
	union {
	  struct {
	    C&         client;
	    RequestRef request;
	    PhaseType  phase;
	  } retn;
	  Time when_next;
	};
      };

      // a function that can be called to look up client information
      using ClientInfoFunc = std::function<ClientInfo(C)>;

      // a function to see whether the server can handle another request
      using CanHandleRequestFunc = std::function<bool(void)>;

      // a function to submit a request to the server; the second
      // parameter is a callback when it's completed
      using HandleRequestFunc =
	       std::function<void(const C&,RequestRef,PhaseType)>;

    protected:

      template<double RequestTag::*tag>
      struct ClientCompare {
	bool operator()(const ClientEntry& n1, const ClientEntry& n2) const {
	  if (n1.has_requests()) {
	    if (n2.has_requests()) {
	      return n1.*tag < n2.*tag;
	    } else {
	      return true;
	    }
	  } else if (n2.has_requests()) {
	    return false;
	  } else {
	    return false; // both have none; keep stable w false
	  }
	}
      };

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

      // specifies which queue next request will get popped from
      enum class HeapId { reservation, ready, proportional };

      // this is returned from next_req to tell the caller the situation
      struct NextReq {
	NextReqStat status;
	union {
	  HeapId heap_id;
	  Time when_ready;
	};
      };


      ClientInfoFunc       client_info_f;
      CanHandleRequestFunc can_handle_f;
      HandleRequestFunc    handle_f;

      mutable std::mutex data_mtx;
      using DataGuard = std::lock_guard<decltype(data_mtx)>;

      // stable mappiing between client ids and client queues
      std::map<C,ClientRec> client_map;


      c::IndIntruHeap<ClientEntryRef,
		      ClientEntry,
		      &ClientEntry::reserv_heap_data,
		      ClientCompare<&RequestTag::reservation>> new_reserv_q;
      c::IndIntruHeap<ClientEntryRef,
		      ClientEntry,
		      &ClientEntry::prop_heap_data,
		      ClientCompare<&RequestTag::proportion>> new_prop_q;
      c::IndIntruHeap<ClientEntryRef,
		      ClientEntry,
		      &ClientEntry::lim_heap_data,
		      ClientCompare<&RequestTag::limit>> new_lim_q;
      c::IndIntruHeap<ClientEntryRef,
		      ClientEntry,
		      &ClientEntry::ready_heap_data,
		      ClientCompare<&RequestTag::proportion>> new_ready_q;

#if 1
      // four heaps that maintain the earliest request by each of the
      // tag components
      c::Heap<EntryRef, ReservationCompare> reserv_q;
      c::Heap<EntryRef, ProportionCompare> prop_q;

      // AKA not-ready queue
      c::Heap<EntryRef, LimitCompare> lim_q;

      // for entries whose limit is passed and that'll be sorted by
      // their proportion tag
      c::Heap<EntryRef, ProportionCompare> ready_q;
#endif

      // if all reservations are met and all other requestes are under
      // limit, this will allow the request next in terms of
      // proportion to still get issued
      bool             allowLimitBreak;
      Mechanism        mechanism;

      std::atomic_bool finishing;

      // for handling timed scheduling
      std::mutex  sched_ahead_mtx;
      std::condition_variable sched_ahead_cv;
      Time sched_ahead_when = TimeZero;

      // every request creates a tick
      Counter tick = 0;

      // performance data collection
      size_t reserv_sched_count = 0;
      size_t prop_sched_count = 0;
      size_t limit_break_sched_count = 0;

      Duration                  idle_age;
      Duration                  erase_age;
      Duration                  check_time;
      std::deque<MarkPoint>     clean_mark_points;

      // NB: All threads declared at end, so they're destructed firs!

      std::thread sched_ahead_thd;
      std::unique_ptr<RunEvery> cleaning_job;


      // COMMON constructor that others feed into; we can accept three
      // different variations of durations
      template<typename Rep, typename Per>
      PriorityQueue(ClientInfoFunc _client_info_f,
		    std::chrono::duration<Rep,Per> _idle_age,
		    std::chrono::duration<Rep,Per> _erase_age,
		    std::chrono::duration<Rep,Per> _check_time,
		    bool _allow_limit_break,
		    Mechanism _mechanism) :
	client_info_f(_client_info_f),
	allowLimitBreak(_allow_limit_break),
	mechanism(_mechanism),
	finishing(false),
	idle_age(std::chrono::duration_cast<Duration>(_idle_age)),
	erase_age(std::chrono::duration_cast<Duration>(_erase_age)),
	check_time(std::chrono::duration_cast<Duration>(_check_time))
      {
	assert(_erase_age >= _idle_age);
	assert(_check_time < _idle_age);
	cleaning_job =
	  std::unique_ptr<RunEvery>(
	    new RunEvery(check_time,
			 std::bind(&PriorityQueue::do_clean, this)));
      }

    public:

      // PUSH constructors -- full and convenience


      // push full constructor
      template<typename Rep, typename Per>
      PriorityQueue(ClientInfoFunc _client_info_f,
		    CanHandleRequestFunc _can_handle_f,
		    HandleRequestFunc _handle_f,
		    std::chrono::duration<Rep,Per> _idle_age,
		    std::chrono::duration<Rep,Per> _erase_age,
		    std::chrono::duration<Rep,Per> _check_time,
		    bool _allow_limit_break = false) :
	PriorityQueue(_client_info_f,
		      _idle_age, _erase_age, _check_time,
		      _allow_limit_break, Mechanism::push)
      {
	can_handle_f = _can_handle_f;
	handle_f = _handle_f;
	sched_ahead_thd = std::thread(&PriorityQueue::run_sched_ahead, this);
      }


      // push convenience constructor
      PriorityQueue(ClientInfoFunc _client_info_f,
		    CanHandleRequestFunc _can_handle_f,
		    HandleRequestFunc _handle_f,
		    bool _allow_limit_break = false) :
	PriorityQueue(_client_info_f,
		      _can_handle_f,
		      _handle_f,
		      std::chrono::minutes(10),
		      std::chrono::minutes(15),
		      std::chrono::minutes(6),
		      _allow_limit_break)
      {
	// empty
      }


      // PULL constructors -- full and convenience

      template<typename Rep, typename Per>
      PriorityQueue(ClientInfoFunc _client_info_f,
		    std::chrono::duration<Rep,Per> _idle_age,
		    std::chrono::duration<Rep,Per> _erase_age,
		    std::chrono::duration<Rep,Per> _check_time,
		    bool _allow_limit_break = false) :
	PriorityQueue(_client_info_f,
		      _idle_age, _erase_age, _check_time,
		      _allow_limit_break, Mechanism::pull)
      {
	// empty
      }


      // pull convenience constructor
      PriorityQueue(ClientInfoFunc _client_info_f,
		    bool _allow_limit_break = false) :
	PriorityQueue(_client_info_f,
		      std::chrono::minutes(10),
		      std::chrono::minutes(15),
		      std::chrono::minutes(6),
		      _allow_limit_break)
      {
	// empty
      }


      // NB: the reason the convenience constructors overload the
      // constructor rather than using default values for the timing
      // arguments is so that callers have to either use all defaults
      // or specify all timings. Mixing default and passed could be
      // problematic as they have to be consistent with one another.


    public:


      ~PriorityQueue() {
	finishing = true;
	if (Mechanism::push == mechanism) {
	  sched_ahead_cv.notify_one();
	  sched_ahead_thd.join();
	}
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
	DataGuard g(data_mtx);
	++tick;

	auto client_it = client_map.find(client_id);
	if (client_map.end() == client_it) {
	  ClientInfo ci = client_info_f(client_id);
	  ClientEntryRef client_entry = std::make_shared<ClientEntry>(client_id);
	  client_map.emplace(client_id, ClientRec(ci, tick, client_entry));
	  client_it = client_map.find(client_id);
	}

	if (client_it->second.idle) {
	  // remove all handled requests from proportional queue
	  while (!prop_q.empty() && prop_q.top()->handled) {
	    prop_q.pop();
	  }

	  // We need to do an adjustment so that idle clients compete
	  // fairly on proportional tags since those tags may have
	  // drifted from real-time. Either use the lowest existing
	  // proportion tag -- O(1) -- or the client with the lowest
	  // previous proportion tag -- O(n) where n = # clients.
	  if (!prop_q.empty()) {
	    double min_prop_tag = prop_q.top()->tag.proportion;
	    client_it->second.set_prev_prop_tag(min_prop_tag, true);
	  } else {
	    double lowest_prop_tag = -1.0;
	    for (auto const &c : client_map) {
	      // don't use ourselves since we're now in the map
	      if (c.first != client_it->first) {
		auto p = c.second.get_prev_prop_tag();
		if (0.0 != p && (lowest_prop_tag < 0 || p < lowest_prop_tag)) {
		    lowest_prop_tag = p;
		}
	      }
	    }
	    if (lowest_prop_tag > 0.0) {
	      client_it->second.set_prev_prop_tag(lowest_prop_tag);
	    }
	  }
	  client_it->second.idle = false;
	}

	EntryRef entry =
	  std::make_shared<Entry>(client_id,
				  RequestTag(client_it->second.get_req_tag(),
					     client_it->second.info,
					     req_params,
					     time),
				  std::move(request));

	// copy tag to previous tag for client
	client_it->second.update_req_tag(entry->tag, tick);

	if (0.0 != entry->tag.reservation) {
	  reserv_q.push(entry);
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

	if (Mechanism::push == mechanism) {
	  schedule_request();
	}
      }


      void request_completed() {
	if (Mechanism::push == mechanism) {
	  DataGuard g(data_mtx);
	  schedule_request();
	}
      }


      PullReq pull_request() {
	assert(Mechanism::pull == mechanism);

	PullReq result;
	DataGuard g(data_mtx);

	NextReq next = next_request();
	result.status = next.status;
	switch(next.status) {
	case NextReqStat::none:
	  return result;
	  break;
	case NextReqStat::future:
	  result.when_next = next.when_ready;
	  return result;
	  break;
	case NextReqStat::returning:
	  break;
	default:
	  assert(false);
	}

	// we'll only get here if we're returning an entry

	switch(next.heap_id) {
	case HeapId::reservation:
	  pull_request_help(result, reserv_q, PhaseType::reservation);
	  ++reserv_sched_count;
	  break;
	case HeapId::ready:
	  pull_request_help(result, ready_q, PhaseType::priority);
	  reduce_reservation_tags(result.client);
	  ++prop_sched_count;
	  break;
	case HeapId::proportional:
	  pull_request_help(result, prop_q, PhaseType::priority);
	  reduce_reservation_tags(result.client);
	  ++limit_break_sched_count;
	  break;
	default:
	  assert(false);
	}

	return result;
      }


      template<typename K>
      void pull_request_ehlp(PullReq& result,
			     Heap<EntryRef, K>& heap,
			     PhaseType phase) {
	EntryRef& top = heap.top();
	top->handled = true;
	result.retn.client = top->client;
	result.retn.request = std::move(top->request);
	result.retn.phase = phase;
	heap.pop();
      }

    protected:

      // for debugging
      void display_queues(bool show_res = true,
			  bool show_lim = true,
			  bool show_ready = true,
			  bool show_prop = true) {
	auto filter = [](const EntryRef& e)->bool { return !e->handled; };
	if (show_res) {
	  reserv_q.displaySorted(std::cout << "RESER:", filter) << std::endl;
	}
	if (show_lim) {
	  lim_q.displaySorted(std::cout << "LIMIT:", filter) << std::endl;
	}
	if (show_ready) {
	  ready_q.displaySorted(std::cout << "READY:", filter) << std::endl;
	}
	if (show_prop) {
	  prop_q.displaySorted(std::cout << "PROPO:", filter) << std::endl;
	}
      }


      // data_mtx should be held when called
      void reduce_reservation_tags(C client_id) {
	auto client_it = client_map.find(client_id);

	// means the client was cleaned from map; should never happen
	// as long as cleaning times are long enough
	if (client_map.end() == client_it) return;

	double reduction = client_it->second.info.reservation_inv;
	for (auto i = reserv_q.begin(); i != reserv_q.end(); ++i) {
	  if ((*i)->client == client_id) {
	    (*i)->tag.reservation -= reduction;
	    i.increase(); // since tag goes down, priority increases
	  }
	}
      }


      // data_mtx should be held when called; furthermore, the heap
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


      // data_mtx should be held when called
      template<typename K>
      void prepare_queue(Heap<EntryRef, K>& heap) {
	while (!heap.empty() && heap.top()->handled) {
	  heap.pop();
	}
      }


      // data_mtx should be held when called
      void schedule_request() {
	NextReq next_req = next_request();
	switch (next_req.status) {
	case NextReqStat::none:
	  return;
	case NextReqStat::future:
	  sched_at(next_req.when_ready);
	  break;
	case NextReqStat::returning:
	  submit_request(next_req.heap_id);
	  break;
	default:
	  assert(false);
	}
      }


      // data_mtx should be held when called
      void submit_request(HeapId heap_id) {
	C client;
	switch(heap_id) {
	case HeapId::reservation:
	  // don't need to note client
	  (void) submit_top_request(reserv_q, PhaseType::reservation);
	  // unlike the other two cases, we do not reduce reservation
	  // tags here
	  ++reserv_sched_count;
	  break;
	case HeapId::ready:
	  client = submit_top_request(ready_q, PhaseType::priority);
	  reduce_reservation_tags(client);
	  ++prop_sched_count;
	  break;
	case HeapId::proportional:
	  client = submit_top_request(prop_q, PhaseType::priority);
	  reduce_reservation_tags(client);
	  ++limit_break_sched_count;
	  break;
	default:
	  assert(false);
	}
      }


      // data_mtx should be held when called
      NextReq next_request() {
	NextReq result;
	
	if (Mechanism::push == mechanism && !can_handle_f()) {
	  result.status = NextReqStat::none;
	  return result;
	}

	Time now = get_time();

	// so queue management is handled incrementally, remove
	// handled items from each of the queues
	prepare_queue(reserv_q);
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

	if (!reserv_q.empty() && reserv_q.top()->tag.reservation <= now) {
	  result.status = NextReqStat::returning;
	  result.heap_id = HeapId::reservation;
	  return result;
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
	  result.status = NextReqStat::returning;
	  result.heap_id = HeapId::ready;
	  return result;
	}

	if (allowLimitBreak) {
	  if (!prop_q.empty()) {
	    result.status = NextReqStat::returning;
	    result.heap_id = HeapId::proportional;
	    return result;
	  }
	}

	// nothing scheduled; make sure we re-run when next
	// reservation item or next limited item comes up

	Time next_call = TimeMax;
	if (!reserv_q.empty()) {
	  next_call = min_not_0_time(next_call, reserv_q.top()->tag.reservation);
	}
	if (!lim_q.empty()) {
	  next_call = min_not_0_time(next_call, lim_q.top()->tag.limit);
	}
	if (next_call < TimeMax) {
	  result.status = NextReqStat::future;
	  result.when_ready = next_call;
	  return result;
	} else {
	  result.status = NextReqStat::none;
	  return result;
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
	    if (finishing) return;

	    l.unlock();
	    if (!finishing) {
	      DataGuard g(data_mtx);
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

    protected:

      /*
       * This is being called regularly by RunEvery. Every time it's
       * called it notes the time and delta counter (mark point) in a
       * deque. It also looks at the deque to find the most recent
       * mark point that is older than clean_age. It then walks the
       * map and delete all server entries that were last used before
       * that mark point.
       */
      void do_clean() {
	TimePoint now = std::chrono::steady_clock::now();
	DataGuard g(data_mtx);
	clean_mark_points.emplace_back(MarkPoint(now, tick));

	// first erase the super-old client records

	Counter erase_point = 0;
	auto point = clean_mark_points.front();
	while (point.first <= now - erase_age) {
	  erase_point = point.second;
	  clean_mark_points.pop_front();
	  point = clean_mark_points.front();
	}

	Counter idle_point = 0;
	for (auto i : clean_mark_points) {
	  if (i.first <= now - idle_age) {
	    idle_point = i.second;
	  } else {
	    break;
	  }
	}

	if (erase_point > 0 || idle_point > 0) {
	  for (auto i = client_map.begin();
	       i != client_map.end();
	       /* empty */) {
	    auto i2 = i++;
	    if (erase_point && i2->second.last_tick <= erase_point) {
	      client_map.erase(i2);
	    } else if (idle_point && i2->second.last_tick <= idle_point) {
	      i2->second.idle = true;
	    }
	  } // for
	} // if
      } // do_clean
    }; // class PriorityQueue
  } // namespace dmclock
} // namespace crimson
