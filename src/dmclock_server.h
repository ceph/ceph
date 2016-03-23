// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#define DEBUGGER

#define OLD_Q 0

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

#include "boost/variant.hpp"

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
      bool   ready; // true when within limit

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
		       req_params.delta)),
	ready(false)
      {
	// empty
      }

      RequestTag(double _res, double _prop, double _lim) :
	reservation(_res),
	proportion(_prop),
	limit(_lim),
	ready(false)
      {
	// empty
      }

      RequestTag(const RequestTag& other) :
	reservation(other.reservation),
	proportion(other.proportion),
	limit(other.limit),
	ready(other.ready)
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

      enum class ReadyOption {ignore, lowers, raises};

      template<double RequestTag::*tag_field,
	       ReadyOption ready_opt=ReadyOption::ignore>
      struct ClientCompare; // forward decl for friend decls

      class ClientReq {
	friend ClientEntry;
	friend PriorityQueue;

	// NB: is there a better way than enumerating all possibilities?
	friend ClientCompare<&RequestTag::reservation,ReadyOption::ignore>;
	friend ClientCompare<&RequestTag::limit,ReadyOption::ignore>;
	friend ClientCompare<&RequestTag::proportion,ReadyOption::ignore>;
	friend ClientCompare<&RequestTag::reservation,ReadyOption::lowers>;
	friend ClientCompare<&RequestTag::limit,ReadyOption::lowers>;
	friend ClientCompare<&RequestTag::proportion,ReadyOption::lowers>;
	friend ClientCompare<&RequestTag::reservation,ReadyOption::raises>;
	friend ClientCompare<&RequestTag::limit,ReadyOption::raises>;
	friend ClientCompare<&RequestTag::proportion,ReadyOption::raises>;

	RequestTag          tag;
	RequestRef          request;

      public:

	ClientReq(const RequestTag& _tag,
		  RequestRef&&      _request) :
	  tag(_tag),
	  request(std::move(_request))
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
	friend ClientCompare<&RequestTag::reservation>;
	friend ClientCompare<&RequestTag::limit>;
	friend ClientCompare<&RequestTag::proportion>;

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
	  requests.emplace_back(ClientReq(tag, std::move(request)));
	}

	inline const ClientReq& next_request() const {
	  return requests.front();
	}

	inline ClientReq& next_request() {
	  return requests.front();
	}

	inline void pop_request() {
	  requests.pop_front();
	}

	inline bool has_request() const {
	  return !requests.empty();
	}

	friend
	std::ostream&
	operator<<(std::ostream& out,
		   const typename PriorityQueue<C,R>::ClientEntry& e) {
	  out << "{ client:" << e.client << " top req: " <<
	    (e.has_request() ? e.next_request() : "none") << " }";
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
      enum class NextReqType { returning, future, none };

      // unions with unique_ptrs are tricky and require manual calls
      // to destructors and placement new operators; it may be
      // worthwhile to revisit this implementation, but for now we'll
      // get rid of the union to avoid such issues
#if 0 
      struct PullReq {
	NextReqType type;
	union {
	  struct {
	    C&         client;
	    RequestRef request;
	    PhaseType  phase;
	  } retn;
	  Time when_next;
	};

	PullReq() {}
	~PullReq() {}
      };
#elif 0
      struct PullReq {
	NextReqType type;      // which type

	C           client;    // used for returning
	RequestRef  request;   // used for returning
	PhaseType   phase;     // used for returning
	Time        when_next; // used for future
      };
#else
      struct PullReq {
	struct Retn {
	  C           client;
	  RequestRef  request;
	  PhaseType   phase;
	};

	NextReqType type;
	boost::variant<Retn,Time> data;
      };
#endif


      // a function that can be called to look up client information
      using ClientInfoFunc = std::function<ClientInfo(C)>;

      // a function to see whether the server can handle another request
      using CanHandleRequestFunc = std::function<bool(void)>;

      // a function to submit a request to the server; the second
      // parameter is a callback when it's completed
      using HandleRequestFunc =
	       std::function<void(const C&,RequestRef,PhaseType)>;

    protected:

      template<double RequestTag::*tag_field, ReadyOption ready_opt>
      struct ClientCompare {
	bool operator()(const ClientEntry& n1, const ClientEntry& n2) const {
	  if (n1.has_request()) {
	    if (n2.has_request()) {
	      const auto& t1 = n1.next_request().tag;
	      const auto& t2 = n2.next_request().tag;
	      if (ReadyOption::ignore == ready_opt || t1.ready == t2.ready) {
		// if we don't care about ready or the ready values are the same
		return t1.*tag_field < t2.*tag_field;
	      } else if (ReadyOption::raises == ready_opt) {
		// use_ready == true && the ready fields are different
		return t1.ready;
	      } else {
		return t2.ready;
	      }
	    } else {
	      return true;
	    }
	  } else if (n2.has_request()) {
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
	NextReqType type;
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
		      ClientCompare<&RequestTag::reservation>> reserv_q;
      c::IndIntruHeap<ClientEntryRef,
		      ClientEntry,
		      &ClientEntry::prop_heap_data,
		      ClientCompare<&RequestTag::proportion>> prop_q;
      c::IndIntruHeap<ClientEntryRef,
		      ClientEntry,
		      &ClientEntry::lim_heap_data,
		      ClientCompare<&RequestTag::limit,ReadyOption::lowers>> limit_q;
      c::IndIntruHeap<ClientEntryRef,
		      ClientEntry,
		      &ClientEntry::ready_heap_data,
		      ClientCompare<&RequestTag::proportion,ReadyOption::raises>> ready_q;

#if 0
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
      bool             allow_limit_break;
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
	allow_limit_break(_allow_limit_break),
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
	const C& client_id = req_params.client;
	DataGuard g(data_mtx);
	++tick;

	auto client_it = client_map.find(client_id);
	if (client_map.end() == client_it) {
	  ClientInfo ci = client_info_f(client_id);
	  ClientEntryRef client_entry = std::make_shared<ClientEntry>(client_id);
	  reserv_q.push(client_entry);
	  prop_q.push(client_entry);
	  limit_q.push(client_entry);
	  ready_q.push(client_entry);
	  client_map.emplace(client_id, ClientRec(ci, tick, client_entry));
	  client_it = client_map.find(client_id);
	}

	if (client_it->second.idle) {
	  // We need to do an adjustment so that idle clients compete
	  // fairly on proportional tags since those tags may have
	  // drifted from real-time. Either use the lowest existing
	  // proportion tag -- O(1) -- or the client with the lowest
	  // previous proportion tag -- O(n) where n = # clients.
	  //
	  // So we don't have to maintain a propotional queue that
	  // keeps the minimum on proportional tag alone (we're
	  // instead using a ready queue), we'll have to check each
	  // client.
	  double lowest_prop_tag = -1.0;
	  for (auto const &c : client_map) {
	    // don't use ourselves since we're now in the map
	    if (c.first != client_it->first) {
	      const auto& entry = c.second.client_entry;
	      // use either lowest proportion tag or previous proportion tag
	      double p = entry->has_request() ?
		entry->next_request().tag.proportion :
		c.second.get_prev_prop_tag();
	      if (0.0 != p && (lowest_prop_tag < 0 || p < lowest_prop_tag)) {
		lowest_prop_tag = p;
	      }
	    }
	  }
	  if (lowest_prop_tag > 0.0) {
	    client_it->second.set_prev_prop_tag(lowest_prop_tag);
	  }
	  client_it->second.idle = false;
	} // if this client was idle

	ClientRec& client_rec = client_it->second;

	RequestTag tag(client_rec.get_req_tag(),
		       client_rec.info,
		       req_params,
		       time);
	client_rec.client_entry->add_request(tag,
					     std::move(request));

	// copy tag to previous tag for client
	client_rec.update_req_tag(tag, tick);

	reserv_q.adjust(*client_rec.client_entry);
	limit_q.adjust(*client_rec.client_entry);
	ready_q.adjust(*client_rec.client_entry);
	prop_q.adjust(*client_rec.client_entry);

	if (Mechanism::push == mechanism) {
	  schedule_request();
	}
      } // add_request


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
	result.type = next.type;
	switch(next.status) {
	case NextReqType::none:
	  return result;
	  break;
	case NextReqType::future:
	  result.when_next = next.when_ready;
	  return result;
	  break;
	case NextReqType::returning:
	  break;
	default:
	  assert(false);
	}

	// we'll only get here if we're returning an entry

	auto process_f =
	  [&] (PullReq& request,
	       PhaseType phase) -> std::function<void(const C&, RequestRef&)> {
	  return [&](const C& client, RequestRef& request) {
	    result.type = NextReqType::returning;
	    result.data =
	    typename PullReq::Retn{client, std::move(request), phase};
	  };
	};

	switch(next.heap_id) {
	case HeapId::reservation:
	  pop_process_request(reserv_q,
			      process_f(result, PhaseType::reservation));
	  ++reserv_sched_count;
	  break;
	case HeapId::ready:
	  pop_process_request(reserv_q, process_f(result, PhaseType::priority));
	  reduce_reservation_tags(result.client);
	  ++prop_sched_count;
	  break;
	case HeapId::proportional:
	  pop_process_request(reserv_q, process_f(result, PhaseType::priority));
	  reduce_reservation_tags(result.client);
	  ++limit_break_sched_count;
	  break;
	default:
	  assert(false);
	}

	return result;
      }


      // data_mtx should be held when called; top of heap should have
      // a ready request
      template<typename C1, IndIntruHeapData ClientEntry::*C2, typename C3>
      void pop_process_request(IndIntruHeap<C1, ClientEntry, C2, C3>& heap,
			       std::function<void(const C& client,
						  RequestRef& request)> process) {
	// gain access to data
	ClientEntry& top = heap.top();
	ClientReq& first = top.next_request();
	RequestRef request = std::move(first.request);

	// pop request and adjust heaps
	top.pop_request();
	reserv_q.adjust_down(top);
	limit_q.adjust_down(top);
	prop_q.adjust_down(top);
	ready_q.adjust_down(top);

	// process
	process(top.client, request);
      }


      // data_mtx should be held when called; furthermore, the heap
      // should not be empty and the top element of the heap should
      // not be already handled
      template<typename C1, IndIntruHeapData ClientEntry::*C2, typename C3>
      C submit_top_request(IndIntruHeap<C1, ClientEntry, C2, C3>& heap,
			   PhaseType phase) {
	C client_result;
	pop_process_request(heap,
			    [&] (const C& client, RequestRef& request) {
			      client_result = client;
			      handle_f(client, std::move(request), phase);
			    });
	return client_result;
      }


    protected:

      // for debugging
      void display_queues(bool show_res = true,
			  bool show_lim = true,
			  bool show_ready = true,
			  bool show_prop = true) {
	auto filter = [](const EntryRef& e)->bool { return !e->handled; };
	if (show_res) {
	  reserv_q.display_sorted(std::cout << "RESER:", filter) << std::endl;
	}
	if (show_lim) {
	  limit_q.display_sorted(std::cout << "LIMIT:", filter) << std::endl;
	}
	if (show_ready) {
	  ready_q.display_sorted(std::cout << "READY:", filter) << std::endl;
	}
	if (show_prop) {
	  prop_q.display_sorted(std::cout << "PROPO:", filter) << std::endl;
	}
      }


      // data_mtx should be held when called
      void reduce_reservation_tags(C client_id) {
	auto client_it = client_map.find(client_id);

	// means the client was cleaned from map; should never happen
	// as long as cleaning times are long enough
	if (client_map.end() == client_it) return;

#if OLD_Q // ************** TRANSLATE LATER ****************
	double reduction = client_it->second.info.reservation_inv;
	for (auto i = reserv_q.begin(); i != reserv_q.end(); ++i) {
	  if ((*i)->client == client_id) {
	    (*i)->tag.reservation -= reduction;
	    i.increase(); // since tag goes down, priority increases
	  }
	}
#endif
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
	switch (next_req.type) {
	case NextReqType::none:
	  return;
	case NextReqType::future:
	  sched_at(next_req.when_ready);
	  break;
	case NextReqType::returning:
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
	  result.type = NextReqType::none;
	  return result;
	}

	// if reservation queue is empty, all are empty (i.e., no active clients)
	if(reserv_q.empty()) {
	  result.type = NextReqType::none;
	  return result;
	}

	Time now = get_time();

	// try constraint (reservation) based scheduling

	auto& reserv = reserv_q.top();
	if (reserv.has_request() &&
	    reserv.next_request().tag.reservation <= now) {
	  result.type = NextReqType::returning;
	  result.heap_id = HeapId::reservation;
	  return result;
	}

	// no existing reservations before now, so try weight-based
	// scheduling

	// all items that are within limit are eligible based on
	// priority
	auto limits = &limit_q.top();
	while (limits->has_request() &&
	       !limits->next_request().tag.ready &&
	       limits->next_request().tag.limit <= now) {
	  limits->next_request().tag.ready = true;
	  ready_q.adjust_up(*limits);
	  limit_q.adjust_down(*limits);

	  limits = &limit_q.top();
	}

	auto readys = &ready_q.top();
	if (readys->has_request() &&
	    (readys->next_request().tag.ready || allow_limit_break)) {
	  result.type = NextReqType::returning;
	  result.heap_id = HeapId::ready;
	  return result;
	}

	// nothing scheduled; make sure we re-run when next
	// reservation item or next limited item comes up

	Time next_call = TimeMax;
	if (reserv_q.top().has_request()) {
	  next_call =
	    min_not_0_time(next_call,
			   reserv_q.top().next_request().tag.reservation);
	}
	if (limit_q.top().has_request()) {
	  const auto& next = limit_q.top().next_request();
	  assert(!next.tag.ready);
	  next_call = min_not_0_time(next_call, next.tag.limit);
	}
	if (next_call < TimeMax) {
	  result.type = NextReqType::future;
	  result.when_ready = next_call;
	  return result;
	} else {
	  result.type = NextReqType::none;
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
