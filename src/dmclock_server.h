// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#define DEBUGGER

/*
 * The prop_heap does not seem to be necessary. The only thing it
 * would help with is quickly finding the mininum proportion/prioity
 * when an idle client became active
 */
// #define USE_PROP_HEAP

#pragma once


#include <assert.h>

#include <cmath>
#include <memory>
#include <map>
#include <deque>
#include <queue>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <iostream>
#include <limits>

#include <boost/variant.hpp>

#include "indirect_intrusive_heap.h"
#include "run_every.h"
#include "dmclock_util.h"
#include "dmclock_recs.h"

#include "gtest/gtest_prod.h"


namespace crimson {

  namespace dmclock {

    namespace c = crimson;

    constexpr double max_tag = std::numeric_limits<double>::max();
    constexpr double min_tag = std::numeric_limits<double>::lowest();

    struct ClientInfo {
      const double reservation;  // minimum
      const double weight;       // proportional
      const double limit;        // maximum

      // multiplicative inverses of above, which we use in calculations
      // and don't want to recalculate repeatedly
      const double reservation_inv;
      const double weight_inv;
      const double limit_inv;

      // order parameters -- min, "normal", max
      ClientInfo(double _reservation, double _weight, double _limit) :
	reservation(_reservation),
	weight(_weight),
	limit(_limit),
	reservation_inv(0.0 == reservation ? 0.0 : 1.0 / reservation),
	weight_inv(     0.0 == weight      ? 0.0 : 1.0 / weight),
	limit_inv(      0.0 == limit       ? 0.0 : 1.0 / limit)
      {
	// empty
      }


      friend std::ostream& operator<<(std::ostream& out,
				      const ClientInfo& client) {
	out <<
	  "{ r:" << client.reservation <<
	  " w:" << client.weight <<
	  " l:" << client.limit <<
	  " 1/r:" << client.reservation_inv <<
	  " 1/w:" << client.weight_inv <<
	  " 1/l:" << client.limit_inv <<
	  " }";
	return out;
      }
    }; // class ClientInfo


    std::ostream& operator<<(std::ostream& out,
			     const crimson::dmclock::ClientInfo& client);

    struct RequestTag {
      double reservation;
      double proportion;
      double limit;
      bool   ready; // true when within limit

      RequestTag(const RequestTag& prev_tag,
		 const ClientInfo& client,
		 const ReqParams& req_params,
		 const Time& time) :
	reservation(tag_calc(time,
			     prev_tag.reservation,
			     client.reservation_inv,
			     req_params.rho,
			     true)),
	proportion(tag_calc(time,
			    prev_tag.proportion,
			    client.weight_inv,
			    req_params.delta,
			    true)),
	limit(tag_calc(time,
		       prev_tag.limit,
		       client.limit_inv,
		       req_params.delta,
		       false)),
	ready(false)
      {
	assert(reservation < max_tag || proportion < max_tag);
      }

      RequestTag(double _res, double _prop, double _lim) :
	reservation(_res),
	proportion(_prop),
	limit(_lim),
	ready(false)
      {
	assert(reservation < max_tag || proportion < max_tag);
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
			     uint32_t dist_req_val,
			     bool extreme_is_high) {
	if (0 != dist_req_val) {
	  increment *= dist_req_val;
	}
	if (0.0 == increment) {
	  return extreme_is_high ? max_tag : min_tag;
	} else {
	  return std::max(time, prev + increment);
	}
      }

      friend std::ostream& operator<<(std::ostream& out,
				      const RequestTag& tag) {
	out <<
	  "{ r:" << format_time(tag.reservation) <<
	  " p:" << format_time(tag.proportion) <<
	  " l:" << format_time(tag.limit) << " }";
	return out;
      }
    }; // class RequestTag


    std::ostream& operator<<(std::ostream& out,
			     const crimson::dmclock::RequestTag& tag);


    enum class QMechanism { push, pull };


    // C is client identifier type, R is request type
    template<typename C, typename R>
    class PriorityQueueBase {
      FRIEND_TEST(dmclock_server, client_idle_erase);

    public:

      using RequestRef = std::unique_ptr<R>;

    protected:

      using TimePoint = decltype(std::chrono::steady_clock::now());
      using Duration = std::chrono::milliseconds;
      using MarkPoint = std::pair<TimePoint,Counter>;

      enum class ReadyOption {ignore, lowers, raises};

      // forward decl for friend decls
      template<double RequestTag::*, ReadyOption, bool>
      struct ClientCompare;


      class ClientReq {
	friend PriorityQueueBase;

	RequestTag tag;
	C          client_id;
	RequestRef request;

      public:

	ClientReq(const RequestTag& _tag,
		  const C&          _client_id,
		  RequestRef&&      _request) :
	  tag(_tag),
	  client_id(_client_id),
	  request(std::move(_request))
	{
	  // empty
	}

	friend std::ostream& operator<<(std::ostream& out, const ClientReq& c) {
	  out << c.tag;
	  return out;
	}
      }; // class ClientReq


      class ClientRec {
	friend PriorityQueueBase<C,R>;

	C                     client;
	RequestTag            prev_tag;
	std::deque<ClientReq> requests;

	// amount added from the proportion tag as a result of
	// an idle client becoming unidle
	double                prop_delta = 0.0;

	c::IndIntruHeapData   reserv_heap_data;
	c::IndIntruHeapData   lim_heap_data;
	c::IndIntruHeapData   ready_heap_data;
	c::IndIntruHeapData   prop_heap_data;

      public:

	ClientInfo            info;
	bool                  idle;
	Counter               last_tick;

	ClientRec(C _client,
		  const ClientInfo& _info,
		  Counter current_tick) :
	  client(_client),
	  prev_tag(0.0, 0.0, 0.0),
	  info(_info),
	  idle(true),
	  last_tick(current_tick)
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

	inline void add_request(const RequestTag& tag,
				const C&          client_id,
				RequestRef&&      request) {
	  requests.emplace_back(ClientReq(tag, client_id, std::move(request)));
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

	friend std::ostream&
	operator<<(std::ostream& out,
		   const typename PriorityQueueBase<C,R>::ClientRec& e) {
	  out << "{ client:" << e.client << " top req: " <<
	    (e.has_request() ? e.next_request() : "none") << " }";
	  return out;
	}
      }; // class ClientRec


      using ClientRecRef = std::shared_ptr<ClientRec>;


    public:

      // when we try to get the next request, we'll be in one of three
      // situations -- we'll have one to return, have one that can
      // fire in the future, or not have any
      enum class NextReqType { returning, future, none };

      // specifies which queue next request will get popped from
      enum class HeapId { reservation, ready
#if USE_PROP_HEAP
	  , proportional
#endif
	  };


      // this is returned from next_req to tell the caller the situation
      struct NextReq {
	NextReqType type;
	union {
	  HeapId    heap_id;
	  Time      when_ready;
	};
      };


      // a function that can be called to look up client information
      using ClientInfoFunc = std::function<ClientInfo(C)>;

    protected:

      // The ClientCompare functor is essentially doing a precedes?
      // operator, returning true if and only if the first parameter
      // must precede the second parameter. If the second must precede
      // the first, or if they are equivalent, false should be
      // returned. The reason for this behavior is that it will be
      // called to test if two items are out of order and if true is
      // returned it will reverse the items. Therefore false is the
      // default return when it doesn't matter to prevent unnecessary
      // re-ordering.
      //
      // The template is supporting variations in sorting based on the
      // heap in question and allowing these variations to be handled
      // at compile-time.
      //
      // tag_field determines which tag is being used for comparison
      //
      // ready_opt determines how the ready flag influences the sort
      //
      // use_prop_delta determines whether the proportial delta is
      // added in for comparison
      template<double RequestTag::*tag_field,
	       ReadyOption ready_opt,
	       bool use_prop_delta>
      struct ClientCompare {
	bool operator()(const ClientRec& n1, const ClientRec& n2) const {
	  if (n1.has_request()) {
	    if (n2.has_request()) {
	      const auto& t1 = n1.next_request().tag;
	      const auto& t2 = n2.next_request().tag;
	      if (ReadyOption::ignore == ready_opt || t1.ready == t2.ready) {
		// if we don't care about ready or the ready values are the same
		if (use_prop_delta) {
		  return (t1.*tag_field + n1.prop_delta) <
		    (t2.*tag_field + n2.prop_delta);
		} else {
		  return t1.*tag_field < t2.*tag_field;
		}
	      } else if (ReadyOption::raises == ready_opt) {
		// use_ready == true && the ready fields are different
		return t1.ready;
	      } else {
		return t2.ready;
	      }
	    } else {
	      // n1 has request but n2 does not
	      return true;
	    }
	  } else if (n2.has_request()) {
	    // n2 has request but n1 does not
	    return false;
	  } else {
	    // both have none; keep stable w false
	    return false;
	  }
	}
      };

      ClientInfoFunc       client_info_f;

      mutable std::mutex data_mtx;
      using DataGuard = std::lock_guard<decltype(data_mtx)>;

      // stable mappiing between client ids and client queues
      std::map<C,ClientRecRef> client_map;


      c::IndIntruHeap<ClientRecRef,
		      ClientRec,
		      &ClientRec::reserv_heap_data,
		      ClientCompare<&RequestTag::reservation,
				    ReadyOption::ignore,
				    false>> resv_heap;
#if USE_PROP_HEAP
      c::IndIntruHeap<ClientRecRef,
		      ClientRec,
		      &ClientRec::prop_heap_data,
		      ClientCompare<&RequestTag::proportion,
				    ReadyOption::ignore,
				    true>> prop_heap;
#endif
      c::IndIntruHeap<ClientRecRef,
		      ClientRec,
		      &ClientRec::lim_heap_data,
		      ClientCompare<&RequestTag::limit,
				    ReadyOption::lowers,
				    false>> limit_heap;
      c::IndIntruHeap<ClientRecRef,
		      ClientRec,
		      &ClientRec::ready_heap_data,
		      ClientCompare<&RequestTag::proportion,
				    ReadyOption::raises,
				    true>> ready_heap;

      // if all reservations are met and all other requestes are under
      // limit, this will allow the request next in terms of
      // proportion to still get issued
      bool             allow_limit_break;

      std::atomic_bool finishing;

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

      // NB: All threads declared at end, so they're destructed first!

      std::unique_ptr<RunEvery> cleaning_job;


      // COMMON constructor that others feed into; we can accept three
      // different variations of durations
      template<typename Rep, typename Per>
      PriorityQueueBase(ClientInfoFunc _client_info_f,
			std::chrono::duration<Rep,Per> _idle_age,
			std::chrono::duration<Rep,Per> _erase_age,
			std::chrono::duration<Rep,Per> _check_time,
			bool _allow_limit_break) :
	client_info_f(_client_info_f),
	allow_limit_break(_allow_limit_break),
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
			 std::bind(&PriorityQueueBase::do_clean, this)));
      }


      ~PriorityQueueBase() {
	finishing = true;
      }


      // data_mtx must be held by caller
      void do_add_request(RequestRef&&     request,
			  const C&         client_id,
			  const ReqParams& req_params,
			  const Time       time) {
	++tick;

	// this pointer will help us create a reference to a shared
	// pointer, no matter which of two codepaths we take
	ClientRec* temp_client;
	
	auto client_it = client_map.find(client_id);
	if (client_map.end() != client_it) {
	  temp_client = &(*client_it->second); // address of obj of shared_ptr
	} else {
	  ClientInfo info = client_info_f(client_id);
	  ClientRecRef client_rec =
	    std::make_shared<ClientRec>(client_id, info, tick);
	  resv_heap.push(client_rec);
#if USE_PROP_HEAP
	  prop_heap.push(client_rec);
#endif
	  limit_heap.push(client_rec);
	  ready_heap.push(client_rec);
	  client_map[client_id] = client_rec;
	  temp_client = &(*client_rec); // address of obj of shared_ptr
	}

	// for convenience, we'll create a reference to the shared pointer
	ClientRec& client = *temp_client;

	if (client.idle) {
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
	  double lowest_prop_tag = NaN; // mark unset value as NaN
	  for (auto const &c : client_map) {
	    // don't use ourselves (or anything else that might be
	    // listed as idle) since we're now in the map
	    if (!c.second->idle) {
	      // use either lowest proportion tag or previous proportion tag
	      if (c.second->has_request()) {
		double p = c.second->next_request().tag.proportion +
		  c.second->prop_delta;
		if (isnan(lowest_prop_tag) || p < lowest_prop_tag) {
		  lowest_prop_tag = p;
		}
	      }
	    }
	  }
	  if (!isnan(lowest_prop_tag)) {
	    client.prop_delta = lowest_prop_tag - time;
	  }
	  client.idle = false;
	} // if this client was idle

	RequestTag tag(client.get_req_tag(), client.info, req_params, time);
	client.add_request(tag, client.client, std::move(request));

	// copy tag to previous tag for client
	client.update_req_tag(tag, tick);

	resv_heap.adjust(client);
	limit_heap.adjust(client);
	ready_heap.adjust(client);
#if USE_PROP_HEAP
	prop_heap.adjust(client);
#endif
      } // add_request


      // data_mtx should be held when called; top of heap should have
      // a ready request
      template<typename C1, IndIntruHeapData ClientRec::*C2, typename C3>
      void pop_process_request(IndIntruHeap<C1, ClientRec, C2, C3>& heap,
			       std::function<void(const C& client,
						  RequestRef& request)> process) {
	// gain access to data
	ClientRec& top = heap.top();
	ClientReq& first = top.next_request();
	RequestRef request = std::move(first.request);

	// pop request and adjust heaps
	top.pop_request();
	resv_heap.demote(top);
	limit_heap.demote(top);
#if USE_PROP_HEAP
	prop_heap.demote(top);
#endif
	ready_heap.demote(top);

	// process
	process(top.client, request);
      } // pop_process_request


      // for debugging
      void display_queues(bool show_res = true,
			  bool show_lim = true,
			  bool show_ready = true,
			  bool show_prop = true) {
	auto filter = [](const ClientRecRef& e)->bool { return !e->handled; };
	if (show_res) {
	  resv_heap.display_sorted(std::cout << "RESER:", filter) << std::endl;
	}
	if (show_lim) {
	  limit_heap.display_sorted(std::cout << "LIMIT:", filter) << std::endl;
	}
	if (show_ready) {
	  ready_heap.display_sorted(std::cout << "READY:", filter) << std::endl;
	}
#if USE_PROP_HEAP
	if (show_prop) {
	  prop_heap.display_sorted(std::cout << "PROPO:", filter) << std::endl;
	}
#endif
      }


      // data_mtx should be held when called
      void reduce_reservation_tags(ClientRec& client) {
	for (auto& r : client.requests) {
	  r.tag.reservation -= client.info.reservation_inv;
	}
	// don't forget to update previous tag
	client.prev_tag.reservation -= client.info.reservation_inv;
	resv_heap.promote(client);
      }


      // data_mtx should be held when called
      void reduce_reservation_tags(const C& client_id) {
	auto client_it = client_map.find(client_id);

	// means the client was cleaned from map; should never happen
	// as long as cleaning times are long enough
	assert(client_map.end() != client_it);
	reduce_reservation_tags(*client_it->second);
      }


      // data_mtx should be held when called
      NextReq next_request(Time now) {
	NextReq result;
	
	// if reservation queue is empty, all are empty (i.e., no active clients)
	if(resv_heap.empty()) {
	  result.type = NextReqType::none;
	  return result;
	}

	// try constraint (reservation) based scheduling

	auto& reserv = resv_heap.top();
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
	auto limits = &limit_heap.top();
	while (limits->has_request() &&
	       !limits->next_request().tag.ready &&
	       limits->next_request().tag.limit <= now) {
	  limits->next_request().tag.ready = true;
	  ready_heap.promote(*limits);
	  limit_heap.demote(*limits);

	  limits = &limit_heap.top();
	}

	auto& readys = ready_heap.top();
	if (readys.has_request() &&
	    readys.next_request().tag.ready &&
	    readys.next_request().tag.proportion < max_tag) {
	  result.type = NextReqType::returning;
	  result.heap_id = HeapId::ready;
	  return result;
	}

	// if nothing is schedulable by reservation or
	// proportion/weight, and if we allow limit break, try to
	// schedule something with the lowest proportion tag or
	// alternatively lowest reservation tag.
	if (allow_limit_break) {
	  if (readys.has_request() &&
	      readys.next_request().tag.proportion < max_tag) {
	    result.type = NextReqType::returning;
	    result.heap_id = HeapId::ready;
	    return result;
	  } else if (reserv.has_request() &&
		     reserv.next_request().tag.reservation < max_tag) {
	    result.type = NextReqType::returning;
	    result.heap_id = HeapId::reservation;
	    return result;
	  }
	}

	// nothing scheduled; make sure we re-run when next
	// reservation item or next limited item comes up

	Time next_call = TimeMax;
	if (resv_heap.top().has_request()) {
	  next_call =
	    min_not_0_time(next_call,
			   resv_heap.top().next_request().tag.reservation);
	}
	if (limit_heap.top().has_request()) {
	  const auto& next = limit_heap.top().next_request();
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
	  for (auto i = client_map.begin(); i != client_map.end(); /* empty */) {
	    auto i2 = i++;
	    if (erase_point && i2->second->last_tick <= erase_point) {
	      client_map.erase(i2);
	      delete_from_heaps(i2->second);
	    } else if (idle_point && i2->second->last_tick <= idle_point) {
	      i2->second->idle = true;
	    }
	  } // for
	} // if
      } // do_clean


      template<IndIntruHeapData ClientRec::*C1,typename C2>
      void delete_from_heap(ClientRecRef& client,
			    c::IndIntruHeap<ClientRecRef,ClientRec,C1,C2>& heap) {
	auto i = heap.rfind(client);
	heap.remove(i);
      }


      void delete_from_heaps(ClientRecRef& client) {
	delete_from_heap(client, resv_heap);
#if USE_PROP_HEAP
	delete_from_heap(client, prop_heap);
#endif
	delete_from_heap(client, limit_heap);
	delete_from_heap(client, ready_heap);
      }
    }; // class PriorityQueueBase


    // PULL version
    template<typename C, typename R, QMechanism M>
    class PriorityQueue : public PriorityQueueBase<C,R> {
      using super = PriorityQueueBase<C,R>;

    public:

      // When a request is pulled, this is the return type.
      struct PullReq {
	struct Retn {
	  C           client;
	  typename super::RequestRef  request;
	  PhaseType   phase;
	};

	typename super::NextReqType        type;
	boost::variant<Retn,Time> data;
      };


      template<typename Rep, typename Per>
      PriorityQueue(typename super::ClientInfoFunc _client_info_f,
		    std::chrono::duration<Rep,Per> _idle_age,
		    std::chrono::duration<Rep,Per> _erase_age,
		    std::chrono::duration<Rep,Per> _check_time,
		    bool _allow_limit_break = false) :
	super(_client_info_f,
	      _idle_age, _erase_age, _check_time,
	      _allow_limit_break)
      {
	// empty
      }


      // pull convenience constructor
      PriorityQueue(typename super::ClientInfoFunc _client_info_f,
		    bool _allow_limit_break = false) :
	PriorityQueue(_client_info_f,
		      std::chrono::minutes(10),
		      std::chrono::minutes(15),
		      std::chrono::minutes(6),
		      _allow_limit_break)
      {
	// empty
      }


      void add_request(const R& request,
		       const C& client_id,
		       const ReqParams& req_params) {
	add_request(typename super::RequestRef(new R(request)),
		    client_id,
		    req_params,
		    get_time());
      }


      void add_request(typename super::RequestRef&& request,
		       const C& client_id,
		       const ReqParams& req_params) {
	add_request(request, req_params, client_id, get_time());
      }


      void add_request(const R& request,
		       const C& client_id,
		       const ReqParams& req_params,
		       const Time time) {
	add_request(typename super::RequestRef(new R(request)),
		    client_id,
		    req_params,
		    time);
      }


      void add_request(typename super::RequestRef&&     request,
		       const C&         client_id,
		       const ReqParams& req_params,
		       const Time       time) {
	typename super::DataGuard g(super::data_mtx);
	super::do_add_request(std::move(request), client_id, req_params, time);
	// no call to schedule_request for pull version
      }


      PullReq pull_request() {
	return pull_request(get_time());
      }


      PullReq pull_request(Time now) {
	PullReq result;
	typename super::DataGuard g(super::data_mtx);

	typename super::NextReq next = super::next_request(now);
	result.type = next.type;
	switch(next.type) {
	case super::NextReqType::none:
	  return result;
	  break;
	case super::NextReqType::future:
	  result.data = next.when_ready;
	  return result;
	  break;
	case super::NextReqType::returning:
	  // to avoid nesting, break out and let code below handle this case
	  break;
	default:
	  assert(false);
	}

	// we'll only get here if we're returning an entry

	auto process_f =
	  [&] (PullReq& pull_result, PhaseType phase) ->
	  std::function<void(const C&,
			     typename super::RequestRef&)> {
	  return [&pull_result, phase](const C& client,
				       typename super::RequestRef& request) {
	    pull_result.data =
	    typename PullReq::Retn{client, std::move(request), phase};
	  };
	};

	switch(next.heap_id) {
	case super::HeapId::reservation:
	  super::pop_process_request(super::resv_heap,
				     process_f(result, PhaseType::reservation));
	  ++super::reserv_sched_count;
	  break;
	case super::HeapId::ready:
	{
	  super::pop_process_request(super::ready_heap,
				     process_f(result, PhaseType::priority));
	  auto& retn = boost::get<typename PullReq::Retn>(result.data);
	  super::reduce_reservation_tags(retn.client);
	  ++super::prop_sched_count;
	}
	break;
#if USE_PROP_HEAP
	case super::HeapId::proportional:
	{
	  super::pop_process_request(prop_heap, process_f(result, PhaseType::priority));
	  auto& retn = boost::get<typename PullReq::Retn>(result.data);
	  super::reduce_reservation_tags(retn.client);
	  ++super::limit_break_sched_count;
	}
	break;
#endif
	default:
	  assert(false);
	}

	return result;
      } // pull_request


    protected:


      // data_mtx should be held when called; unfortunately this
      // function has to be repeated in both push & pull
      // specializations
      typename super::NextReq next_request() {
	return next_request(get_time());
      }
    };


    // PUSH version
    template<typename C, typename R>
    class PriorityQueue<C,R,QMechanism::push> : public PriorityQueueBase<C,R> {

    protected:

      using super = PriorityQueueBase<C,R>;

    public:

      // a function to see whether the server can handle another request
      using CanHandleRequestFunc = std::function<bool(void)>;

      // a function to submit a request to the server; the second
      // parameter is a callback when it's completed
      using HandleRequestFunc =
	std::function<void(const C&,typename super::RequestRef,PhaseType)>;

    protected:

      CanHandleRequestFunc can_handle_f;
      HandleRequestFunc    handle_f;
      // for handling timed scheduling
      std::mutex  sched_ahead_mtx;
      std::condition_variable sched_ahead_cv;
      Time sched_ahead_when = TimeZero;

      // NB: threads declared last, so constructed last and destructed first

      std::thread sched_ahead_thd;

    public:

      // push full constructor
      template<typename Rep, typename Per>
      PriorityQueue(typename super::ClientInfoFunc _client_info_f,
		    CanHandleRequestFunc _can_handle_f,
		    HandleRequestFunc _handle_f,
		    std::chrono::duration<Rep,Per> _idle_age,
		    std::chrono::duration<Rep,Per> _erase_age,
		    std::chrono::duration<Rep,Per> _check_time,
		    bool _allow_limit_break = false) :
	super(_client_info_f,
	      _idle_age, _erase_age, _check_time,
	      _allow_limit_break)
      {
	can_handle_f = _can_handle_f;
	handle_f = _handle_f;
	sched_ahead_thd = std::thread(&PriorityQueue::run_sched_ahead, this);
      }


      // push convenience constructor
      PriorityQueue(typename super::ClientInfoFunc _client_info_f,
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


      ~PriorityQueue() {
	sched_ahead_cv.notify_one();
	sched_ahead_thd.join();
      }

    public:

      void add_request(const R& request,
		       const C& client_id,
		       const ReqParams& req_params) {
	add_request(typename super::RequestRef(new R(request)),
		    client_id,
		    req_params,
		    get_time());
      }


      void add_request(typename super::RequestRef&& request,
		       const C& client_id,
		       const ReqParams& req_params) {
	add_request(request, req_params, client_id, get_time());
      }


      void add_request(const R& request,
		       const C& client_id,
		       const ReqParams& req_params,
		       const Time time) {
	add_request(typename super::RequestRef(new R(request)),
		    client_id,
		    req_params,
		    time);
      }


      void add_request(typename super::RequestRef&&     request,
		       const C&         client_id,
		       const ReqParams& req_params,
		       const Time       time) {
	typename super::DataGuard g(super::data_mtx);
	super::do_add_request(std::move(request), client_id, req_params, time);
	schedule_request();
      }


      void request_completed() {
	typename super::DataGuard g(super::data_mtx);
	schedule_request();
      }

    protected:

      // data_mtx should be held when called; furthermore, the heap
      // should not be empty and the top element of the heap should
      // not be already handled
      template<typename C1, IndIntruHeapData super::ClientRec::*C2, typename C3>
      C submit_top_request(IndIntruHeap<C1, typename super::ClientRec, C2, C3>& heap,
			   PhaseType phase) {
	C client_result;
	super::pop_process_request(heap,
				   [this, phase, &client_result]
				   (const C& client, typename super::RequestRef& request) {
				     client_result = client;
				     handle_f(client, std::move(request), phase);
				   });
	return client_result;
      }


      // data_mtx should be held when called
      void submit_request(typename super::HeapId heap_id) {
	C client;
	switch(heap_id) {
	case super::HeapId::reservation:
	  // don't need to note client
	  (void) submit_top_request(super::resv_heap, PhaseType::reservation);
	  // unlike the other two cases, we do not reduce reservation
	  // tags here
	  ++super::reserv_sched_count;
	  break;
	case super::HeapId::ready:
	  client = submit_top_request(super::ready_heap, PhaseType::priority);
	  super::reduce_reservation_tags(client);
	  ++super::prop_sched_count;
	  break;
#if USE_PROP_HEAP
	case super::HeapId::proportional:
	  client = submit_top_request(super::prop_heap, PhaseType::priority);
	  super::reduce_reservation_tags(client);
	  ++super::limit_break_sched_count;
	  break;
#endif
	default:
	  assert(false);
	}
      } // submit_request


      // data_mtx should be held when called; unfortunately this
      // function has to be repeated in both push & pull
      // specializations
      typename super::NextReq next_request() {
	return next_request(get_time());
      }


      // data_mtx should be held when called; overrides member
      // function in base class to add check for whether a request can
      // be pushed to the server
      typename super::NextReq next_request(Time now) {
	if (!can_handle_f()) {
	  typename super::NextReq result;
	  result.type = super::NextReqType::none;
	  return result;
	} else {
	  return super::next_request(now);
	}
      } // next_request


      // data_mtx should be held when called
      void schedule_request() {
	typename super::NextReq next_req = next_request();
	switch (next_req.type) {
	case super::NextReqType::none:
	  return;
	case super::NextReqType::future:
	  sched_at(next_req.when_ready);
	  break;
	case super::NextReqType::returning:
	  submit_request(next_req.heap_id);
	  break;
	default:
	  assert(false);
	}
      }


      // this is the thread that handles running schedule_request at
      // future times when nothing can be scheduled immediately
      void run_sched_ahead() {
	std::unique_lock<std::mutex> l(sched_ahead_mtx);

	while (!super::finishing) {
	  if (TimeZero == sched_ahead_when) {
	    sched_ahead_cv.wait(l);
	  } else {
	    Time now;
	    while (!super::finishing && (now = get_time()) < sched_ahead_when) {
	      long microseconds_l = long(1 + 1000000 * (sched_ahead_when - now));
	      auto microseconds = std::chrono::microseconds(microseconds_l);
	      sched_ahead_cv.wait_for(l, microseconds);
	    }
	    sched_ahead_when = TimeZero;
	    if (super::finishing) return;

	    l.unlock();
	    if (!super::finishing) {
	      typename super::DataGuard g(super::data_mtx);
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
    };

  } // namespace dmclock
} // namespace crimson
