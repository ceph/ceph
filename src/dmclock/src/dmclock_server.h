// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2017 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#pragma once

/* COMPILATION OPTIONS
 *
 * The prop_heap does not seem to be necessary. The only thing it
 * would help with is quickly finding the minimum proportion/prioity
 * when an idle client became active. To have the code maintain the
 * proportional heap, define USE_PROP_HEAP (i.e., compiler argument
 * -DUSE_PROP_HEAP).
 */

#include <assert.h>

#include <cmath>
#include <memory>
#include <map>
#include <deque>
#include <queue>
#ifndef WITH_CRIMSON
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#endif
#include <iostream>
#include <sstream>
#include <limits>
#include <variant>

#include "indirect_intrusive_heap.h"
#include "../support/src/run_every.h"
#include "dmclock_util.h"
#include "dmclock_recs.h"

#ifdef PROFILE
#include "profile.h"
#endif


namespace crimson {

  namespace dmclock {

    namespace c = crimson;

    constexpr double max_tag = std::numeric_limits<double>::is_iec559 ?
      std::numeric_limits<double>::infinity() :
      std::numeric_limits<double>::max();
    constexpr double min_tag = std::numeric_limits<double>::is_iec559 ?
      -std::numeric_limits<double>::infinity() :
      std::numeric_limits<double>::lowest();
    constexpr unsigned tag_modulo = 1000000;

    constexpr auto standard_idle_age  = std::chrono::seconds(300);
    constexpr auto standard_erase_age = std::chrono::seconds(600);
    constexpr auto standard_check_time = std::chrono::seconds(60);
    constexpr auto aggressive_check_time = std::chrono::seconds(5);
    constexpr unsigned standard_erase_max = 2000;

    enum class AtLimit {
      // requests are delayed until the limit is restored
      Wait,
      // requests are allowed to exceed their limit, if all other reservations
      // are met and below their limits
      Allow,
      // if an incoming request would exceed its limit, add_request() will
      // reject it with EAGAIN instead of adding it to the queue. cannot be used
      // with DelayedTagCalc, because add_request() needs an accurate tag
      Reject,
    };

    // when AtLimit::Reject is used, only start rejecting requests once their
    // limit is above this threshold. requests under this threshold are
    // enqueued and processed like AtLimit::Wait
    using RejectThreshold = Time;

    // the AtLimit constructor parameter can either accept AtLimit or a value
    // for RejectThreshold (which implies AtLimit::Reject)
    using AtLimitParam = std::variant<AtLimit, RejectThreshold>;

    struct ClientInfo {
      double reservation;  // minimum
      double weight;       // proportional
      double limit;        // maximum

      // multiplicative inverses of above, which we use in calculations
      // and don't want to recalculate repeatedly
      double reservation_inv;
      double weight_inv;
      double limit_inv;

      // order parameters -- min, "normal", max
      ClientInfo(double _reservation, double _weight, double _limit) {
	update(_reservation, _weight, _limit);
      }
 
      inline void update(double _reservation, double _weight, double _limit) {
       reservation = _reservation;
       weight = _weight;
       limit = _limit;
       reservation_inv = (0.0 == reservation) ? 0.0 : 1.0 / reservation;
       weight_inv = (0.0 == weight) ? 0.0 : 1.0 / weight;
       limit_inv = (0.0 == limit) ? 0.0 : 1.0 / limit;
      }

      friend std::ostream& operator<<(std::ostream& out,
				      const ClientInfo& client) {
	out <<
	  "{ ClientInfo:: r:" << client.reservation <<
	  " w:" << std::fixed << client.weight <<
	  " l:" << std::fixed << client.limit <<
	  " 1/r:" << std::fixed << client.reservation_inv <<
	  " 1/w:" << std::fixed << client.weight_inv <<
	  " 1/l:" << std::fixed << client.limit_inv <<
	  " }";
	return out;
      }
    }; // class ClientInfo


    struct RequestTag {
      double   reservation;
      double   proportion;
      double   limit;
      uint32_t delta;
      uint32_t rho;
      Cost     cost;
      bool     ready; // true when within limit
      Time     arrival;

      RequestTag(const RequestTag& prev_tag,
		 const ClientInfo& client,
		 const uint32_t _delta,
		 const uint32_t _rho,
		 const Time time,
		 const Cost _cost = 1u,
		 const double anticipation_timeout = 0.0) :
	delta(_delta),
	rho(_rho),
	cost(_cost),
	ready(false),
	arrival(time)
      {
	assert(cost > 0);
	Time max_time = time;
	if (time - anticipation_timeout < prev_tag.arrival)
	  max_time -= anticipation_timeout;
	
	reservation = tag_calc(max_time,
			       prev_tag.reservation,
			       client.reservation_inv,
			       rho,
			       true,
			       cost);
	proportion = tag_calc(max_time,
			      prev_tag.proportion,
			      client.weight_inv,
			      delta,
			      true,
			      cost);
	limit = tag_calc(max_time,
			 prev_tag.limit,
			 client.limit_inv,
			 delta,
			 false,
			 cost);

	assert(reservation < max_tag || proportion < max_tag);
      }

      RequestTag(const RequestTag& prev_tag,
		 const ClientInfo& client,
		 const ReqParams req_params,
		 const Time time,
		 const Cost cost = 1u,
		 const double anticipation_timeout = 0.0) :
	RequestTag(prev_tag, client, req_params.delta, req_params.rho, time,
		   cost, anticipation_timeout)
      { /* empty */ }

      RequestTag(const double _res, const double _prop, const double _lim,
		 const Time _arrival,
		 const uint32_t _delta = 0,
		 const uint32_t _rho = 0,
		 const Cost _cost = 1u) :
	reservation(_res),
	proportion(_prop),
	limit(_lim),
	delta(_delta),
	rho(_rho),
	cost(_cost),
	ready(false),
	arrival(_arrival)
      {
	assert(cost > 0);
	assert(reservation < max_tag || proportion < max_tag);
      }

      RequestTag(const RequestTag& other) :
	reservation(other.reservation),
	proportion(other.proportion),
	limit(other.limit),
	delta(other.delta),
	rho(other.rho),
	cost(other.cost),
	ready(other.ready),
	arrival(other.arrival)
      { /* empty */ }

      static std::string format_tag_change(double before, double after) {
	if (before == after) {
	  return std::string("same");
	} else {
	  std::stringstream ss;
	  ss << format_tag(before) << "=>" << format_tag(after);
	  return ss.str();
	}
      }

      static std::string format_tag(double value) {
	if (max_tag == value) {
	  return std::string("max");
	} else if (min_tag == value) {
	  return std::string("min");
	} else {
	  return format_time(value, tag_modulo);
	}
      }

    private:

      static double tag_calc(const Time time,
			     const double prev,
			     const double increment,
			     const uint32_t dist_req_val,
			     const bool extreme_is_high,
			     const Cost cost) {
	if (0.0 == increment) {
	  return extreme_is_high ? max_tag : min_tag;
	} else {
	  // insure 64-bit arithmetic before conversion to double
	  double tag_increment = increment * (uint64_t(dist_req_val) + cost);
	  return std::max(time, prev + tag_increment);
	}
      }

      friend std::ostream& operator<<(std::ostream& out,
				      const RequestTag& tag) {
	out <<
	  "{ RequestTag:: ready:" << (tag.ready ? "true" : "false") <<
	  " r:" << format_tag(tag.reservation) <<
	  " p:" << format_tag(tag.proportion) <<
	  " l:" << format_tag(tag.limit) <<
#if 0 // try to resolve this to make sure Time is operator<<'able.
	  " arrival:" << tag.arrival <<
#endif
	  " }";
	return out;
      }
    }; // class RequestTag

    // C is client identifier type, R is request type,
    // IsDelayed controls whether tag calculation is delayed until the request
    //   reaches the front of its queue. This is an optimization over the
    //   originally published dmclock algorithm, allowing it to use the most
    //   recent values of rho and delta.
    // U1 determines whether to use client information function dynamically,
    // B is heap branching factor
    template<typename C, typename R, bool IsDelayed, bool U1, unsigned B>
    class PriorityQueueBase {
      // we don't want to include gtest.h just for FRIEND_TEST
      friend class dmclock_server_client_idle_erase_Test;
      friend class dmclock_server_add_req_pushprio_queue_Test;

      // types used for tag dispatch to select between implementations
      using TagCalc = std::integral_constant<bool, IsDelayed>;
      using DelayedTagCalc = std::true_type;
      using ImmediateTagCalc = std::false_type;

    public:

      using RequestRef = std::unique_ptr<R>;

    protected:

      using Clock = std::chrono::steady_clock;
      using TimePoint = Clock::time_point;
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
	  out << "{ ClientReq:: tag:" << c.tag << " client:" <<
	    c.client_id << " }";
	  return out;
	}
      }; // class ClientReq

      struct RequestMeta {
        C          client_id;
        RequestTag tag;

        RequestMeta(const C&  _client_id, const RequestTag& _tag) :
          client_id(_client_id),
          tag(_tag)
        {
          // empty
        }
      };

    public:

      // NOTE: ClientRec is in the "public" section for compatibility
      // with g++ 4.8.4, which complains if it's not. By g++ 6.3.1
      // ClientRec could be "protected" with no issue. [See comments
      // associated with function submit_top_request.]
      class ClientRec {
	friend PriorityQueueBase<C,R,IsDelayed,U1,B>;

	C                     client;
	RequestTag            prev_tag;
	std::deque<ClientReq> requests;

	// amount added from the proportion tag as a result of
	// an idle client becoming unidle
	double                prop_delta = 0.0;

	c::IndIntruHeapData   reserv_heap_data {};
	c::IndIntruHeapData   lim_heap_data {};
	c::IndIntruHeapData   ready_heap_data {};
#if USE_PROP_HEAP
	c::IndIntruHeapData   prop_heap_data {};
#endif

      public:

	const ClientInfo*     info;
	bool                  idle;
	Counter               last_tick;
	uint32_t              cur_rho;
	uint32_t              cur_delta;

	ClientRec(C _client,
		  const ClientInfo* _info,
		  Counter current_tick) :
	  client(_client),
	  prev_tag(0.0, 0.0, 0.0, TimeZero),
	  info(_info),
	  idle(true),
	  last_tick(current_tick),
	  cur_rho(1),
	  cur_delta(1)
	{
	  // empty
	}

	inline const RequestTag& get_req_tag() const {
	  return prev_tag;
	}

	static inline void assign_unpinned_tag(double& lhs, const double rhs) {
	  if (rhs != max_tag && rhs != min_tag) {
	    lhs = rhs;
	  }
	}

	inline void update_req_tag(const RequestTag& _prev,
				   const Counter& _tick) {
	  assign_unpinned_tag(prev_tag.reservation, _prev.reservation);
	  assign_unpinned_tag(prev_tag.limit, _prev.limit);
	  assign_unpinned_tag(prev_tag.proportion, _prev.proportion);
	  prev_tag.arrival = _prev.arrival;
	  last_tick = _tick;
	}

	inline void add_request(const RequestTag& tag, RequestRef&& request) {
	  requests.emplace_back(tag, client, std::move(request));
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

	inline size_t request_count() const {
	  return requests.size();
	}

	// NB: because a deque is the underlying structure, this
	// operation might be expensive
	bool remove_by_req_filter_fw(std::function<bool(RequestRef&&)> filter_accum) {
	  bool any_removed = false;
	  for (auto i = requests.begin();
	       i != requests.end();
	       /* no inc */) {
	    if (filter_accum(std::move(i->request))) {
	      any_removed = true;
	      i = requests.erase(i);
	    } else {
	      ++i;
	    }
	  }
	  return any_removed;
	}

	// NB: because a deque is the underlying structure, this
	// operation might be expensive
	bool remove_by_req_filter_bw(std::function<bool(RequestRef&&)> filter_accum) {
	  bool any_removed = false;
	  for (auto i = requests.rbegin();
	       i != requests.rend();
	       /* no inc */) {
	    if (filter_accum(std::move(i->request))) {
	      any_removed = true;
	      i = decltype(i){ requests.erase(std::next(i).base()) };
	    } else {
	      ++i;
	    }
	  }
	  return any_removed;
	}

	inline bool
	remove_by_req_filter(std::function<bool(RequestRef&&)> filter_accum,
			     bool visit_backwards) {
	  if (visit_backwards) {
	    return remove_by_req_filter_bw(filter_accum);
	  } else {
	    return remove_by_req_filter_fw(filter_accum);
	  }
	}

	friend std::ostream&
	operator<<(std::ostream& out,
		   const typename PriorityQueueBase::ClientRec& e) {
	  out << "{ ClientRec::" <<
	    " client:" << e.client <<
	    " prev_tag:" << e.prev_tag <<
	    " req_count:" << e.requests.size() <<
	    " top_req:";
	  if (e.has_request()) {
	    out << e.next_request();
	  } else {
	    out << "none";
	  }
	  out << " }";

	  return out;
	}
      }; // class ClientRec

      using ClientRecRef = std::shared_ptr<ClientRec>;

      // when we try to get the next request, we'll be in one of three
      // situations -- we'll have one to return, have one that can
      // fire in the future, or not have any
      enum class NextReqType { returning, future, none };

      // specifies which queue next request will get popped from
      enum class HeapId { reservation, ready };

      // this is returned from next_req to tell the caller the situation
      struct NextReq {
	NextReqType type;
	union {
	  HeapId    heap_id;
	  Time      when_ready;
	};

	inline explicit NextReq() :
	  type(NextReqType::none)
	{ }

	inline NextReq(HeapId _heap_id) :
	  type(NextReqType::returning),
	  heap_id(_heap_id)
	{ }

	inline NextReq(Time _when_ready) :
	  type(NextReqType::future),
	  when_ready(_when_ready)
	{ }

	// calls to this are clearer than calls to the default
	// constructor
	static inline NextReq none() {
	  return NextReq();
	}
      };


      // a function that can be called to look up client information
      using ClientInfoFunc = std::function<const ClientInfo*(const C&)>;


      bool empty() const {
	DataGuard g(data_mtx);
	return (resv_heap.empty() || ! resv_heap.top().has_request());
      }


      size_t client_count() const {
	DataGuard g(data_mtx);
	return resv_heap.size();
      }


      size_t request_count() const {
	DataGuard g(data_mtx);
	size_t total = 0;
	for (auto i = resv_heap.cbegin(); i != resv_heap.cend(); ++i) {
	  total += i->request_count();
	}
	return total;
      }


      bool remove_by_req_filter(std::function<bool(RequestRef&&)> filter_accum,
				bool visit_backwards = false) {
	bool any_removed = false;
	DataGuard g(data_mtx);
	for (auto i : client_map) {
	  bool modified =
	    i.second->remove_by_req_filter(filter_accum, visit_backwards);
	  if (modified) {
	    resv_heap.adjust(*i.second);
	    limit_heap.adjust(*i.second);
	    ready_heap.adjust(*i.second);
#if USE_PROP_HEAP
	    prop_heap.adjust(*i.second);
#endif
	    any_removed = true;
	  }
	}
	return any_removed;
      }


      // use as a default value when no accumulator is provide
      static void request_sink(RequestRef&& req) {
	// do nothing
      }


      void remove_by_client(const C& client,
			    bool reverse = false,
			    std::function<void (RequestRef&&)> accum = request_sink) {
	DataGuard g(data_mtx);

	auto i = client_map.find(client);

	if (i == client_map.end()) return;

	if (reverse) {
	  for (auto j = i->second->requests.rbegin();
	       j != i->second->requests.rend();
	       ++j) {
	    accum(std::move(j->request));
	  }
	} else {
	  for (auto j = i->second->requests.begin();
	       j != i->second->requests.end();
	       ++j) {
	    accum(std::move(j->request));
	  }
	}

	i->second->requests.clear();

	resv_heap.adjust(*i->second);
	limit_heap.adjust(*i->second);
	ready_heap.adjust(*i->second);
#if USE_PROP_HEAP
	prop_heap.adjust(*i->second);
#endif
      }


      unsigned get_heap_branching_factor() const {
	return B;
      }


      void update_client_info(const C& client_id) {
	DataGuard g(data_mtx);
	auto client_it = client_map.find(client_id);
	if (client_map.end() != client_it) {
	  ClientRec& client = (*client_it->second);
	  client.info = client_info_f(client_id);
	}
      }


      void update_client_infos() {
	DataGuard g(data_mtx);
	for (auto i : client_map) {
	  i.second->info = client_info_f(i.second->client);
	}
      }


      friend std::ostream& operator<<(std::ostream& out,
				      const PriorityQueueBase& q) {
	std::lock_guard<decltype(q.data_mtx)> guard(q.data_mtx);

	out << "{ PriorityQueue::";
	for (const auto& c : q.client_map) {
	  out << "  { client:" << c.first << ", record:" << *c.second <<
	    " }";
	}
	if (!q.resv_heap.empty()) {
	  const auto& resv = q.resv_heap.top();
	  out << " { reservation_top:" << resv << " }";
	  const auto& ready = q.ready_heap.top();
	  out << " { ready_top:" << ready << " }";
	  const auto& limit = q.limit_heap.top();
	  out << " { limit_top:" << limit << " }";
	} else {
	  out << " HEAPS-EMPTY";
	}
	out << " }";

	return out;
      }

      // for debugging
      void display_queues(std::ostream& out,
			  bool show_res = true,
			  bool show_lim = true,
			  bool show_ready = true,
			  bool show_prop = true) const {
	auto filter = [](const ClientRec& e)->bool { return true; };
	DataGuard g(data_mtx);
	if (show_res) {
	  resv_heap.display_sorted(out << "RESER:", filter);
	}
	if (show_lim) {
	  limit_heap.display_sorted(out << "LIMIT:", filter);
	}
	if (show_ready) {
	  ready_heap.display_sorted(out << "READY:", filter);
	}
#if USE_PROP_HEAP
	if (show_prop) {
	  prop_heap.display_sorted(out << "PROPO:", filter);
	}
#endif
      } // display_queues


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
      // use_prop_delta determines whether the proportional delta is
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

      ClientInfoFunc        client_info_f;
      static constexpr bool is_dynamic_cli_info_f = U1;

#ifdef WITH_CRIMSON
      static constexpr int data_mtx = 0;
      struct DataGuard { DataGuard(int) {} };
#else
      mutable std::mutex data_mtx;
      using DataGuard = std::lock_guard<decltype(data_mtx)>;
#endif

      // stable mapping between client ids and client queues
      std::map<C,ClientRecRef> client_map;

      c::IndIntruHeap<ClientRecRef,
		      ClientRec,
		      &ClientRec::reserv_heap_data,
		      ClientCompare<&RequestTag::reservation,
				    ReadyOption::ignore,
				    false>,
		      B> resv_heap;
#if USE_PROP_HEAP
      c::IndIntruHeap<ClientRecRef,
		      ClientRec,
		      &ClientRec::prop_heap_data,
		      ClientCompare<&RequestTag::proportion,
				    ReadyOption::ignore,
				    true>,
		      B> prop_heap;
#endif
      c::IndIntruHeap<ClientRecRef,
		      ClientRec,
		      &ClientRec::lim_heap_data,
		      ClientCompare<&RequestTag::limit,
				    ReadyOption::lowers,
				    false>,
		      B> limit_heap;
      c::IndIntruHeap<ClientRecRef,
		      ClientRec,
		      &ClientRec::ready_heap_data,
		      ClientCompare<&RequestTag::proportion,
				    ReadyOption::raises,
				    true>,
		      B> ready_heap;

      AtLimit          at_limit;
      RejectThreshold  reject_threshold = 0;

      double           anticipation_timeout;
#ifdef WITH_CRIMSON
      bool finishing;
#else
      std::atomic_bool finishing;
#endif
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
      // max number of clients to erase at a time
      Counter erase_max;
      // unfinished last erase point
      Counter last_erase_point = 0;

      // NB: All threads declared at end, so they're destructed first!

      std::unique_ptr<RunEvery> cleaning_job;

      // helper function to return the value of a variant if it matches the
      // given type T, or a default value of T otherwise
      template <typename T, typename Variant>
      static T get_or_default(const Variant& param, T default_value) {
	const T *p = std::get_if<T>(&param);
	return p ? *p : default_value;
      }

      // COMMON constructor that others feed into; we can accept three
      // different variations of durations
      template<typename Rep, typename Per>
      PriorityQueueBase(ClientInfoFunc _client_info_f,
			std::chrono::duration<Rep,Per> _idle_age,
			std::chrono::duration<Rep,Per> _erase_age,
			std::chrono::duration<Rep,Per> _check_time,
			AtLimitParam at_limit_param,
			double _anticipation_timeout) :
	client_info_f(_client_info_f),
	at_limit(get_or_default(at_limit_param, AtLimit::Reject)),
	reject_threshold(get_or_default(at_limit_param, RejectThreshold{0})),
	anticipation_timeout(_anticipation_timeout),
	finishing(false),
	idle_age(std::chrono::duration_cast<Duration>(_idle_age)),
	erase_age(std::chrono::duration_cast<Duration>(_erase_age)),
	check_time(std::chrono::duration_cast<Duration>(_check_time)),
	erase_max(standard_erase_max)
      {
	assert(_erase_age >= _idle_age);
	assert(_check_time < _idle_age);
	// AtLimit::Reject depends on ImmediateTagCalc
	assert(at_limit != AtLimit::Reject || !IsDelayed);
	cleaning_job =
	  std::unique_ptr<RunEvery>(
	    new RunEvery(check_time,
			 std::bind(&PriorityQueueBase::do_clean, this)));
      }


      ~PriorityQueueBase() {
	finishing = true;
      }


      inline const ClientInfo* get_cli_info(ClientRec& client) const {
	if (is_dynamic_cli_info_f) {
	  client.info = client_info_f(client.client);
	}
	return client.info;
      }

      // data_mtx must be held by caller
      RequestTag initial_tag(DelayedTagCalc delayed, ClientRec& client,
			     const ReqParams& params, Time time, Cost cost) {
	RequestTag tag(0, 0, 0, time, 0, 0, cost);

	// only calculate a tag if the request is going straight to the front
	if (!client.has_request()) {
	  const ClientInfo* client_info = get_cli_info(client);
	  assert(client_info);
	  tag = RequestTag(client.get_req_tag(), *client_info,
			   params, time, cost, anticipation_timeout);

	  // copy tag to previous tag for client
	  client.update_req_tag(tag, tick);
	}
	return tag;
      }

      // data_mtx must be held by caller
      RequestTag initial_tag(ImmediateTagCalc imm, ClientRec& client,
			     const ReqParams& params, Time time, Cost cost) {
	// calculate the tag unconditionally
	const ClientInfo* client_info = get_cli_info(client);
	assert(client_info);
	RequestTag tag(client.get_req_tag(), *client_info,
		       params, time, cost, anticipation_timeout);

	// copy tag to previous tag for client
	client.update_req_tag(tag, tick);
	return tag;
      }

      // data_mtx must be held by caller. returns 0 on success. when using
      // AtLimit::Reject, requests that would exceed their limit are rejected
      // with EAGAIN, and the queue will not take ownership of the given
      // 'request' argument
      int do_add_request(RequestRef&& request,
			 const C& client_id,
			 const ReqParams& req_params,
			 const Time time,
			 const Cost cost = 1u) {
	++tick;

        auto insert = client_map.emplace(client_id, ClientRecRef{});
        if (insert.second) {
          // new client entry
	  const ClientInfo* info = client_info_f(client_id);
	  auto client_rec = std::make_shared<ClientRec>(client_id, info, tick);
	  resv_heap.push(client_rec);
#if USE_PROP_HEAP
	  prop_heap.push(client_rec);
#endif
	  limit_heap.push(client_rec);
	  ready_heap.push(client_rec);
	  insert.first->second = std::move(client_rec);
	}

	// for convenience, we'll create a reference to the shared pointer
	ClientRec& client = *insert.first->second;

	if (client.idle) {
	  // We need to do an adjustment so that idle clients compete
	  // fairly on proportional tags since those tags may have
	  // drifted from real-time. Either use the lowest existing
	  // proportion tag -- O(1) -- or the client with the lowest
	  // previous proportion tag -- O(n) where n = # clients.
	  //
	  // So we don't have to maintain a proportional queue that
	  // keeps the minimum on proportional tag alone (we're
	  // instead using a ready queue), we'll have to check each
	  // client.
	  //
	  // The alternative would be to maintain a proportional queue
	  // (define USE_PROP_TAG) and do an O(1) operation here.

	  // Was unable to confirm whether equality testing on
	  // std::numeric_limits<double>::max() is guaranteed, so
	  // we'll use a compile-time calculated trigger that is one
	  // third the max, which should be much larger than any
	  // expected organic value.
	  constexpr double lowest_prop_tag_trigger =
	    std::numeric_limits<double>::max() / 3.0;

	  double lowest_prop_tag = std::numeric_limits<double>::max();
	  for (auto const &c : client_map) {
	    // don't use ourselves (or anything else that might be
	    // listed as idle) since we're now in the map
	    if (!c.second->idle) {
	      double p;
	      // use either lowest proportion tag or previous proportion tag
	      if (c.second->has_request()) {
		p = c.second->next_request().tag.proportion +
		  c.second->prop_delta;
	      } else {
	        p = c.second->get_req_tag().proportion + c.second->prop_delta;
	      }

	      if (p < lowest_prop_tag) {
		lowest_prop_tag = p;
	      }
	    }
	  }

	  // if this conditional does not fire, it
	  if (lowest_prop_tag < lowest_prop_tag_trigger) {
	    client.prop_delta = lowest_prop_tag - time;
	  }
	  client.idle = false;
	} // if this client was idle

	RequestTag tag = initial_tag(TagCalc{}, client, req_params, time, cost);

	if (at_limit == AtLimit::Reject &&
            tag.limit > time + reject_threshold) {
	  // if the client is over its limit, reject it here
	  return EAGAIN;
	}

	client.add_request(tag, std::move(request));
	if (1 == client.requests.size()) {
	  // NB: can the following 4 calls to adjust be changed
	  // promote? Can adding a request ever demote a client in the
	  // heaps?
	  resv_heap.adjust(client);
	  limit_heap.adjust(client);
	  ready_heap.adjust(client);
#if USE_PROP_HEAP
	  prop_heap.adjust(client);
#endif
	}

	client.cur_rho = req_params.rho;
	client.cur_delta = req_params.delta;

	resv_heap.adjust(client);
	limit_heap.adjust(client);
	ready_heap.adjust(client);
#if USE_PROP_HEAP
	prop_heap.adjust(client);
#endif
	return 0;
      } // do_add_request

      // data_mtx must be held by caller
      void update_next_tag(DelayedTagCalc delayed, ClientRec& top,
			   const RequestTag& tag) {
	if (top.has_request()) {
	  // perform delayed tag calculation on the next request
	  ClientReq& next_first = top.next_request();
	  const ClientInfo* client_info = get_cli_info(top);
	  assert(client_info);
	  next_first.tag = RequestTag(tag, *client_info,
				      top.cur_delta, top.cur_rho,
				      next_first.tag.arrival,
				      next_first.tag.cost,
				      anticipation_timeout);
	  // copy tag to previous tag for client
	  top.update_req_tag(next_first.tag, tick);
	}
      }

      void update_next_tag(ImmediateTagCalc imm, ClientRec& top,
			   const RequestTag& tag) {
	// the next tag was already calculated on insertion
      }

      // data_mtx should be held when called; top of heap should have
      // a ready request
      template<typename C1, IndIntruHeapData ClientRec::*C2, typename C3>
      RequestTag pop_process_request(IndIntruHeap<C1, ClientRec, C2, C3, B>& heap,
			       std::function<void(const C& client,
						  const Cost cost,
						  RequestRef& request)> process) {
	// gain access to data
	ClientRec& top = heap.top();

	Cost request_cost = top.next_request().tag.cost;
	RequestRef request = std::move(top.next_request().request);
	RequestTag tag = top.next_request().tag;

	// pop request and adjust heaps
	top.pop_request();

	update_next_tag(TagCalc{}, top, tag);

	resv_heap.demote(top);
	limit_heap.adjust(top);
#if USE_PROP_HEAP
	prop_heap.demote(top);
#endif
	ready_heap.demote(top);

	// process
	process(top.client, request_cost, request);

	return tag;
      } // pop_process_request


      // data_mtx must be held by caller
      void reduce_reservation_tags(DelayedTagCalc delayed, ClientRec& client,
                                   const RequestTag& tag) {
	if (!client.requests.empty()) {
	  // only maintain a tag for the first request
	  auto& r = client.requests.front();
	  r.tag.reservation -=
	    client.info->reservation_inv * (tag.cost + tag.rho);
	}
      }

      // data_mtx should be held when called
      void reduce_reservation_tags(ImmediateTagCalc imm, ClientRec& client,
                                   const RequestTag& tag) {
        double res_offset =
          client.info->reservation_inv * (tag.cost + tag.rho);
	for (auto& r : client.requests) {
	  r.tag.reservation -= res_offset;
	}
      }

      // data_mtx should be held when called
      void reduce_reservation_tags(const C& client_id, const RequestTag& tag) {
	auto client_it = client_map.find(client_id);

	// means the client was cleaned from map; should never happen
	// as long as cleaning times are long enough
	assert(client_map.end() != client_it);
	ClientRec& client = *client_it->second;
	reduce_reservation_tags(TagCalc{}, client, tag);

	// don't forget to update previous tag
	client.prev_tag.reservation -=
	  client.info->reservation_inv * (tag.cost + tag.rho);
	resv_heap.promote(client);
      }


      // data_mtx should be held when called
      NextReq do_next_request(Time now) {
	// if reservation queue is empty, all are empty (i.e., no
	// active clients)
	if(resv_heap.empty()) {
	  return NextReq::none();
	}

	// try constraint (reservation) based scheduling

	auto& reserv = resv_heap.top();
	if (reserv.has_request() &&
	    reserv.next_request().tag.reservation <= now) {
	  return NextReq(HeapId::reservation);
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
	  return NextReq(HeapId::ready);
	}

	// if nothing is schedulable by reservation or
	// proportion/weight, and if we allow limit break, try to
	// schedule something with the lowest proportion tag or
	// alternatively lowest reservation tag.
	if (at_limit == AtLimit::Allow) {
	  if (readys.has_request() &&
	      readys.next_request().tag.proportion < max_tag) {
	    return NextReq(HeapId::ready);
	  } else if (reserv.has_request() &&
		     reserv.next_request().tag.reservation < max_tag) {
	    return NextReq(HeapId::reservation);
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
	  assert(!next.tag.ready || max_tag == next.tag.proportion);
	  next_call = min_not_0_time(next_call, next.tag.limit);
	}
	if (next_call < TimeMax) {
	  return NextReq(next_call);
	} else {
	  return NextReq::none();
	}
      } // do_next_request


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

	Counter erase_point = last_erase_point;
	auto point = clean_mark_points.front();
	while (point.first <= now - erase_age) {
	  last_erase_point = point.second;
	  erase_point = last_erase_point;
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

	Counter erased_num = 0;
	if (erase_point > 0 || idle_point > 0) {
	  for (auto i = client_map.begin(); i != client_map.end(); /* empty */) {
	    auto i2 = i++;
	    if (!(i2->second->has_request()) &&
		erase_point &&
	        erased_num < erase_max &&
	        i2->second->last_tick <= erase_point) {
	      delete_from_heaps(i2->second);
	      client_map.erase(i2);
	      erased_num++;
	    } else if (idle_point && i2->second->last_tick <= idle_point) {
	      i2->second->idle = true;
	    }
	  } // for

	  auto wperiod = check_time;
	  if (erased_num >= erase_max) {
	    wperiod = duration_cast<milliseconds>(aggressive_check_time);
	  } else {
	    // clean finished, refresh
	    last_erase_point = 0;
	  }
	  cleaning_job->try_update(wperiod);
	} // if
      } // do_clean


      // data_mtx must be held by caller
      template<IndIntruHeapData ClientRec::*C1,typename C2>
      void delete_from_heap(ClientRecRef& client,
			    c::IndIntruHeap<ClientRecRef,ClientRec,C1,C2,B>& heap) {
	auto i = heap.at(client);
	heap.remove(i);
      }


      // data_mtx must be held by caller
      void delete_from_heaps(ClientRecRef& client) {
	delete_from_heap(client, resv_heap);
#if USE_PROP_HEAP
	delete_from_heap(client, prop_heap);
#endif
	delete_from_heap(client, limit_heap);
	delete_from_heap(client, ready_heap);
      }
    }; // class PriorityQueueBase


    template<typename C, typename R, bool IsDelayed=false, bool U1=false, unsigned B=2>
    class PullPriorityQueue : public PriorityQueueBase<C,R,IsDelayed,U1,B> {
      using super = PriorityQueueBase<C,R,IsDelayed,U1,B>;

    public:

      // When a request is pulled, this is the return type.
      struct PullReq {
	struct Retn {
	  C                          client;
	  typename super::RequestRef request;
	  PhaseType                  phase;
	  Cost                       cost;
	};

	typename super::NextReqType   type;
	std::variant<Retn,Time>     data;

	bool is_none() const { return type == super::NextReqType::none; }

	bool is_retn() const { return type == super::NextReqType::returning; }
	Retn& get_retn() {
	  return std::get<Retn>(data);
	}

	bool is_future() const { return type == super::NextReqType::future; }
	Time getTime() const { return std::get<Time>(data); }
      };


#ifdef PROFILE
      ProfileTimer<std::chrono::nanoseconds> pull_request_timer;
      ProfileTimer<std::chrono::nanoseconds> add_request_timer;
#endif

      template<typename Rep, typename Per>
      PullPriorityQueue(typename super::ClientInfoFunc _client_info_f,
			std::chrono::duration<Rep,Per> _idle_age,
			std::chrono::duration<Rep,Per> _erase_age,
			std::chrono::duration<Rep,Per> _check_time,
			AtLimitParam at_limit_param = AtLimit::Wait,
			double _anticipation_timeout = 0.0) :
	super(_client_info_f,
	      _idle_age, _erase_age, _check_time,
	      at_limit_param, _anticipation_timeout)
      {
	// empty
      }


      // pull convenience constructor
      PullPriorityQueue(typename super::ClientInfoFunc _client_info_f,
			AtLimitParam at_limit_param = AtLimit::Wait,
			double _anticipation_timeout = 0.0) :
	PullPriorityQueue(_client_info_f,
			  standard_idle_age,
			  standard_erase_age,
			  standard_check_time,
			  at_limit_param,
			  _anticipation_timeout)
      {
	// empty
      }


      int add_request(R&& request,
		      const C& client_id,
		      const ReqParams& req_params,
		      const Cost cost = 1u) {
	return add_request(typename super::RequestRef(new R(std::move(request))),
			   client_id,
			   req_params,
			   get_time(),
			   cost);
      }


      int add_request(R&& request,
		      const C& client_id,
		      const Cost cost = 1u) {
	static const ReqParams null_req_params;
	return add_request(typename super::RequestRef(new R(std::move(request))),
			   client_id,
			   null_req_params,
			   get_time(),
			   cost);
      }


      int add_request_time(R&& request,
			   const C& client_id,
			   const ReqParams& req_params,
			   const Time time,
			   const Cost cost = 1u) {
	return add_request(typename super::RequestRef(new R(std::move(request))),
			   client_id,
			   req_params,
			   time,
			   cost);
      }


      int add_request(typename super::RequestRef&& request,
		      const C& client_id,
		      const ReqParams& req_params,
		      const Cost cost = 1u) {
	return add_request(std::move(request), client_id, req_params, get_time(), cost);
      }


      int add_request(typename super::RequestRef&& request,
		      const C& client_id,
		      const Cost cost = 1u) {
	static const ReqParams null_req_params;
	return add_request(std::move(request), client_id, null_req_params, get_time(), cost);
      }


      // this does the work; the versions above provide alternate interfaces
      int add_request(typename super::RequestRef&& request,
		      const C& client_id,
		      const ReqParams& req_params,
		      const Time time,
		      const Cost cost = 1u) {
	typename super::DataGuard g(this->data_mtx);
#ifdef PROFILE
	add_request_timer.start();
#endif
	int r = super::do_add_request(std::move(request),
				      client_id,
				      req_params,
				      time,
				      cost);
	// no call to schedule_request for pull version
#ifdef PROFILE
	add_request_timer.stop();
#endif
	return r;
      }


      inline PullReq pull_request() {
	return pull_request(get_time());
      }


      PullReq pull_request(const Time now) {
	PullReq result;
	typename super::DataGuard g(this->data_mtx);
#ifdef PROFILE
	pull_request_timer.start();
#endif

	typename super::NextReq next = super::do_next_request(now);
	result.type = next.type;
	switch(next.type) {
	case super::NextReqType::none:
	  return result;
	case super::NextReqType::future:
	  result.data = next.when_ready;
	  return result;
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
			     uint64_t,
			     typename super::RequestRef&)> {
	  return [&pull_result, phase](const C& client,
				       const Cost request_cost,
				       typename super::RequestRef& request) {
	    pull_result.data = typename PullReq::Retn{ client,
						       std::move(request),
						       phase,
						       request_cost };
	  };
	};

	switch(next.heap_id) {
	case super::HeapId::reservation:
	  (void) super::pop_process_request(this->resv_heap,
				     process_f(result,
					       PhaseType::reservation));
	  ++this->reserv_sched_count;
	  break;
	case super::HeapId::ready:
	  {
	    auto tag = super::pop_process_request(this->ready_heap,
				     process_f(result, PhaseType::priority));
	    // need to use retn temporarily
	    auto& retn = std::get<typename PullReq::Retn>(result.data);
	    super::reduce_reservation_tags(retn.client, tag);
	  }
	  ++this->prop_sched_count;
	  break;
	default:
	  assert(false);
	}

#ifdef PROFILE
	pull_request_timer.stop();
#endif
	return result;
      } // pull_request


    protected:


      // data_mtx should be held when called; unfortunately this
      // function has to be repeated in both push & pull
      // specializations
      typename super::NextReq next_request() {
	return next_request(get_time());
      }
    }; // class PullPriorityQueue

#ifndef WITH_CRIMSON
    // TODO: PushPriorityQueue is not ported to seastar yet
    // PUSH version
    template<typename C, typename R, bool IsDelayed=false, bool U1=false, unsigned B=2>
    class PushPriorityQueue : public PriorityQueueBase<C,R,IsDelayed,U1,B> {

    protected:

      using super = PriorityQueueBase<C,R,IsDelayed,U1,B>;

    public:

      // a function to see whether the server can handle another request
      using CanHandleRequestFunc = std::function<bool(void)>;

      // a function to submit a request to the server; the second
      // parameter is a callback when it's completed
      using HandleRequestFunc =
	std::function<void(const C&,typename super::RequestRef,PhaseType,uint64_t)>;

    protected:

      CanHandleRequestFunc can_handle_f;
      HandleRequestFunc    handle_f;
      // for handling timed scheduling
      std::mutex  sched_ahead_mtx;
      std::condition_variable sched_ahead_cv;
      Time sched_ahead_when = TimeZero;

#ifdef PROFILE
    public:
      ProfileTimer<std::chrono::nanoseconds> add_request_timer;
      ProfileTimer<std::chrono::nanoseconds> request_complete_timer;
    protected:
#endif

      // NB: threads declared last, so constructed last and destructed first

      std::thread sched_ahead_thd;

    public:

      // push full constructor
      template<typename Rep, typename Per>
      PushPriorityQueue(typename super::ClientInfoFunc _client_info_f,
			CanHandleRequestFunc _can_handle_f,
			HandleRequestFunc _handle_f,
			std::chrono::duration<Rep,Per> _idle_age,
			std::chrono::duration<Rep,Per> _erase_age,
			std::chrono::duration<Rep,Per> _check_time,
			AtLimitParam at_limit_param = AtLimit::Wait,
			double anticipation_timeout = 0.0) :
	super(_client_info_f,
	      _idle_age, _erase_age, _check_time,
	      at_limit_param, anticipation_timeout)
      {
	can_handle_f = _can_handle_f;
	handle_f = _handle_f;
	sched_ahead_thd = std::thread(&PushPriorityQueue::run_sched_ahead, this);
      }


      // push convenience constructor
      PushPriorityQueue(typename super::ClientInfoFunc _client_info_f,
			CanHandleRequestFunc _can_handle_f,
			HandleRequestFunc _handle_f,
			AtLimitParam at_limit_param = AtLimit::Wait,
			double _anticipation_timeout = 0.0) :
	PushPriorityQueue(_client_info_f,
			  _can_handle_f,
			  _handle_f,
			  standard_idle_age,
			  standard_erase_age,
			  standard_check_time,
			  at_limit_param,
			  _anticipation_timeout)
      {
	// empty
      }


      ~PushPriorityQueue() {
	this->finishing = true;
	{
	  std::lock_guard<std::mutex> l(sched_ahead_mtx);
	  sched_ahead_cv.notify_one();
	}
	sched_ahead_thd.join();
      }

    public:

      int add_request(R&& request,
		      const C& client_id,
		      const ReqParams& req_params,
		      const Cost cost = 1u) {
	return add_request(typename super::RequestRef(new R(std::move(request))),
			   client_id,
			   req_params,
			   get_time(),
			   cost);
      }


      int add_request(typename super::RequestRef&& request,
		      const C& client_id,
		      const ReqParams& req_params,
		      const Cost cost = 1u) {
	return add_request(std::move(request), client_id, req_params, get_time(), cost);
      }


      int add_request_time(const R& request,
			   const C& client_id,
			   const ReqParams& req_params,
			   const Time time,
			   const Cost cost = 1u) {
	return add_request(typename super::RequestRef(new R(request)),
			   client_id,
			   req_params,
			   time,
			   cost);
      }


      int add_request(typename super::RequestRef&& request,
		      const C& client_id,
		      const ReqParams& req_params,
		      const Time time,
		      const Cost cost = 1u) {
	typename super::DataGuard g(this->data_mtx);
#ifdef PROFILE
	add_request_timer.start();
#endif
	int r = super::do_add_request(std::move(request),
				      client_id,
				      req_params,
				      time,
				      cost);
        if (r == 0) {
	  (void) schedule_request();
        }
#ifdef PROFILE
	add_request_timer.stop();
#endif
	return r;
      }


      void request_completed() {
	typename super::DataGuard g(this->data_mtx);
#ifdef PROFILE
	request_complete_timer.start();
#endif
	(void) schedule_request();
#ifdef PROFILE
	request_complete_timer.stop();
#endif
      }

    protected:

      // data_mtx should be held when called; furthermore, the heap
      // should not be empty and the top element of the heap should
      // not be already handled
      //
      // NOTE: the use of "super::ClientRec" in either the template
      // construct or as a parameter to submit_top_request generated
      // a compiler error in g++ 4.8.4, when ClientRec was
      // "protected" rather than "public". By g++ 6.3.1 this was not
      // an issue. But for backwards compatibility
      // PriorityQueueBase::ClientRec is public.
      template<typename C1,
	       IndIntruHeapData super::ClientRec::*C2,
	       typename C3,
	       unsigned B4>
      typename super::RequestMeta
      submit_top_request(IndIntruHeap<C1,typename super::ClientRec,C2,C3,B4>& heap,
			 PhaseType phase) {
	C client_result;
	RequestTag tag = super::pop_process_request(heap,
				   [this, phase, &client_result]
				   (const C& client,
				    const Cost request_cost,
				    typename super::RequestRef& request) {
				     client_result = client;
				     handle_f(client, std::move(request), phase, request_cost);
				   });
	typename super::RequestMeta req(client_result, tag);
	return req;
      }


      // data_mtx should be held when called
      void submit_request(typename super::HeapId heap_id) {
	switch(heap_id) {
	case super::HeapId::reservation:
	  // don't need to note client
	  (void) submit_top_request(this->resv_heap, PhaseType::reservation);
	  // unlike the other two cases, we do not reduce reservation
	  // tags here
	  ++this->reserv_sched_count;
	  break;
	case super::HeapId::ready:
	  {
	    auto req = submit_top_request(this->ready_heap, PhaseType::priority);
	    super::reduce_reservation_tags(req.client_id, req.tag);
	  }
	  ++this->prop_sched_count;
	  break;
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
	  return super::do_next_request(now);
	}
      } // next_request


      // data_mtx should be held when called
      typename super::NextReqType schedule_request() {
	typename super::NextReq next_req = next_request();
	switch (next_req.type) {
	case super::NextReqType::none:
	  break;
	case super::NextReqType::future:
	  sched_at(next_req.when_ready);
	  break;
	case super::NextReqType::returning:
	  submit_request(next_req.heap_id);
	  break;
	default:
	  assert(false);
	}
	return next_req.type;
      }


      // this is the thread that handles running schedule_request at
      // future times when nothing can be scheduled immediately
      void run_sched_ahead() {
	std::unique_lock<std::mutex> l(sched_ahead_mtx);

	while (!this->finishing) {
	  // predicate for cond.wait()
	  const auto pred = [this] () -> bool {
	    return this->finishing || sched_ahead_when > TimeZero;
	  };

	  if (TimeZero == sched_ahead_when) {
	    sched_ahead_cv.wait(l, pred);
	  } else {
	    // cast from Time -> duration<Time> -> Duration -> TimePoint
	    const auto until = typename super::TimePoint{
		duration_cast<typename super::Duration>(
		    std::chrono::duration<Time>{sched_ahead_when})};
	    sched_ahead_cv.wait_until(l, until, pred);
	    sched_ahead_when = TimeZero;
	    if (this->finishing) return;

	    l.unlock();
	    if (!this->finishing) {
	      do {
 	        typename super::DataGuard g(this->data_mtx);
 	        if (schedule_request() == super::NextReqType::future)
 	          break;
	      } while (!this->empty());
 	    }
	    l.lock();
	  }
	}
      }


      void sched_at(Time when) {
	std::lock_guard<std::mutex> l(sched_ahead_mtx);
	if (this->finishing) return;
	if (TimeZero == sched_ahead_when || when < sched_ahead_when) {
	  sched_ahead_when = when;
	  sched_ahead_cv.notify_one();
	}
      }
    }; // class PushPriorityQueue
#endif // !WITH_CRIMSON
  } // namespace dmclock
} // namespace crimson
