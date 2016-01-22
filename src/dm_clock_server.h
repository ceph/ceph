// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#define DEBUGGER


#pragma once


#include <sys/time.h>
#include <assert.h>

#include <memory>
#include <map>
#include <deque>
#include <queue>
#include <mutex>
#include <thread>
#include <iostream>
#include <sstream>

#include "crimson/heap.h"
#include "dm_clock_util.h"
#include "dm_clock_recs.h"


namespace c = crimson;


namespace crimson {
  
  namespace dmclock {

    static bool info = true;

    // we're using double to represent time, but we could change it by
    // changing the following declarations (and by making sure a min
    // function existed)
    typedef double Time;
    static const double TimeZero = 0.0;
    static const double TimeMax = std::numeric_limits<Time>::max();

    inline Time getTime() {
      struct timeval now;
      assert(0 == gettimeofday(&now, NULL));
      return now.tv_sec + (now.tv_usec / 1000000.0);
    }

    
    inline std::string formatTime(const Time& time, uint modulo = 10000) {
      long subtract = long(time / modulo) * modulo;
      std::stringstream ss;
      ss << std::fixed << time - subtract;
      return ss.str();
    }


    struct ClientInfo {
      const double weight;       // proportional
      const double reservation;  // minimum
      const double limit;        // maximum

      // multiplicative inverses of above, which we use in calculations
      // and don't want to recalculate repeatedlu
      const double weight_inv;
      const double reservation_inv;
      const double limit_inv;

      ClientInfo(double _weight, double _reservation, double _limit) :
	weight(_weight),
	reservation(_reservation),
	limit(_limit),
	weight_inv(     0.0 == weight      ? 0.0 : 1.0 / weight),
	reservation_inv(0.0 == reservation ? 0.0 : 1.0 / reservation),
	limit_inv(      0.0 == limit       ? 0.0 : 1.0 / limit)
      {
	// empty
      }

      friend std::ostream& operator<<(std::ostream&, const ClientInfo&);
    }; // class ClientInfo


    std::ostream& operator<<(std::ostream& out,
			     const crimson::dmclock::ClientInfo& client);


    struct RequestTag {
      double proportion;
      double reservation;
      double limit;

      RequestTag(const RequestTag& prev_tag,
		 const ClientInfo& client,
		 Time time) :

	proportion(tagCalc(time, prev_tag.proportion, client.weight_inv)),
	reservation(tagCalc(time, prev_tag.reservation, client.reservation_inv)),
	limit(tagCalc(time, prev_tag.limit, client.limit_inv))
      {
	// empty
      }

      // copy constructor
      RequestTag(const RequestTag& other) :
	proportion(other.proportion),
	reservation(other.reservation),
	limit(other.limit)
      {
	// empty
      }

      RequestTag() :
	proportion(0.0), reservation(0.0), limit(0.0)
      {
	// empty
      }

    private:

      static double tagCalc(Time time, double prev, double increment) {
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

      typedef typename std::unique_ptr<R> RequestRef;

    protected:

      class ClientRec {
	friend PriorityQueue<C,R>;

	ClientInfo         info;
	RequestTag         prev_tag;
	bool               idle;

	ClientRec(const ClientInfo& _info) :
	  info(_info),
	  idle(true)
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


      typedef std::shared_ptr<Entry> EntryRef;

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
      typedef typename std::function<ClientInfo(C)>     ClientInfoFunc;

      // a function to see whether the server can handle another request
      typedef typename std::function<bool(void)>        CanHandleRequestFunc;

      // a function to submit a request to the server; the second
      // parameter is a callback when it's completed
      typedef
      typename std::function<void(RequestRef,PhaseType)> HandleRequestFunc;


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


      ClientInfoFunc       clientInfoF;
      CanHandleRequestFunc canHandleF;
      HandleRequestFunc    handleF;


      typedef typename std::lock_guard<std::mutex> Guard;

      mutable std::mutex data_mutex;


      // stable mappiing between client ids and client queues
      std::map<C,ClientRec> clientMap;

      // four heaps that maintain the earliest request by each of the
      // tag components
      c::Heap<EntryRef, ReservationCompare> resQ;
      c::Heap<EntryRef, ProportionCompare> propQ;

      // AKA not-ready queue
      c::Heap<EntryRef, LimitCompare> limQ;

      // for entries whose limit is passed and that'll be sorted by
      // their proportion tag
      c::Heap<EntryRef, ProportionCompare> readyQ;

      // if all reservations are met and all other requestes are under
      // limit, this will allow the request next in terms of
      // proportion to still get issued
      bool allowLimitBreak;


      // performance data collection
      size_t res_sched_count;
      size_t prop_sched_count;
      size_t limit_break_sched_count;

    public:

      PriorityQueue(ClientInfoFunc _clientInfoF,
		    CanHandleRequestFunc _canHandleF,
		    HandleRequestFunc _handleF,
		    bool _allowLimitBreak = false) :
	clientInfoF(_clientInfoF),
	canHandleF(_canHandleF),
	handleF(_handleF),
	allowLimitBreak(_allowLimitBreak),
	res_sched_count(0),
	prop_sched_count(0),
	limit_break_sched_count(0)
      {
	// empty
      }


      ~PriorityQueue() {
	if (info) {
	  std::cout << "Ops scheduled via reservation: " <<
	    res_sched_count << std::endl;
	  std::cout << "Ops scheduled via proportion: " <<
	    prop_sched_count << std::endl;
	  if (limit_break_sched_count > 0) {
	    std::cout << "Ops scheduled via limit break: " <<
	      limit_break_sched_count << std::endl;
	  }
	}
      }


      void markAsIdle(const C& client_id) {
	auto client_it = clientMap.find(client_id);
	if (clientMap.end() != client_it) {
	  client_it->second.idle = true;
	}
      }


      void addRequest(const R& request,
		      const C& client_id,
		      const Time& time) {
	addRequest(RequestRef(new R(request)), client_id, time);
      }


      void addRequest(RequestRef&& request,
		      const C& client_id,
		      const Time& time) {
	Guard g(data_mutex);

	auto client_it = clientMap.find(client_id);
	if (clientMap.end() == client_it) {
	  ClientInfo ci = clientInfoF(client_id);
	  clientMap.emplace(client_id, ClientRec(ci));
	  client_it = clientMap.find(client_id);
	}

	if (client_it->second.idle) {
	  while (!propQ.empty() && propQ.top()->handled) {
	    propQ.pop();
	  }
	  if (!propQ.empty()) {
	    double min_prop_tag = propQ.top()->tag.proportion;
	    double reduction = min_prop_tag - time;
	    for (auto i = propQ.begin(); i != propQ.end(); ++i) {
	      (*i)->tag.proportion -= reduction;
	    }
	  }
	  client_it->second.idle = false;
	}

	EntryRef entry(new Entry(client_id,
				 RequestTag(client_it->second.prev_tag,
					    client_it->second.info,
					    time),
				 std::move(request)));

	// copy tag to previous tag for client
	client_it->second.prev_tag = entry->tag;

	if (0.0 != entry->tag.reservation) {
	  resQ.push(entry);
	}

	if (0.0 != entry->tag.proportion) {
	  propQ.push(entry);

	  if (0.0 == entry->tag.limit) {
	    readyQ.push(entry);
	  } else {
	    limQ.push(entry);
	  }
	}

#if 0
	{
	  static uint count = 0;
	  ++count;
	  if (50 <= count && count < 55) {
	    std::cout << "addRequest:" << std::endl;
	    std::cout << "time:" << formatTime(time) << std::endl;
	    displayQueues();
	    std::cout << std::endl;
	    debugger();
	  }
	}
#endif

	scheduleRequest();
      }


      void requestCompleted() {
	Guard g(data_mutex);
	scheduleRequest();
      }


    protected:


      // for debugging
      void displayQueues() {
	auto filter = [](const EntryRef& e)->bool { return !e->handled; };
	resQ.displaySorted(std::cout << "RESER:", filter) << std::endl;
	limQ.displaySorted(std::cout << "LIMIT:", filter) << std::endl;
	readyQ.displaySorted(std::cout << "READY:", filter) << std::endl;
	propQ.displaySorted(std::cout << "PROPO:", filter) << std::endl;
      }

      
      void reduceReservationTags(C client_id) {
	auto client_it = clientMap.find(client_id);
	assert(clientMap.end() != client_it);
	double reduction = client_it->second.info.reservation_inv;
	for (auto i = resQ.begin(); i != resQ.end(); ++i) {
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
      C submitTopRequest(Heap<EntryRef, K>& heap, PhaseType phase) {
	EntryRef& top = heap.top();
	top->handled = true;
	handleF(std::move(top->request), phase);
	C client_result = top->client;
	heap.pop();
	return client_result;
      }


      // data_mutex should be held when called
      template<typename K>
      void prepareQueue(Heap<EntryRef, K>& heap) {
	while (!heap.empty() && heap.top()->handled) {
	  heap.pop();
	}
      }


      // data_mutex should be held when called
      void scheduleRequest() {
	if (!canHandleF()) {
	  return;
	}

	Time now = getTime();

	// so queue management is handled incrementally, remove
	// handled items from each of the queues
	prepareQueue(resQ);
	prepareQueue(readyQ);
	prepareQueue(limQ);
	prepareQueue(propQ);

	// try constraint (reservation) based scheduling

#if 0
	{
	  static uint count = 0;
	  ++count;
	  if (50 <= count && count <= 55) {
	    std::cout << "scheduleRequest A:" << std::endl;
	    std::cout << "now:" << formatTime(now) << std::endl;
	    displayQueues();
	    std::cout << std::endl;
	    debugger();
	  }
	}
#endif

	if (!resQ.empty() && resQ.top()->tag.reservation <= now) {
	  (void) submitTopRequest(resQ, PhaseType::reservation);
	  ++res_sched_count;
	  return;
	}

	// no existing reservations before now, so try weight-based
	// scheduling

	// all items that are within limit are eligible based on
	// priority
	while (!limQ.empty()) {
	  auto top = limQ.top();
	  if (top->handled) {
	    limQ.pop();
	  } else if (top->tag.limit <= now) {
	    readyQ.push(top);
	    limQ.pop();
	  } else {
	    break;
	  }
	}

#if 0
	{
	  static uint count = 0;
	  ++count;
	  if (50 <= count && count <= 55) {
	    std::cout << "scheduleRequest B:" << std::endl;
	    std::cout << "now:" << formatTime(now) << std::endl;
	    displayQueues();
	    std::cout << std::endl;
	    debugger();
	  }
	}
#endif

	if (!readyQ.empty()) {
	  C client = submitTopRequest(readyQ, PhaseType::priority);
	  reduceReservationTags(client);
	  ++prop_sched_count;
	  return;
	}

	if (allowLimitBreak) {
	  if (!propQ.empty()) {
	    C client = submitTopRequest(propQ, PhaseType::priority);
	    reduceReservationTags(client);
	    ++limit_break_sched_count;
	    return;
	  }
	}

	// nothing scheduled; make sure we re-run when next queued
	// item is ready

	Time nextCall = TimeMax;
	if (!resQ.empty()) {
	  nextCall = minNotZeroTime(nextCall, resQ.top()->tag.reservation);
	}
	if (!limQ.empty()) {
	  nextCall = minNotZeroTime(nextCall, limQ.top()->tag.limit);
	}
	if (!readyQ.empty()) {
	  nextCall = minNotZeroTime(nextCall, readyQ.top()->tag.proportion);
	}
	if (!propQ.empty()) {
	  nextCall = minNotZeroTime(nextCall, propQ.top()->tag.proportion);
	}
	if (nextCall < TimeMax) {
	  now = getTime();
	  // TODO rather than starting a thread, consider having a
	  // service thread that just calls into scheduleRequest based
	  // on the nearest interesting time; communicate to it w a
	  // mutex/cv.
	  std::thread t(
	    [this, nextCall, now]() {
	      long microseconds_l = long(1 + 1000000 * (nextCall - now));
	      auto microseconds = std::chrono::microseconds(microseconds_l);
	      std::this_thread::sleep_for(microseconds);
	      Guard g(data_mutex);
	      scheduleRequest();
	    });
	  t.detach();
	}
      } // scheduleRequest

      static inline const Time& minNotZeroTime(const Time& current,
					       const Time& possible) {
	return TimeZero == possible ? current : std::min(current, possible);
      }
    }; // class PriorityQueue
  } // namespace dmclock
} // namespace crimson
