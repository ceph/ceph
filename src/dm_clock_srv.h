// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#include <sys/time.h>
#include <assert.h>

#include <memory>
#include <map>
#include <deque>
#include <queue>
#include <mutex>
#include <iostream>

#include "crimson/heap.h"


namespace c = crimson;


namespace crimson {
  namespace dmclock {

    typedef double Time;

    inline Time getTime() {
      struct timeval now;
      assert(0 == gettimeofday(&now, NULL));
      return now.tv_sec + (now.tv_usec / 1000000.0);
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

      RequestTag(double p, double r, double l) :
	proportion(p), reservation(r), limit(l)
      {
	// empty
      }

      static double tagCalc(Time time, double prev, double increment) {
	if (0.0 == increment) {
	  return 0.0;
	} else {
	  return std::max(time, prev + increment);
	}
      }

#if 0
      RequestTag() : RequestTag(0, 0, 0)
      {
	// empty
      }
#endif

      friend std::ostream& operator<<(std::ostream&, const RequestTag&);
    };

    std::ostream& operator<<(std::ostream& out,
			     const crimson::dmclock::RequestTag& tag);

    // T is client identifier type, R is request type
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

#if 0
	Entry(Entry&& e) :
	  tag(e.tag), handled(e.handled), request(std::move(e.request))
	{
	  // empty
	}
#endif
      }; // struct Entry

      typedef std::shared_ptr<Entry> EntryRef;
  
    public:

      // a function that can be called to look up client information
      typedef typename std::function<ClientInfo(C)>       ClientInfoFunc;

      // a function to see whether the server can handle another request
      typedef typename std::function<bool(void)>          CanHandleRequestFunc;

      // a function to submit a request to the server; the second
      // parameter is a callback when it's completed
      typedef
      typename std::function<void(RequestRef,
				  std::function<void()>)> HandleRequestFunc;

    protected:

      struct ReservationCompare {
	bool operator()(const EntryRef& n1, const EntryRef& n2) const {
	  return n1->tag.reservation < n2->tag.reservation;
	}
      };

      struct ProportionCompare {
	bool operator()(const EntryRef& n1, const EntryRef& n2) const {
	  return n1->tag.proportion < n2->tag.proportion;
	}
      };

      struct LimitCompare {
	bool operator()(const EntryRef& n1, const EntryRef& n2) const {
	  return n1->tag.limit < n2->tag.limit;
	}
      };


      ClientInfoFunc clientInfoF;
      CanHandleRequestFunc canHandleF;
      HandleRequestFunc handleF;


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

    public:

      PriorityQueue(ClientInfoFunc _clientInfoF,
		    CanHandleRequestFunc _canHandleF,
		    HandleRequestFunc _handleF) :
	clientInfoF(_clientInfoF),
	canHandleF(_canHandleF),
	handleF(_handleF)
      {
	// empty
      }


      void addRequest(const R& request,
		      const C& client_id,
		      const Time& time) {
	addRequest(RequestRef(new R(request)), client_id, time);
	// addRequest(std::unique_ptr<R>(new R(request)), client_id, time);
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
	  std::cout << "Request for idle client; do something." << std::endl;
	  client_it->second.idle = false;
	}

	EntryRef entry(new Entry(client_id,
				 RequestTag(client_it->second.prev_tag,
					    client_it->second.info,
					    time),
				 std::move(request)));
	if (0.0 != entry->tag.reservation) {
	  resQ.push(entry);
	}

	if (0.0 == entry->tag.limit) {
	  readyQ.push(entry);
	} else {
	  limQ.push(entry);
	}

	if (0.0 != entry->tag.proportion) {
	  propQ.push(entry);
	}

	scheduleRequest();
      }

    protected:

      // data_mutex should be held when called
      void scheduleRequest() {
	if (!canHandleF()) {
	  return;
	}

	Time now = getTime();

	while (!resQ.empty()) {
	  if (resQ.top()->handled) {
	    resQ.pop();
	  }
	}

#if 0
	if (!resQ.empty()) {
	  auto top = resQ.top();
	  while (top && top->handled) {
	    resQ.pop();

	    auto handle = resQ.s_handle_from_iterator(resQ.begin());
	
	    resQ.update(handle);

	    top = resQ.top()->peek();
	  }
	} // resQ not empty
#endif
      }

      void requestComplete() {
	Guard g(data_mutex);
	scheduleRequest();
      }
  
    }; // class PriorityQueue

  } // namespace dmclock
} // namespace crimson
