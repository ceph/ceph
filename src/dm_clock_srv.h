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

      RequestTag(double p, double r, double l) :
	proportion(p), reservation(r), limit(l)
      {
	// empty
      }

      RequestTag(const RequestTag& prev_tag,
		 const ClientInfo& client,
		 Time time) :
	proportion(std::max(time,
			    prev_tag.proportion + client.weight_inv)),
	reservation(std::max(time,
			     prev_tag.reservation + client.reservation_inv)),
	limit(std::max(time,
		       prev_tag.limit + client.limit_inv))
      {
	// empty
      }

      RequestTag(const RequestTag& other) :
	proportion(other.proportion),
	reservation(other.reservation),
	limit(other.limit)
      {
	// empty
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

#if 0

    template<typename C, typename R>
    class ClientQueue{

    protected:

      typedef typename std::lock_guard<std::mutex> Guard;
      typedef typename std::shared_ptr<ClientQueue<R>> HeapEntry;
    

    public:

      ClientQueue(const ClientInfo& _info) : info(_info), idle(false) {}

      const ClientInfo& getInfo() const { return info; }
      const RequestTag& getPrevTag() const { return prev_tag; }

      const Entry* peek() const {
	Guard g(queue_mutex);
	if (queue.empty()) {
	  return NULL;
	} else {
	  return &queue.front();
	}
      }

      // can only be called when queue is not empty
      void pop() {
	Guard g(queue_mutex);
	queue.pop_front();
      }

      void push(RequestRef&& request, Time time) {
	std::lock_guard<std::mutex> guard(queue_mutex);
	queue.emplace_back(
      }

      // can only be called when queue is not empty
      bool empty() const {
	Guard g(queue_mutex);
	return queue.empty();
      }
    }; // class ClientQueue

#endif

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
	bool operator()(const Entry& n1, const Entry& n2) const {
	  return n1.tag.reservation < n2.tag.reservation;
	}
      };

#define NOT_YET 0

#if NOT_YET
      struct ProportionCompare {
	bool operator()(const CQueueRef& n1, const CQueueRef& n2) const {
	  auto q1 = n1->peek();
	  auto q2 = n2->peek();

	  if (q1) {
	    if (q2) {
	      return q1->tag.proportion < q2->tag.proportion;
	    } else {
	      return true;
	    }
	  } else {
	    return NULL == q2;
	  }
	}
      };

      struct LimitCompare {
	bool operator()(const CQueueRef& n1, const CQueueRef& n2) const {
	  auto q1 = n1->peek();
	  auto q2 = n2->peek();

	  if (q1) {
	    if (q2) {
	      return q1->tag.limit < q2->tag.limit;
	    } else {
	      return true;
	    }
	  } else {
	    return NULL == q2;
	  }
	}
      };

#endif

      ClientInfoFunc clientInfoF;
      CanHandleRequestFunc canHandleF;
      HandleRequestFunc handleF;


      typedef typename std::lock_guard<std::mutex> Guard;

      mutable std::mutex data_mutex;


      // stable mappiing between client ids and client queues
      std::map<C,ClientRec> clientMap;

      typedef std::shared_ptr<Entry> EntryRef;

      // four heaps that maintain the earliest request by each of the
      // tag components
      c::Heap<EntryRef, ReservationCompare> resQ;

#if NOT_YET
      heap::fibonacci_heap<CQueueRef,
			   heap::compare<LimitCompare>> limQ;
      heap::fibonacci_heap<CQueueRef,
			   heap::compare<ProportionCompare>> propQ;
      heap::fibonacci_heap<CQueueRef,
			   heap::compare<ProportionCompare>> readyQ;
#endif

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

      void test() {
	std::cout << clientInfoF(0) << std::endl;
	std::cout << clientInfoF(3) << std::endl;
	std::cout << clientInfoF(99) << std::endl;
      }


      void addRequest(RequestRef&& request, C client_id, Time time) {
	Guard g(data_mutex);

	auto client_it = clientMap.find(client_id);
	if (clientMap.end() == client_it) {
	  ClientInfo ci = clientInfoF(client_id);
	  clientMap[client_id] = ClientRec(ci);
	  client_it = clientMap.find(client_id);
	}

	if (client_it->second.idle) {
	  std::cout << "Request for idle client; do something." << std::endl;
	  client_it->second.idle = false;
	}

	EntryRef entry(new Entry(client_id,
				 RequestTag(client_it->second.prev_tag,
					    time),
				 std::move(request)));

#if NOT_YET
	typename ClientQueue<R>::RequestRef req_ref(new R(request));
	// bool was_empty = client->empty();
	client->push(std::move(req_ref), time);
	if (add_to_queues) {
	  resQ.push(client);
	  limQ.push(client);
	  propQ.push(client);
	}
#endif

	scheduleRequest();
      }

    protected:

      // data_mutex should be held when called
      void scheduleRequest() {
	if (!canHandleF()) {
	  return;
	}

	if (!resQ.empty()) {
	  auto top = resQ.top()->peek();
	  while (top && top->handled) {
	    resQ.pop();

	    auto handle = resQ.s_handle_from_iterator(resQ.begin());
	
	    resQ.update(handle);

	    top = resQ.top()->peek();
	  }
	} // resQ not empty
      }

      void requestComplete() {
	Guard g(data_mutex);
	scheduleRequest();
      }
  
    }; // class PriorityQueue

  } // namespace dmclock
} // namespace crimson
