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

#include <boost/heap/fibonacci_heap.hpp>
// #include <boost/heap/heap_concepts.hpp>


// dmClock namespace
namespace dmc {

  typedef double Time;

  inline Time getTime() {
    struct timeval now;
    assert(0 == gettimeofday(&now, NULL));
    return now.tv_sec + (now.tv_usec / 1000000.0);
  }

  struct RequestTag {
    double proportion;
    double reservation;
    double limit;

    RequestTag(double p, double r, double l) :
      proportion(p), reservation(r), limit(l)
    {
      // empty
    }

    RequestTag() : RequestTag(0, 0, 0)
    {
      // empty
    }

    friend std::ostream& operator<<(std::ostream&, const RequestTag&);
  };

  std::ostream& operator<<(std::ostream& out, const dmc::RequestTag& tag);

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
      weight_inv(1.0 / weight),
      reservation_inv(1.0 / reservation),
      limit_inv(1.0 / limit)
    {
      // empty
    }

    friend std::ostream& operator<<(std::ostream&, const ClientInfo&);
  };

  std::ostream& operator<<(std::ostream& out, const dmc::ClientInfo& client);


  template<typename R>
  class ClientQueue {

  public:

    typedef typename std::unique_ptr<R> RequestRef;

    struct Entry {
      RequestTag tag;
      RequestRef request;

      Entry(RequestTag t, RequestRef&& r) :
	tag(t), request(std::move(r))
      {
	// empty
      }

      Entry(Entry&& e) :
	tag(e.tag), request(std::move(e.request))
      {
	// empty
      }
    }; // struct Entry

  protected:

    typedef typename std::lock_guard<std::mutex> Guard;

    ClientInfo         info;
    RequestTag         prev_tag;
    std::deque<Entry>  queue;
    mutable std::mutex queue_mutex;
    bool               idle;

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
	Entry(
	  RequestTag(std::max(time,
			      prev_tag.proportion + 1.0 / info.weight),
		     std::max(time,
			      prev_tag.reservation + 1.0 / info.reservation),
		     std::max(time,
			      prev_tag.limit + 1.0 / info.limit)),
	  std::move(request)));
    }

    // can only be called when queue is not empty
    bool empty() const {
      Guard g(queue_mutex);
      return queue.empty();
    }
  }; // class ClientQueue


  // T is client identifier type, R is request type
  template<typename C, typename R>
  class PriorityQueue {

  public:
    typedef typename std::unique_ptr<R> RequestRef;

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

    typedef ClientQueue<R>                   CQueue;
    typedef typename std::shared_ptr<CQueue> CQueueRef;

    struct ReservationCompare {
      bool operator()(const CQueueRef& n1, const CQueueRef& n2) const {
	auto q1 = n1->peek();
	auto q2 = n2->peek();

	if (q1) {
	  if (q2) {
	    return q1->tag.reservation < q2->tag.reservation;
	  } else {
	    return true;
	  }
	} else {
	  return NULL == q2;
	}
      }
    };

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


    ClientInfoFunc clientInfoF;
    CanHandleRequestFunc canHandleF;
    HandleRequestFunc handleF;

    // stable mappiing between client ids and client queues
    std::map<C,CQueueRef> clientMap;


    // four heaps that maintain the earliest request by each of the
    // tag components
    boost::heap::fibonacci_heap<CQueueRef,
				boost::heap::compare<ReservationCompare>> resQ;
    boost::heap::fibonacci_heap<CQueueRef,
				boost::heap::compare<LimitCompare>> limQ;
    boost::heap::fibonacci_heap<CQueueRef,
				boost::heap::compare<ProportionCompare>> propQ;

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

    void addRequest(R request, C client_id, Time time) {
      auto client_it = clientMap.find(client_id);
      CQueueRef client;
      if (clientMap.end() == client_it) {
	ClientInfo ci = clientInfoF(client_id);
	client = CQueueRef(new ClientQueue<R>(ci));
	clientMap[client_id] = client;
      } else {
	client = client_it->second;
      }

      typename ClientQueue<R>::RequestRef req_ref(new R(request));
      bool was_empty = client->empty();
      client->push(std::move(req_ref), time);
      if (was_empty) {
	resQ.push(client);
	limQ.push(client);
	propQ.push(client);
      }
    }

  }; // class PriorityQueue


} // namespace dmc
