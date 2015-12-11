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

    RequestTag() :
      proportion(0), reservation(0), limit(0)
    {
      // empty
    }

    friend std::ostream& operator<<(std::ostream&, const RequestTag&);
  };


  struct ClientInfo {
    double weight;       // proportional
    double reservation;  // minimum
    double limit;        // maximum

    friend std::ostream& operator<<(std::ostream&, const ClientInfo&);
  };

  struct ClientInfo_old {
    double weight;       // proportional
    double reservation;  // minimum
    double limit;        // maximum

    bool isIdle;

    RequestTag prevTag;

    ClientInfo_old() : ClientInfo_old(-1.0, -1.0, -1.0) {}

    ClientInfo_old(double w, double r, double l) :
      weight(w),
      reservation(r),
      limit(l),
      isIdle(true)
    {
      // empty
    }

    bool isUnset() const {
      return -1 == weight;
    }

    friend std::ostream& operator<<(std::ostream&, const ClientInfo_old&);
  };


  std::ostream& operator<<(std::ostream& out, const dmc::ClientInfo_old& client);
  std::ostream& operator<<(std::ostream& out, const dmc::RequestTag& tag);


  // T is client identifier type
  template<typename T>
  class ClientDB {

  protected:

    typename std::map<T,ClientInfo_old> map;

  public:

    // typedef std::map<T,ClientInfo_old>::const_iterator client_ref;

    // client_ref find(const T& clientId) const;
    ClientInfo_old* find(const int& clientId) {
      auto it = map.find(clientId);
      if (it == map.cend()) {
	return NULL;
      } else {
	return &it->second;
      }
    }
        
    void put(const T& clientId, const ClientInfo_old& info) {
      map[clientId] = info;
    }

    void clear(const T& clientId) {
      map.erase(clientId);
    }
  };

    
  template<typename R>
  class ClientQueue {

    typedef typename std::unique_ptr<R> RequestRef;

  public:

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

    std::deque<Entry>  queue;
    mutable std::mutex queue_mutex;
    bool               idle;

  public:

    ClientQueue() : idle(false) {}

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

    void push(RequestRef&& request) {
      std::lock_guard<std::mutex> guard(queue_mutex);
      queue.emplace_back(Entry(RequestTag(), std::move(request)));
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

    typedef typename std::function<ClientInfo(C)> ClientInfoFunc;

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
	    return q1->tag.priority < q2->tag.priority;
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


    ClientInfoFunc clientInfo;

    std::map<C,CQueueRef> clientMap;
    boost::heap::fibonacci_heap<CQueueRef,
				boost::heap::compare<ReservationCompare>> resQ;
    boost::heap::fibonacci_heap<CQueueRef,
				boost::heap::compare<LimitCompare>> limQ;
    boost::heap::fibonacci_heap<CQueueRef,
				boost::heap::compare<ProportionCompare>> propQ;
      
  public:

    PriorityQueue(ClientInfoFunc _clientInfo) :
      clientInfo(_clientInfo)
    {
      // empty
    }

    void test() {
      std::cout << clientInfo(0) << std::endl;
      std::cout << clientInfo(3) << std::endl;
    }

    void addRequest(R request, C client, Time time) {
#if 0
      ClientInfo_old* client = clientDB.find(client);
      if (!client) {
	
      }
#endif
      
    }

  }; // class PriorityQueue

    
} // namespace dmc
