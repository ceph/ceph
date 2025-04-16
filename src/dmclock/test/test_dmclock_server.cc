// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#include <memory>
#include <chrono>
#include <iostream>
#include <list>
#include <vector>


#include "dmclock_server.h"
#include "dmclock_util.h"
#include "gtest/gtest.h"

// process control to prevent core dumps during gtest death tests
#include "dmcPrCtl.h"


namespace dmc = crimson::dmclock;


// we need a request object; an empty one will do
struct Request {
};


namespace crimson {
  namespace dmclock {

    /*
     * Allows us to test the code provided with the mutex provided locked.
     */
    static void test_locked(std::mutex& mtx, std::function<void()> code) {
      std::unique_lock<std::mutex> l(mtx);
      code();
    }


    TEST(dmclock_server, bad_tag_deathtest) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 18;

      double reservation = 0.0;
      double weight = 0.0;

      dmc::ClientInfo ci1(reservation, weight, 0.0);
      dmc::ClientInfo ci2(reservation, weight, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &ci1;
	else if (client2 == c) return &ci2;
	else {
	  ADD_FAILURE() << "got request from neither of two clients";
	  return nullptr;
	}
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));
      ReqParams req_params(1,1);

      // Disable coredumps
      PrCtl unset_dumpable;

      EXPECT_DEATH_IF_SUPPORTED(pq->add_request(Request{}, client1, req_params),
				"Assertion.*reservation.*max_tag.*"
				"proportion.*max_tag") <<
	"we should fail if a client tries to generate a reservation tag "
	"where reservation and proportion are both 0";


      EXPECT_DEATH_IF_SUPPORTED(pq->add_request(Request{}, client2, req_params),
				"Assertion.*reservation.*max_tag.*"
				"proportion.*max_tag") <<
	"we should fail if a client tries to generate a reservation tag "
	"where reservation and proportion are both 0";

      EXPECT_DEATH_IF_SUPPORTED(Queue(client_info_f, AtLimit::Reject),
				"Assertion.*Reject.*Delayed") <<
	"we should fail if a client tries to construct a queue with both "
        "DelayedTagCalc and AtLimit::Reject";
    }


    TEST(dmclock_server, client_idle_erase) {
      using ClientId = int;
      using Queue = dmc::PushPriorityQueue<ClientId,Request>;
      ClientId client = 17;
      double reservation = 100.0;

      dmc::ClientInfo ci(reservation, 1.0, 0.0);
      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &ci;
      };
      auto server_ready_f = [] () -> bool { return true; };
      auto submit_req_f = [] (const ClientId& c,
			      std::unique_ptr<Request> req,
			      dmc::PhaseType phase,
			      uint64_t req_cost) {
	// empty; do nothing
      };

      Queue pq(client_info_f,
	       server_ready_f,
	       submit_req_f,
	       std::chrono::seconds(3),
	       std::chrono::seconds(5),
	       std::chrono::seconds(2),
	       AtLimit::Wait);

      auto lock_pq = [&](std::function<void()> code) {
	test_locked(pq.data_mtx, code);
      };


      /* The timeline should be as follows:
       *
       *     0 seconds : request created
       *
       *     1 seconds : map is size 1, idle is false
       *
       * 2 seconds : clean notes first mark; +2 is base for further calcs
       *
       * 4 seconds : clean does nothing except makes another mark
       *
       *   5 seconds : when we're secheduled to idle (+2 + 3)
       *
       * 6 seconds : clean idles client
       *
       *   7 seconds : when we're secheduled to erase (+2 + 5)
       *
       *     7 seconds : verified client is idle
       *
       * 8 seconds : clean erases client info
       *
       *     9 seconds : verified client is erased
       */

      lock_pq([&] () {
	  EXPECT_EQ(0u, pq.client_map.size()) <<
	    "client map initially has size 0";
	});

      Request req;
      dmc::ReqParams req_params(1, 1);
      EXPECT_EQ(0, pq.add_request_time(req, client, req_params, dmc::get_time()));

      std::this_thread::sleep_for(std::chrono::seconds(1));

      lock_pq([&] () {
	  EXPECT_EQ(1u, pq.client_map.size()) <<
	    "client map has 1 after 1 client";
	  EXPECT_FALSE(pq.client_map.at(client)->idle) <<
	    "initially client map entry shows not idle.";
	});

      std::this_thread::sleep_for(std::chrono::seconds(6));

      lock_pq([&] () {
	  EXPECT_TRUE(pq.client_map.at(client)->idle) <<
	    "after idle age client map entry shows idle.";
	});

      std::this_thread::sleep_for(std::chrono::seconds(2));

      lock_pq([&] () {
	  EXPECT_EQ(0u, pq.client_map.size()) <<
	    "client map loses its entry after erase age";
	});
    } // TEST


    TEST(dmclock_server, add_req_pushprio_queue) {
      struct MyReq {
        int id;

        MyReq(int _id) :
          id(_id)
        {
          // empty
        }
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PushPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;
      ClientId client1 = 17;
      ClientId client2 = 34;

      dmc::ClientInfo ci(0.0, 1.0, 0.0);
      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        return &ci;
      };
      auto server_ready_f = [] () -> bool { return true; };
      auto submit_req_f = [] (const ClientId& c,
                              std::unique_ptr<MyReq> req,
                              dmc::PhaseType phase,
                              uint64_t req_cost) {
        // empty; do nothing
      };

      Queue pq(client_info_f,
               server_ready_f,
               submit_req_f,
               std::chrono::seconds(3),
               std::chrono::seconds(5),
               std::chrono::seconds(2),
               AtLimit::Wait);

      auto lock_pq = [&](std::function<void()> code) {
        test_locked(pq.data_mtx, code);
      };

      lock_pq([&] () {
          EXPECT_EQ(0u, pq.client_map.size()) <<
            "client map initially has size 0";
        });

      dmc::ReqParams req_params(1, 1);

      // Create a reference to a request
      MyReqRef rr1 = MyReqRef(new MyReq(11));

      // Exercise different versions of add_request()
      EXPECT_EQ(0, pq.add_request(std::move(rr1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(22), client2, req_params));

      std::this_thread::sleep_for(std::chrono::seconds(1));

      lock_pq([&] () {
          EXPECT_EQ(2u, pq.client_map.size()) <<
            "client map has 2 after 2 clients";
          EXPECT_FALSE(pq.client_map.at(client1)->idle) <<
            "initially client1 map entry shows not idle.";
          EXPECT_FALSE(pq.client_map.at(client2)->idle) <<
            "initially client2 map entry shows not idle.";
        });

      // Check client idle state
      std::this_thread::sleep_for(std::chrono::seconds(6));
      lock_pq([&] () {
          EXPECT_TRUE(pq.client_map.at(client1)->idle) <<
            "after idle age client1 map entry shows idle.";
          EXPECT_TRUE(pq.client_map.at(client2)->idle) <<
            "after idle age client2 map entry shows idle.";
        });

      // Sleep until after erase age elapses
      std::this_thread::sleep_for(std::chrono::seconds(2));

      lock_pq([&] () {
          EXPECT_EQ(0u, pq.client_map.size()) <<
          "client map loses its entries after erase age";
        });
    } // TEST


    TEST(dmclock_server, schedule_req_pushprio_queue) {
      struct MyReq {
        int id;

        MyReq(int _id) :
          id(_id)
        {
          // empty
        }
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PushPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;
      ClientId client1 = 17;
      ClientId client2 = 34;
      ClientId client3 = 48;

      dmc::ClientInfo ci(1.0, 1.0, 1.0);
      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        return &ci;
      };
      auto server_ready_f = [] () -> bool { return true; };
      auto submit_req_f = [] (const ClientId& c,
                              std::unique_ptr<MyReq> req,
                              dmc::PhaseType phase,
                              uint64_t req_cost) {
        // empty; do nothing
      };

      Queue pq(client_info_f,
               server_ready_f,
               submit_req_f,
               std::chrono::seconds(3),
               std::chrono::seconds(5),
               std::chrono::seconds(2),
               AtLimit::Wait);

      dmc::ReqParams req_params(1, 1);

      // Create a reference to a request
      MyReqRef rr1 = MyReqRef(new MyReq(11));

      // Exercise different versions of add_request()
      EXPECT_EQ(0, pq.add_request(std::move(rr1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(22), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(33), client3, req_params));

      std::this_thread::sleep_for(std::chrono::seconds(4));

      ASSERT_TRUE(pq.request_count() == 0);
    } // TEST


    TEST(dmclock_server, delayed_tag_calc) {
      using ClientId = int;
      constexpr ClientId client1 = 17;

      using DelayedQueue = PullPriorityQueue<ClientId, Request, true>;
      using ImmediateQueue = PullPriorityQueue<ClientId, Request, false>;

      ClientInfo info(0.0, 1.0, 1.0);
      auto client_info_f = [&] (ClientId c) -> const ClientInfo* {
	return &info;
      };

      Time t{1};
      {
	DelayedQueue queue(client_info_f);

	queue.add_request_time({}, client1, {0,0}, t);
	queue.add_request_time({}, client1, {0,0}, t + 1);
	queue.add_request_time({}, client1, {10,10}, t + 2);

	auto pr1 = queue.pull_request(t);
	ASSERT_TRUE(pr1.is_retn());
	auto pr2 = queue.pull_request(t + 1);
	// ReqParams{10,10} from request #3 pushes request #2 over limit by 10s
	ASSERT_TRUE(pr2.is_future());
	EXPECT_DOUBLE_EQ(t + 11, pr2.getTime());
      }
      {
	ImmediateQueue queue(client_info_f);

	queue.add_request_time({}, client1, {0,0}, t);
	queue.add_request_time({}, client1, {0,0}, t + 1);
	queue.add_request_time({}, client1, {10,10}, t + 2);

	auto pr1 = queue.pull_request(t);
	ASSERT_TRUE(pr1.is_retn());
	auto pr2 = queue.pull_request(t + 1);
	// ReqParams{10,10} from request #3 has no effect on request #2
	ASSERT_TRUE(pr2.is_retn());
	auto pr3 = queue.pull_request(t + 2);
	ASSERT_TRUE(pr3.is_future());
	EXPECT_DOUBLE_EQ(t + 12, pr3.getTime());
      }
    }

#if 0
    TEST(dmclock_server, reservation_timing) {
      using ClientId = int;
      // NB? PUSH OR PULL
      using Queue = std::unique_ptr<dmc::PriorityQueue<ClientId,Request>>;
      using std::chrono::steady_clock;

      int client = 17;

      std::vector<dmc::Time> times;
      std::mutex times_mtx;
      using Guard = std::lock_guard<decltype(times_mtx)>;

      // reservation every second
      dmc::ClientInfo ci(1.0, 0.0, 0.0);
      Queue pq;

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &ci;
      };
      auto server_ready_f = [] () -> bool { return true; };
      auto submit_req_f = [&] (const ClientId& c,
			       std::unique_ptr<Request> req,
			       dmc::PhaseType phase) {
	{
	  Guard g(times_mtx);
	  times.emplace_back(dmc::get_time());
	}
	std::thread complete([&](){ pq->request_completed(); });
	complete.detach();
      };

      // NB? PUSH OR PULL
      pq = Queue(new dmc::PriorityQueue<ClientId,Request>(client_info_f,
							  server_ready_f,
							  submit_req_f,
							  false));

      Request req;
      ReqParams<ClientId> req_params(client, 1, 1);

      for (int i = 0; i < 5; ++i) {
	pq->add_request_time(req, req_params, dmc::get_time());
      }

      {
	Guard g(times_mtx);
	std::this_thread::sleep_for(std::chrono::milliseconds(5500));
	EXPECT_EQ(5, times.size()) <<
	  "after 5.5 seconds, we should have 5 requests times at 1 second apart";
      }
    } // TEST
#endif


    TEST(dmclock_server, remove_by_req_filter) {
      struct MyReq {
	int id;

	MyReq(int _id) :
	  id(_id)
	{
	  // empty
	}
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info1;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      ReqParams req_params(1,1);

      EXPECT_EQ(0, pq.add_request(MyReq(1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(11), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(0), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(13), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(13), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(98), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(44), client1, req_params));

      EXPECT_EQ(2u, pq.client_count());
      EXPECT_EQ(9u, pq.request_count());

      pq.remove_by_req_filter([](MyReqRef&& r) -> bool {return 1 == r->id % 2;});

      EXPECT_EQ(5u, pq.request_count());

      std::list<MyReq> capture;
      pq.remove_by_req_filter(
	[&capture] (MyReqRef&& r) -> bool {
	  if (0 == r->id % 2) {
	    capture.push_front(*r);
	    return true;
	  } else {
	    return false;
	  }
	},
	true);

      EXPECT_EQ(0u, pq.request_count());
      EXPECT_EQ(5u, capture.size());
      int total = 0;
      for (auto i : capture) {
	total += i.id;
      }
      EXPECT_EQ(146, total) << " sum of captured items should be 146";
    } // TEST


    TEST(dmclock_server, remove_by_req_filter_ordering_forwards_visit) {
      struct MyReq {
	int id;

	MyReq(int _id) :
	  id(_id)
	{
	  // empty
	}
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;

      ClientId client1 = 17;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info1;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      ReqParams req_params(1,1);

      EXPECT_EQ(0, pq.add_request(MyReq(1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(3), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(4), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(5), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(6), client1, req_params));

      EXPECT_EQ(1u, pq.client_count());
      EXPECT_EQ(6u, pq.request_count());

      // remove odd ids in forward order and append to end

      std::vector<MyReq> capture;
      pq.remove_by_req_filter(
	[&capture] (MyReqRef&& r) -> bool {
	  if (1 == r->id % 2) {
	    capture.push_back(*r);
	    return true;
	  } else {
	    return false;
	  }
	},
	false);

      EXPECT_EQ(3u, pq.request_count());
      EXPECT_EQ(3u, capture.size());
      EXPECT_EQ(1, capture[0].id) << "items should come out in forward order";
      EXPECT_EQ(3, capture[1].id) << "items should come out in forward order";
      EXPECT_EQ(5, capture[2].id) << "items should come out in forward order";

      // remove even ids in reverse order but insert at front so comes
      // out forwards

      std::vector<MyReq> capture2;
      pq.remove_by_req_filter(
	[&capture2] (MyReqRef&& r) -> bool {
	  if (0 == r->id % 2) {
	    capture2.insert(capture2.begin(), *r);
	    return true;
	  } else {
	    return false;
	  }
	},
	false);

      EXPECT_EQ(0u, pq.request_count());
      EXPECT_EQ(3u, capture2.size());
      EXPECT_EQ(6, capture2[0].id) << "items should come out in reverse order";
      EXPECT_EQ(4, capture2[1].id) << "items should come out in reverse order";
      EXPECT_EQ(2, capture2[2].id) << "items should come out in reverse order";
    } // TEST


    TEST(dmclock_server, remove_by_req_filter_ordering_backwards_visit) {
      struct MyReq {
	int id;

	MyReq(int _id) :
	  id(_id)
	{
	  // empty
	}
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;

      ClientId client1 = 17;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info1;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      ReqParams req_params(1,1);

      EXPECT_EQ(0, pq.add_request(MyReq(1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(3), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(4), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(5), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(6), client1, req_params));

      EXPECT_EQ(1u, pq.client_count());
      EXPECT_EQ(6u, pq.request_count());

      // now remove odd ids in forward order

      std::vector<MyReq> capture;
      pq.remove_by_req_filter(
	[&capture] (MyReqRef&& r) -> bool {
	  if (1 == r->id % 2) {
	    capture.insert(capture.begin(), *r);
	    return true;
	  } else {
	    return false;
	  }
	},
	true);

      EXPECT_EQ(3u, pq.request_count());
      EXPECT_EQ(3u, capture.size());
      EXPECT_EQ(1, capture[0].id) << "items should come out in forward order";
      EXPECT_EQ(3, capture[1].id) << "items should come out in forward order";
      EXPECT_EQ(5, capture[2].id) << "items should come out in forward order";

      // now remove even ids in reverse order

      std::vector<MyReq> capture2;
      pq.remove_by_req_filter(
	[&capture2] (MyReqRef&& r) -> bool {
	  if (0 == r->id % 2) {
	    capture2.push_back(*r);
	    return true;
	  } else {
	    return false;
	  }
	},
	true);

      EXPECT_EQ(0u, pq.request_count());
      EXPECT_EQ(3u, capture2.size());
      EXPECT_EQ(6, capture2[0].id) << "items should come out in reverse order";
      EXPECT_EQ(4, capture2[1].id) << "items should come out in reverse order";
      EXPECT_EQ(2, capture2[2].id) << "items should come out in reverse order";
    } // TEST


    TEST(dmclock_server, remove_by_client) {
      struct MyReq {
	int id;

	MyReq(int _id) :
	  id(_id)
	{
	  // empty
	}
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info1;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      ReqParams req_params(1,1);

      EXPECT_EQ(0, pq.add_request(MyReq(1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(11), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(0), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(13), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(13), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(98), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(44), client1, req_params));

      EXPECT_EQ(2u, pq.client_count());
      EXPECT_EQ(9u, pq.request_count());

      std::list<MyReq> removed;

      pq.remove_by_client(client1,
			  true,
			  [&removed] (MyReqRef&& r) {
			    removed.push_front(*r);
			  });

      EXPECT_EQ(3u, removed.size());
      EXPECT_EQ(1, removed.front().id);
      removed.pop_front();
      EXPECT_EQ(11, removed.front().id);
      removed.pop_front();
      EXPECT_EQ(44, removed.front().id);
      removed.pop_front();

      EXPECT_EQ(6u, pq.request_count());

      Queue::PullReq pr = pq.pull_request();
      EXPECT_TRUE(pr.is_retn());
      EXPECT_EQ(2, pr.get_retn().request->id);

      pr = pq.pull_request();
      EXPECT_TRUE(pr.is_retn());
      EXPECT_EQ(0, pr.get_retn().request->id);

      pq.remove_by_client(client2);
      EXPECT_EQ(0u, pq.request_count()) <<
	"after second client removed, none left";
    } // TEST


    TEST(dmclock_server, add_req_ref) {
      struct MyReq {
        int id;

        MyReq(int _id) :
          id(_id)
        {
          // empty
        }
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;
      ClientId client1 = 22;
      ClientId client2 = 44;

      dmc::ClientInfo info(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        return &info;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      ReqParams req_params(1,1);

      MyReqRef rr1 = MyReqRef(new MyReq(1));
      MyReqRef rr2 = MyReqRef(new MyReq(2));
      MyReqRef rr3 = MyReqRef(new MyReq(3));
      MyReqRef rr4 = MyReqRef(new MyReq(4));
      MyReqRef rr5 = MyReqRef(new MyReq(5));
      EXPECT_EQ(0, pq.add_request(std::move(rr1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(std::move(rr2), client2, req_params));
      EXPECT_EQ(0, pq.add_request(std::move(rr3), client1, req_params));
      EXPECT_EQ(0, pq.add_request(std::move(rr4), client2, req_params));
      EXPECT_EQ(0, pq.add_request(std::move(rr5), client2, req_params));

      EXPECT_EQ(2u, pq.client_count());
      EXPECT_EQ(5u, pq.request_count());

      pq.remove_by_req_filter([](MyReqRef&& r) -> bool {return 0 == r->id % 2;});

      EXPECT_EQ(3u, pq.request_count());

      std::list<MyReq> capture;
      pq.remove_by_req_filter(
        [&capture] (MyReqRef&& r) -> bool {
          if (1 == r->id % 2) {
            capture.push_front(*r);
            return true;
          } else {
            return false;
          }
        },
        true);

      EXPECT_EQ(0u, pq.request_count());
      EXPECT_EQ(3u, capture.size());
      int total = 0;
      for (auto i : capture) {
        total += i.id;
      }
      EXPECT_EQ(9, total) << " sum of captured items should be 9";
    } // TEST


     TEST(dmclock_server, add_req_ref_null_req_params) {
      struct MyReq {
        int id;

        MyReq(int _id) :
          id(_id)
        {
          // empty
        }
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;
      ClientId client1 = 22;
      ClientId client2 = 44;

      dmc::ClientInfo info(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        return &info;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      MyReqRef&& rr1 = MyReqRef(new MyReq(1));
      MyReqRef&& rr2 = MyReqRef(new MyReq(2));
      MyReqRef&& rr3 = MyReqRef(new MyReq(3));
      MyReqRef&& rr4 = MyReqRef(new MyReq(4));
      MyReqRef&& rr5 = MyReqRef(new MyReq(5));
      EXPECT_EQ(0, pq.add_request(std::move(rr1), client1));
      EXPECT_EQ(0, pq.add_request(std::move(rr2), client2));
      EXPECT_EQ(0, pq.add_request(std::move(rr3), client1));
      EXPECT_EQ(0, pq.add_request(std::move(rr4), client2));
      EXPECT_EQ(0, pq.add_request(std::move(rr5), client2));

      EXPECT_EQ(2u, pq.client_count());
      EXPECT_EQ(5u, pq.request_count());

      pq.remove_by_req_filter([](MyReqRef&& r) -> bool {return 1 == r->id % 2;});

      EXPECT_EQ(2u, pq.request_count());

      std::list<MyReq> capture;
      pq.remove_by_req_filter(
        [&capture] (MyReqRef&& r) -> bool {
          if (0 == r->id % 2) {
            capture.push_front(*r);
            return true;
          } else {
            return false;
          }
        },
        true);

      EXPECT_EQ(0u, pq.request_count());
      EXPECT_EQ(2u, capture.size());
      int total = 0;
      for (auto i : capture) {
        total += i.id;
      }
      EXPECT_EQ(6, total) << " sum of captured items should be 6";
    } // TEST


  TEST(dmclock_server_pull, pull_weight) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);
      dmc::ClientInfo info2(0.0, 2.0, 0.0);

      QueueRef pq;

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &info1;
	else if (client2 == c) return &info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existent client";
	  return nullptr;
	}
      };

      pq = QueueRef(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(1,1);

      auto now = dmc::get_time();

      for (int i = 0; i < 5; ++i) {
	EXPECT_EQ(0, pq->add_request(Request{}, client1, req_params));
	EXPECT_EQ(0, pq->add_request(Request{}, client2, req_params));
	now += 0.0001;
      }

      int c1_count = 0;
      int c2_count = 0;
      for (int i = 0; i < 6; ++i) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	auto& retn = std::get<Queue::PullReq::Retn>(pr.data);

	if (client1 == retn.client) ++c1_count;
	else if (client2 == retn.client) ++c2_count;
	else ADD_FAILURE() << "got request from neither of two clients";

	EXPECT_EQ(PhaseType::priority, retn.phase);
      }

      EXPECT_EQ(2, c1_count) <<
	"one-third of request should have come from first client";
      EXPECT_EQ(4, c2_count) <<
	"two-thirds of request should have come from second client";
    }


    TEST(dmclock_server_pull, pull_reservation) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 8;

      dmc::ClientInfo info1(2.0, 0.0, 0.0);
      dmc::ClientInfo info2(1.0, 0.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &info1;
	else if (client2 == c) return &info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existent client";
	  return nullptr;
	}
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto old_time = dmc::get_time() - 100.0;

      for (int i = 0; i < 5; ++i) {
	EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, old_time));
	EXPECT_EQ(0, pq->add_request_time(Request{}, client2, req_params, old_time));
	old_time += 0.001;
      }

      int c1_count = 0;
      int c2_count = 0;

      for (int i = 0; i < 6; ++i) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	auto& retn = std::get<Queue::PullReq::Retn>(pr.data);

	if (client1 == retn.client) ++c1_count;
	else if (client2 == retn.client) ++c2_count;
	else ADD_FAILURE() << "got request from neither of two clients";

	EXPECT_EQ(PhaseType::reservation, retn.phase);
      }

      EXPECT_EQ(4, c1_count) <<
	"two-thirds of request should have come from first client";
      EXPECT_EQ(2, c2_count) <<
	"one-third of request should have come from second client";
    } // dmclock_server_pull.pull_reservation


    TEST(dmclock_server_pull, update_client_info) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request,false>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 100.0, 0.0);
      dmc::ClientInfo info2(0.0, 200.0, 0.0);

      QueueRef pq;

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &info1;
	else if (client2 == c) return &info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existent client";
	  return nullptr;
	}
      };

      pq = QueueRef(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(1,1);

      auto now = dmc::get_time();

      for (int i = 0; i < 5; ++i) {
	EXPECT_EQ(0, pq->add_request(Request{}, client1, req_params));
	EXPECT_EQ(0, pq->add_request(Request{}, client2, req_params));
	now += 0.0001;
      }

      int c1_count = 0;
      int c2_count = 0;
      for (int i = 0; i < 10; ++i) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	auto& retn = std::get<Queue::PullReq::Retn>(pr.data);

	if (i > 5) continue;
	if (client1 == retn.client) ++c1_count;
	else if (client2 == retn.client) ++c2_count;
	else ADD_FAILURE() << "got request from neither of two clients";

	EXPECT_EQ(PhaseType::priority, retn.phase);
      }

      EXPECT_EQ(2, c1_count) <<
	"before: one-third of request should have come from first client";
      EXPECT_EQ(4, c2_count) <<
	"before: two-thirds of request should have come from second client";

      std::chrono::seconds dura(1);
      std::this_thread::sleep_for(dura);

      info1 = dmc::ClientInfo(0.0, 200.0, 0.0);
      pq->update_client_info(17);

      now = dmc::get_time();

      for (int i = 0; i < 5; ++i) {
	EXPECT_EQ(0, pq->add_request(Request{}, client1, req_params));
	EXPECT_EQ(0, pq->add_request(Request{}, client2, req_params));
	now += 0.0001;
      }

      c1_count = 0;
      c2_count = 0;
      for (int i = 0; i < 6; ++i) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	auto& retn = std::get<Queue::PullReq::Retn>(pr.data);

	if (client1 == retn.client) ++c1_count;
	else if (client2 == retn.client) ++c2_count;
	else ADD_FAILURE() << "got request from neither of two clients";

	EXPECT_EQ(PhaseType::priority, retn.phase);
      }

      EXPECT_EQ(3, c1_count) <<
	"after: one-third of request should have come from first client";
      EXPECT_EQ(3, c2_count) <<
	"after: two-thirds of request should have come from second client";
    }


    TEST(dmclock_server_pull, dynamic_cli_info_f) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request,true,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1[] = {
        dmc::ClientInfo(0.0, 100.0, 0.0),
        dmc::ClientInfo(0.0, 150.0, 0.0)};
      dmc::ClientInfo info2[] = {
        dmc::ClientInfo(0.0, 200.0, 0.0),
        dmc::ClientInfo(0.0, 50.0, 0.0)};

      size_t cli_info_group = 0;

      QueueRef pq;

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &info1[cli_info_group];
	else if (client2 == c) return &info2[cli_info_group];
	else {
	  ADD_FAILURE() << "client info looked up for non-existent client";
	  return nullptr;
	}
      };

      pq = QueueRef(new Queue(client_info_f, AtLimit::Wait));

      auto now = dmc::get_time();

      auto run_test = [&](float lower_bound, float upper_bound) {
	ReqParams req_params(1,1);
	constexpr unsigned num_requests = 1000;

	for (int i = 0; i < num_requests; i += 2) {
	  EXPECT_EQ(0, pq->add_request(Request{}, client1, req_params));
	  EXPECT_EQ(0, pq->add_request(Request{}, client2, req_params));
	  now += 0.0001;
	}

	int c1_count = 0;
	int c2_count = 0;
	for (int i = 0; i < num_requests; ++i) {
	  Queue::PullReq pr = pq->pull_request();
	  EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	  // only count the specified portion of the served request
	  if (i < num_requests * lower_bound || i > num_requests * upper_bound) {
	    continue;
	  }
	  auto& retn = std::get<Queue::PullReq::Retn>(pr.data);
	  if (client1 == retn.client) {
	    ++c1_count;
	  } else if (client2 == retn.client) {
	    ++c2_count;
	  } else {
	    ADD_FAILURE() << "got request from neither of two clients";
	  }
	  EXPECT_EQ(PhaseType::priority, retn.phase);
	}

        constexpr float tolerance = 0.002;
        float prop1 = float(info1[cli_info_group].weight) / (info1[cli_info_group].weight +
                                                             info2[cli_info_group].weight);
        float prop2 = float(info2[cli_info_group].weight) / (info1[cli_info_group].weight +
                                                             info2[cli_info_group].weight);
        EXPECT_NEAR(float(c1_count) / (c1_count + c2_count), prop1, tolerance) <<
          "before: " << prop1 << " of requests should have come from first client";
        EXPECT_NEAR
          (float(c2_count) / (c1_count + c2_count), prop2, tolerance) <<
          "before: " << prop2 << " of requests should have come from second client";
      };
      cli_info_group = 0;
      // only count the first half of the served requests, so we can check
      // the prioritized ones
      run_test(0.0F /* lower bound */,
               0.5F /* upper bound */);

      std::chrono::seconds dura(1);
      std::this_thread::sleep_for(dura);

      // check the middle part of the request sequence which is less likely
      // to be impacted by previous requests served before we switch to the
      // new client info.
      cli_info_group = 1;
      run_test(1.0F/3 /* lower bound */,
               2.0F/3 /* upper bound */);
    }

    // This test shows what happens when a request can be ready (under
    // limit) but not schedulable since proportion tag is 0. We expect
    // to get some future and none responses.
    TEST(dmclock_server_pull, ready_and_under_limit) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 8;

      dmc::ClientInfo info1(1.0, 0.0, 0.0);
      dmc::ClientInfo info2(1.0, 0.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &info1;
	else if (client2 == c) return &info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existent client";
	  return nullptr;
	}
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(0, 0);

      // make sure all times are well before now
      auto start_time = dmc::get_time() - 100.0;

      // add six requests; for same client reservations spaced one apart
      for (int i = 0; i < 3; ++i) {
	EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, start_time));
	EXPECT_EQ(0, pq->add_request_time(Request{}, client2, req_params, start_time));
      }

      Queue::PullReq pr = pq->pull_request(start_time + 0.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 0.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 0.5);
      EXPECT_EQ(Queue::NextReqType::future, pr.type) <<
	"too soon for next reservation";

      pr = pq->pull_request(start_time + 1.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 1.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 1.5);
      EXPECT_EQ(Queue::NextReqType::future, pr.type) <<
	"too soon for next reservation";

      pr = pq->pull_request(start_time + 2.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 2.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 2.5);
      EXPECT_EQ(Queue::NextReqType::none, pr.type) << "no more requests left";
    }


    TEST(dmclock_server_pull, pull_none) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      dmc::ClientInfo info(1.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));

      // Request req;
      ReqParams req_params(1,1);

      auto now = dmc::get_time();

      Queue::PullReq pr = pq->pull_request(now + 100);

      EXPECT_EQ(Queue::NextReqType::none, pr.type);
    }


    TEST(dmclock_server_pull, pull_future) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      // ClientId client2 = 8;

      dmc::ClientInfo info(1.0, 0.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, now + 100));
      Queue::PullReq pr = pq->pull_request(now);

      EXPECT_EQ(Queue::NextReqType::future, pr.type);

      Time when = std::get<Time>(pr.data);
      EXPECT_EQ(now + 100, when);
    }


    TEST(dmclock_server_pull, pull_future_limit_break_weight) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      // ClientId client2 = 8;

      dmc::ClientInfo info(0.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Allow));

      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, now + 100));
      Queue::PullReq pr = pq->pull_request(now);

      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      auto& retn = std::get<Queue::PullReq::Retn>(pr.data);
      EXPECT_EQ(client1, retn.client);
    }


    TEST(dmclock_server_pull, pull_future_limit_break_reservation) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      // ClientId client2 = 8;

      dmc::ClientInfo info(1.0, 0.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Allow));

      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, now + 100));
      Queue::PullReq pr = pq->pull_request(now);

      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      auto& retn = std::get<Queue::PullReq::Retn>(pr.data);
      EXPECT_EQ(client1, retn.client);
    }


    TEST(dmclock_server_pull, pull_reject_at_limit) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId, Request, false>;
      using MyReqRef = typename Queue::RequestRef;

      ClientId client1 = 52;
      ClientId client2 = 53;

      dmc::ClientInfo info(0.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      Queue pq(client_info_f, AtLimit::Reject);

      {
        // success at 1 request per second
        EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{1}));
        EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{2}));
        EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{3}));
        // request too soon
        EXPECT_EQ(EAGAIN, pq.add_request_time({}, client1, {}, Time{3.9}));
        // previous rejected request counts against limit
        EXPECT_EQ(EAGAIN, pq.add_request_time({}, client1, {}, Time{4}));
        EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{6}));
      }
      {
        auto r1 = MyReqRef{new Request};
        ASSERT_EQ(0, pq.add_request(std::move(r1), client2, {}, Time{1}));
        EXPECT_EQ(nullptr, r1); // add_request takes r1 on success
        auto r2 = MyReqRef{new Request};
        ASSERT_EQ(EAGAIN, pq.add_request(std::move(r2), client2, {}, Time{1}));
        EXPECT_NE(nullptr, r2); // add_request does not take r2 on failure
      }
    }


    TEST(dmclock_server_pull, pull_reject_threshold) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId, Request, false>;

      ClientId client1 = 52;

      dmc::ClientInfo info(0.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      // allow up to 3 seconds worth of limit before rejecting
      Queue pq(client_info_f, RejectThreshold{3.0});

      EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{1})); // at limit=1
      EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{1})); // 1 over
      EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{1})); // 2 over
      EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{1})); // 3 over
      EXPECT_EQ(EAGAIN, pq.add_request_time({}, client1, {}, Time{1})); // reject
      EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{3})); // 3 over
    }


    TEST(dmclock_server_pull, pull_wait_at_limit) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 8;

      // Create client1 with high limit.
      // Create client2 with low limit and with lower weight than client1
      dmc::ClientInfo info1(1.0, 2.0, 100.0);
      dmc::ClientInfo info2(1.0, 1.0, 2.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) {
          return &info1;
        } else if (client2 == c) {
          return &info2;
        } else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(1,1);

      // make sure all times are before now
      auto add_time = dmc::get_time() - 1.0;
      auto old_time = add_time;

      for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, add_time));
        EXPECT_EQ(0, pq->add_request_time(Request{}, client2, req_params, add_time));
        add_time += 0.01;
      }

      EXPECT_EQ(2u, pq->client_count());
      EXPECT_EQ(100u, pq->request_count());
      int c1_count = 0;
      int c2_count = 0;

      // Pull couple of requests, should come from reservation queue.
      // One request each from client1 and client2 should be pulled.
      for (int i = 0; i < 2; ++i) {
        Queue::PullReq pr = pq->pull_request();
        EXPECT_EQ(Queue::NextReqType::returning, pr.type);
        auto& retn = std::get<Queue::PullReq::Retn>(pr.data);

        if (client1 == retn.client) {
          ++c1_count;
        } else if (client2 == retn.client) {
          ++c2_count;
        } else {
          ADD_FAILURE() << "got request from neither of two clients";
        }

        EXPECT_EQ(PhaseType::reservation, retn.phase);
      }

      EXPECT_EQ(1, c1_count) <<
        "one request should have come from first client";
      EXPECT_EQ(1, c2_count) <<
        "one request should have come from second client";

      EXPECT_EQ(2u, pq->client_count());
      EXPECT_EQ(98u, pq->request_count());

      // Pull more requests out.
      // All remaining requests from client1 should be pulled.
      // Only 1 request from client2 should be pulled.
      for (int i = 0; i < 50; ++i) {
        Queue::PullReq pr = pq->pull_request();
        EXPECT_EQ(Queue::NextReqType::returning, pr.type);
        auto& retn = std::get<Queue::PullReq::Retn>(pr.data);

        if (client1 == retn.client) {
          ++c1_count;
        } else if (client2 == retn.client) {
          ++c2_count;
        } else {
          ADD_FAILURE() << "got request from neither of two clients";
        }

        EXPECT_EQ(PhaseType::priority, retn.phase);
      }

      EXPECT_EQ(2u, pq->client_count());
      EXPECT_EQ(48u, pq->request_count());

      // Pulling the remaining client2 requests shouldn't succeed.
      Queue::PullReq pr = pq->pull_request();
      EXPECT_EQ(Queue::NextReqType::future, pr.type);
      Time when_ready = pr.getTime();
      EXPECT_EQ(old_time + 2.0, when_ready);

      EXPECT_EQ(50, c1_count) <<
        "half of the total requests should have come from first client";
      EXPECT_EQ(2, c2_count) <<
        "only two requests should have come from second client";

      // Trying to pull a request after restoring the limit should succeed.
      pr = pq->pull_request(old_time + 2.0);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);
      auto& retn = std::get<Queue::PullReq::Retn>(pr.data);
      EXPECT_EQ(retn.client, client2);
      EXPECT_EQ(47u, pq->request_count());
    } // dmclock_server_pull.pull_wait_at_limit

  } // namespace dmclock
} // namespace crimson
