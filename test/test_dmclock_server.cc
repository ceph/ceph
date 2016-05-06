// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include <memory>
#include <chrono>
#include <iostream>


#include "dmclock_server.h"
#include "dmclock_util.h"
#include "gtest/gtest.h"


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
      using Queue = dmc::PriorityQueue<ClientId,Request,dmc::QMechanism::pull>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 18;

      double reservation = 0.0;
      double weight = 0.0;

      dmc::ClientInfo ci1(reservation, weight, 0.0);
      dmc::ClientInfo ci2(reservation, weight, 1.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	if (client1 == c) return ci1;
	else if (client2 == c) return ci2;
	else {
	  ADD_FAILURE() << "got request from neither of two clients";
	  return ci1; // must return
	}
      };

      QueueRef pq(new Queue(client_info_f, false));
      Request req;
      ReqParams req_params(1,1);

      EXPECT_DEATH_IF_SUPPORTED(pq->add_request(req, client1, req_params),
				"Assertion.*reservation.*max_tag.*"
				"proportion.*max_tag") <<
	"we should fail if a client tries to generate a reservation tag "
	"where reservation and proportion are both 0";


      EXPECT_DEATH_IF_SUPPORTED(pq->add_request(req, client2, req_params),
				"Assertion.*reservation.*max_tag.*"
				"proportion.*max_tag") <<
	"we should fail if a client tries to generate a reservation tag "
	"where reservation and proportion are both 0";
    }

    
    TEST(dmclock_server, client_idle_erase) {
      using ClientId = int;
      using Queue = dmc::PriorityQueue<ClientId,Request,dmc::QMechanism::push>;
      int client = 17;
      double reservation = 100.0;

      dmc::ClientInfo ci(reservation, 1.0, 0.0);
      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo { return ci; };
      auto server_ready_f = [] () -> bool { return true; };
      auto submit_req_f = [] (const ClientId& c,
			      std::unique_ptr<Request> req,
			      dmc::PhaseType phase) {
	// empty; do nothing
      };

      Queue pq(client_info_f,
	       server_ready_f,
	       submit_req_f,
	       std::chrono::seconds(3),
	       std::chrono::seconds(5),
	       std::chrono::seconds(2),
	       false);

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
	  EXPECT_EQ(pq.client_map.size(), 0) << "client map initially has size 0";
	});

      Request req;
      dmc::ReqParams req_params(1, 1);
      pq.add_request(req, client, req_params, dmc::get_time());

      std::this_thread::sleep_for(std::chrono::seconds(1));

      lock_pq([&] () {
	  EXPECT_EQ(1, pq.client_map.size()) << "client map has 1 after 1 client";
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
	  EXPECT_EQ(0, pq.client_map.size()) <<
	    "client map loses its entry after erase age";
	});
    } // TEST


#if 0
    TEST(dmclock_server, reservation_timing) {
      using ClientId = int;
      using Queue = std::unique_ptr<dmc::PriorityQueue<ClientId,Request>>;
      using std::chrono::steady_clock;

      int client = 17;

      std::vector<dmc::Time> times;
      std::mutex times_mtx;
      using Guard = std::lock_guard<decltype(times_mtx)>;

      // reservation every second
      dmc::ClientInfo ci(1.0, 0.0, 0.0);
      Queue pq;

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo { return ci; };
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

      pq = Queue(new dmc::PriorityQueue<ClientId,Request>(client_info_f,
							  server_ready_f,
							  submit_req_f,
							  false));

      Request req;
      ReqParams<ClientId> req_params(client, 1, 1);

      for (int i = 0; i < 5; ++i) {
	pq->add_request(req, req_params, dmc::get_time());
      }

      {
	Guard g(times_mtx);
	std::this_thread::sleep_for(std::chrono::milliseconds(5500));
	EXPECT_EQ(5, times.size()) <<
	  "after 5.5 seconds, we should have 5 requests times at 1 second apart";
      }
    } // TEST
#endif


    TEST(dmclock_server_pull, pull_weight) {
      using ClientId = int;
      using Queue = dmc::PriorityQueue<ClientId,Request,dmc::QMechanism::pull>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);
      dmc::ClientInfo info2(0.0, 2.0, 0.0);

      QueueRef pq;

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	if (client1 == c) return info1;
	else if (client2 == c) return info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existant client";
	  return info1;
	}
      };

      pq = QueueRef(new Queue(client_info_f, false));

      Request req;
      ReqParams req_params(1,1);

      auto now = dmc::get_time();

      for (int i = 0; i < 5; ++i) {
	pq->add_request(req, client1, req_params);
	pq->add_request(req, client2, req_params);
	now += 0.0001;
      }

      int c1_count = 0;
      int c2_count = 0;
      for (int i = 0; i < 6; ++i) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);

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
      using Queue = dmc::PriorityQueue<ClientId,Request,dmc::QMechanism::pull>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 8;

      dmc::ClientInfo info1(2.0, 0.0, 0.0);
      dmc::ClientInfo info2(1.0, 0.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	if (client1 == c) return info1;
	else if (client2 == c) return info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existant client";
	  return info1;
	}
      };

      QueueRef pq(new Queue(client_info_f, false));

      Request req;
      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto old_time = dmc::get_time() - 100.0;

      for (int i = 0; i < 5; ++i) {
	pq->add_request(req, client1, req_params, old_time);
	pq->add_request(req, client2, req_params, old_time);
	old_time += 0.001;
      }

      int c1_count = 0;
      int c2_count = 0;

      for (int i = 0; i < 6; ++i) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);

	if (client1 == retn.client) ++c1_count;
	else if (client2 == retn.client) ++c2_count;
	else ADD_FAILURE() << "got request from neither of two clients";

	EXPECT_EQ(PhaseType::reservation, retn.phase);
      }

      EXPECT_EQ(4, c1_count) <<
	"two-thirds of request should have come from first client";
      EXPECT_EQ(2, c2_count) <<
	"one-third of request should have come from second client";
    }


    TEST(dmclock_server_pull, pull_none) {
      using ClientId = int;
      using Queue = dmc::PriorityQueue<ClientId,Request,dmc::QMechanism::pull>;
      using QueueRef = std::unique_ptr<Queue>;

      dmc::ClientInfo info(1.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info;
      };

      QueueRef pq(new Queue(client_info_f, false));

      Request req;
      ReqParams req_params(1,1);

      auto now = dmc::get_time();

      Queue::PullReq pr = pq->pull_request(now + 100);

      EXPECT_EQ(Queue::NextReqType::none, pr.type);
    }


    TEST(dmclock_server_pull, pull_future) {
      using ClientId = int;
      using Queue = dmc::PriorityQueue<ClientId,Request,dmc::QMechanism::pull>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 8;

      dmc::ClientInfo info(1.0, 0.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info;
      };

      QueueRef pq(new Queue(client_info_f, false));

      Request req;
      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      pq->add_request(req, client1, req_params, now + 100);
      Queue::PullReq pr = pq->pull_request(now);

      EXPECT_EQ(Queue::NextReqType::future, pr.type);

      Time when = boost::get<Time>(pr.data);
      EXPECT_EQ(now + 100, when);
    }


    TEST(dmclock_server_pull, pull_future_limit_break_weight) {
      using ClientId = int;
      using Queue = dmc::PriorityQueue<ClientId,Request,dmc::QMechanism::pull>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 8;

      dmc::ClientInfo info(0.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info;
      };

      QueueRef pq(new Queue(client_info_f, true));

      Request req;
      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      pq->add_request(req, client1, req_params, now + 100);
      Queue::PullReq pr = pq->pull_request(now);

      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
      EXPECT_EQ(client1, retn.client);
    }

    TEST(dmclock_server_pull, pull_future_limit_break_reservation) {
      using ClientId = int;
      using Queue = dmc::PriorityQueue<ClientId,Request,dmc::QMechanism::pull>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 8;

      dmc::ClientInfo info(1.0, 0.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info;
      };

      QueueRef pq(new Queue(client_info_f, true));

      Request req;
      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      pq->add_request(req, client1, req_params, now + 100);
      Queue::PullReq pr = pq->pull_request(now);

      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
      EXPECT_EQ(client1, retn.client);
    }
  } // namespace dmclock
} // namespace crimson
