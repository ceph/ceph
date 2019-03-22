// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <unistd.h>

#include "gtest/gtest.h"
#include "include/Context.h"
#include "global/global_init.h"
#include "global/global_context.h"

#include "test/immutable_object_cache/test_common.h"
#include "tools/immutable_object_cache/CacheClient.h"
#include "tools/immutable_object_cache/CacheServer.h"

using namespace ceph::immutable_obj_cache;

class TestCommunication :public ::testing::Test {
public:
  CacheServer* m_cache_server;
  std::thread* srv_thd;
  CacheClient* m_cache_client;
  std::string m_local_path;
  pthread_mutex_t m_mutex;
  pthread_cond_t m_cond;
  std::atomic<uint64_t> m_send_request_index;
  std::atomic<uint64_t> m_recv_ack_index;
  WaitEvent m_wait_event;
  unordered_set<std::string> m_hit_entry_set;

  TestCommunication()
    : m_cache_server(nullptr), m_cache_client(nullptr),
      m_local_path("/tmp/ceph_test_domain_socket"),
      m_send_request_index(0), m_recv_ack_index(0)
    {}

  ~TestCommunication() {}

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  void SetUp() override {
    std::remove(m_local_path.c_str());
    m_cache_server = new CacheServer(g_ceph_context, m_local_path,
      [this](CacheSession* sid, ObjectCacheRequest* req){
        handle_request(sid, req);
    });
    ASSERT_TRUE(m_cache_server != nullptr);
    srv_thd = new std::thread([this]() {m_cache_server->run();});

    m_cache_client = new CacheClient(m_local_path, g_ceph_context);
    ASSERT_TRUE(m_cache_client != nullptr);
    m_cache_client->run();

    while (true) {
      if (0 == m_cache_client->connect()) {
        break;
      }
    }

    auto ctx = new FunctionContext([this](bool reg) {
      ASSERT_TRUE(reg);
    });
    m_cache_client->register_client(ctx);
    ASSERT_TRUE(m_cache_client->is_session_work());
  }

  void TearDown() override {

    delete m_cache_client;
    m_cache_server->stop();
    if (srv_thd->joinable()) {
      srv_thd->join();
    }
    delete m_cache_server;
    std::remove(m_local_path.c_str());
    delete srv_thd;
  }

  void handle_request(CacheSession* session_id, ObjectCacheRequest* req) {

    switch (req->get_request_type()) {
      case RBDSC_REGISTER: {
        ObjectCacheRequest* reply = new ObjectCacheRegReplyData(RBDSC_REGISTER_REPLY, req->seq);
        session_id->send(reply);
        break;
      }
      case RBDSC_READ: {
        ObjectCacheReadData* read_req = (ObjectCacheReadData*)req;
        ObjectCacheRequest* reply = nullptr;
        if (m_hit_entry_set.find(read_req->oid) == m_hit_entry_set.end()) {
          reply = new ObjectCacheReadRadosData(RBDSC_READ_RADOS, req->seq);
        } else {
          reply = new ObjectCacheReadReplyData(RBDSC_READ_REPLY, req->seq, "/fakepath");
        }
        session_id->send(reply);
        break;
      }
    }
  }

  // times: message number
  // queue_depth : imitate message queue depth
  // thinking : imitate handing message time
  void startup_pingpong_testing(uint64_t times, uint64_t queue_depth, int thinking) {
    m_send_request_index.store(0);
    m_recv_ack_index.store(0);
    for (uint64_t index = 0; index < times; index++) {
      auto ctx = new LambdaGenContext<std::function<void(ObjectCacheRequest*)>,
       ObjectCacheRequest*>([this, thinking, times](ObjectCacheRequest* ack){
         if (thinking != 0) {
           usleep(thinking); // handling message
         }
         m_recv_ack_index++;
         if (m_recv_ack_index == times) {
           m_wait_event.signal();
         }
      });

      // simple queue depth
      while (m_send_request_index - m_recv_ack_index > queue_depth) {
        usleep(1);
      }

      m_cache_client->lookup_object("pool_nspace", 1, 2, "object_name", ctx);
      m_send_request_index++;
    }
    m_wait_event.wait();
  }

  bool startup_lookupobject_testing(std::string pool_nspace, std::string object_id) {
    bool hit;
    auto ctx = new LambdaGenContext<std::function<void(ObjectCacheRequest*)>,
        ObjectCacheRequest*>([this, &hit](ObjectCacheRequest* ack){
       hit = ack->type == RBDSC_READ_REPLY;
       m_wait_event.signal();
    });
    m_cache_client->lookup_object(pool_nspace, 1, 2, object_id, ctx);
    m_wait_event.wait();
    return hit;
  }

  void set_hit_entry_in_fake_lru(std::string cache_file_name) {
    if (m_hit_entry_set.find(cache_file_name) == m_hit_entry_set.end()) {
      m_hit_entry_set.insert(cache_file_name);
    }
  }
};

TEST_F(TestCommunication, test_pingpong) {

  startup_pingpong_testing(64, 16, 0);
  ASSERT_TRUE(m_send_request_index == m_recv_ack_index);
  startup_pingpong_testing(200, 128, 0);
  ASSERT_TRUE(m_send_request_index == m_recv_ack_index);
}

TEST_F(TestCommunication, test_lookup_object) {

  m_hit_entry_set.clear();

  srand(time(0));
  uint64_t random_hit = random();

  for (uint64_t i = 50; i < 100; i++) {
    if ((random_hit % i) == 0) {
      set_hit_entry_in_fake_lru(std::to_string(i));
    }
  }
  for (uint64_t i = 50; i < 100; i++) {
    if ((random_hit % i) != 0) {
      ASSERT_FALSE(startup_lookupobject_testing("test_nspace", std::to_string(i)));
    } else {
      ASSERT_TRUE(startup_lookupobject_testing("test_nspace", std::to_string(i)));
    }
  }
}
