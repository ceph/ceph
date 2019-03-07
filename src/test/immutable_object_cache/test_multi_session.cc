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

class TestMultiSession : public ::testing::Test {
public:
  std::string m_local_path;
  CacheServer* m_cache_server;
  std::thread* m_cache_server_thread;
  std::vector<CacheClient*> m_cache_client_vec;
  WaitEvent m_wait_event;
  std::atomic<uint64_t> m_send_request_index;
  std::atomic<uint64_t> m_recv_ack_index;
  uint64_t m_session_num = 110;

  TestMultiSession() : m_local_path("/tmp/ceph_test_multisession_socket"),
                       m_cache_server_thread(nullptr),  m_send_request_index(0),
                       m_recv_ack_index(0) {
    m_cache_client_vec.resize(m_session_num + 1, nullptr);
  }

  ~TestMultiSession() {}

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  void SetUp() override {
    std::remove(m_local_path.c_str());
    m_cache_server = new CacheServer(g_ceph_context, m_local_path,
      [this](CacheSession* session_id, ObjectCacheRequest* req){
        server_handle_request(session_id, req);
    });
    ASSERT_TRUE(m_cache_server != nullptr);

    m_cache_server_thread = new std::thread(([this]() {
        m_wait_event.signal();
        m_cache_server->run();
    }));

    // waiting for thread running.
    m_wait_event.wait();

    // waiting for io_service run.
    usleep(2);
  }

  void TearDown() override {
    for (uint64_t i = 0; i < m_session_num; i++) {
      if (m_cache_client_vec[i] != nullptr) {
        m_cache_client_vec[i]->close();
        delete m_cache_client_vec[i];
      }
    }
    m_cache_server->stop();
    if (m_cache_server_thread->joinable()) {
      m_cache_server_thread->join();
    }
    delete m_cache_server;
    delete m_cache_server_thread;

    std::remove(m_local_path.c_str());
  }

  CacheClient* create_session(uint64_t random_index) {
     CacheClient* cache_client = new CacheClient(m_local_path, g_ceph_context);
     cache_client->run();
     while (true) {
       if (0 == cache_client->connect()) {
         break;
       }
     }
    m_cache_client_vec[random_index] = cache_client;
    return cache_client;
  }

  void server_handle_request(CacheSession* session_id, ObjectCacheRequest* req) {

    switch (req->get_request_type()) {
      case RBDSC_REGISTER: {
        ObjectCacheRequest* reply = new ObjectCacheRegReplyData(RBDSC_REGISTER_REPLY,
                                                                req->seq);
        session_id->send(reply);
        break;
      }
      case RBDSC_READ: {
        ObjectCacheRequest* reply = new ObjectCacheReadReplyData(RBDSC_READ_REPLY,
                                                                req->seq);
        session_id->send(reply);
        break;
      }
    }
  }

  void test_register_client(uint64_t random_index) {
    ASSERT_TRUE(m_cache_client_vec[random_index] == nullptr);

    auto ctx = new FunctionContext([](bool ret){
       ASSERT_TRUE(ret);
    });
    auto session = create_session(random_index);
    session->register_client(ctx);

    ASSERT_TRUE(m_cache_client_vec[random_index] != nullptr);
    ASSERT_TRUE(session->is_session_work());
  }

  void test_lookup_object(std::string pool_nspace, uint64_t index,
                          uint64_t request_num, bool is_last) {

    for (uint64_t i = 0; i < request_num; i++) {
      auto ctx = new LambdaGenContext<std::function<void(ObjectCacheRequest*)>,
        ObjectCacheRequest*>([this](ObjectCacheRequest* ack) {
        m_recv_ack_index++;
      });
      m_send_request_index++;
      // here just for concurrently testing register + lookup, so fix object id.
      m_cache_client_vec[index]->lookup_object(pool_nspace, 1, 2, "1234", ctx);
    }

    if (is_last) {
      while(m_send_request_index != m_recv_ack_index) {
        usleep(1);
      }
      m_wait_event.signal();
    }
  }
};

// test concurrent : multi-session + register_client + lookup_request
TEST_F(TestMultiSession, test_multi_session) {

  uint64_t test_times = 1000;
  uint64_t test_session_num = 100;

  for (uint64_t i = 0; i <= test_times; i++) {
    uint64_t random_index = random() % test_session_num;
    if (m_cache_client_vec[random_index] == nullptr) {
      test_register_client(random_index);
    } else {
      test_lookup_object(string("test_nspace") + std::to_string(random_index),
                         random_index, 4, i == test_times ? true : false);
    }
  }

  // make sure all ack will be received.
  m_wait_event.wait();

  ASSERT_TRUE(m_send_request_index == m_recv_ack_index);
}
