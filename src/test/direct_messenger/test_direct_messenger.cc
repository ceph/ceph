// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <condition_variable>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

#include "global/global_init.h"
#include "common/ceph_argparse.h"

#include "DirectMessenger.h"
#include "msg/FastStrategy.h"
#include "msg/QueueStrategy.h"
#include "messages/MPing.h"


/// mock dispatcher that calls the given callback
class MockDispatcher : public Dispatcher {
  std::function<void(Message*)> callback;
 public:
  MockDispatcher(CephContext *cct, std::function<void(Message*)> callback)
    : Dispatcher(cct), callback(std::move(callback)) {}
  bool ms_handle_reset(Connection *con) override { return false; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; }
  bool ms_dispatch(Message *m) override {
    callback(m);
    m->put();
    return true;
  }
};

/// test synchronous dispatch of messenger and connection interfaces
TEST(DirectMessenger, SyncDispatch)
{
  auto cct = g_ceph_context;

  // use FastStrategy for synchronous dispatch
  DirectMessenger client(cct, entity_name_t::CLIENT(1),
                         "client", 0, new FastStrategy());
  DirectMessenger server(cct, entity_name_t::CLIENT(2),
                         "server", 0, new FastStrategy());

  ASSERT_EQ(0, client.set_direct_peer(&server));
  ASSERT_EQ(0, server.set_direct_peer(&client));

  bool got_request = false;
  bool got_reply = false;

  MockDispatcher client_dispatcher(cct, [&] (Message *m) {
    got_reply = true;
  });
  client.add_dispatcher_head(&client_dispatcher);

  MockDispatcher server_dispatcher(cct, [&] (Message *m) {
    got_request = true;
    ASSERT_EQ(0, m->get_connection()->send_message(new MPing()));
  });
  server.add_dispatcher_head(&server_dispatcher);

  ASSERT_EQ(0, client.start());
  ASSERT_EQ(0, server.start());

  // test DirectMessenger::send_message()
  ASSERT_EQ(0, client.send_message(new MPing(), server.get_myinst()));
  ASSERT_TRUE(got_request);
  ASSERT_TRUE(got_reply);

  // test DirectConnection::send_message()
  {
    got_request = false;
    got_reply = false;
    auto conn = client.get_connection(server.get_myinst());
    ASSERT_EQ(0, conn->send_message(new MPing()));
    ASSERT_TRUE(got_request);
    ASSERT_TRUE(got_reply);
  }

  // test DirectMessenger::send_message() with loopback address
  got_request = false;
  got_reply = false;
  ASSERT_EQ(0, client.send_message(new MPing(), client.get_myinst()));
  ASSERT_FALSE(got_request); // server should never see this
  ASSERT_TRUE(got_reply);

  // test DirectConnection::send_message() with loopback address
  {
    got_request = false;
    got_reply = false;
    auto conn = client.get_connection(client.get_myinst());
    ASSERT_EQ(0, conn->send_message(new MPing()));
    ASSERT_FALSE(got_request); // server should never see this
    ASSERT_TRUE(got_reply);
  }

  // test DirectConnection::send_message() with loopback connection
  {
    got_request = false;
    got_reply = false;
    auto conn = client.get_loopback_connection();
    ASSERT_EQ(0, conn->send_message(new MPing()));
    ASSERT_FALSE(got_request); // server should never see this
    ASSERT_TRUE(got_reply);
  }

  ASSERT_EQ(0, client.shutdown());
  client.wait();

  ASSERT_EQ(0, server.shutdown());
  server.wait();
}

/// test asynchronous dispatch of messenger and connection interfaces
TEST(DirectMessenger, AsyncDispatch)
{
  auto cct = g_ceph_context;

  // use QueueStrategy for async replies
  DirectMessenger client(cct, entity_name_t::CLIENT(1),
                         "client", 0, new QueueStrategy(1));
  DirectMessenger server(cct, entity_name_t::CLIENT(2),
                         "server", 0, new FastStrategy());

  ASSERT_EQ(0, client.set_direct_peer(&server));
  ASSERT_EQ(0, server.set_direct_peer(&client));

  // condition variable to wait on ping reply
  std::mutex mutex;
  std::condition_variable cond;
  bool done = false;

  auto wait_for_reply = [&] {
    std::unique_lock<std::mutex> lock(mutex);
    while (!done) {
      cond.wait(lock);
    }
    done = false; // clear for reuse
  };

  // client dispatcher signals the condition variable on reply
  MockDispatcher client_dispatcher(cct, [&] (Message *m) {
    std::lock_guard<std::mutex> lock(mutex);
    done = true;
    cond.notify_one();
  });
  client.add_dispatcher_head(&client_dispatcher);

  MockDispatcher server_dispatcher(cct, [&] (Message *m) {
    // hold the lock over the call to send_message() to prove that the client's
    // dispatch is asynchronous. if it isn't, it will deadlock
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ(0, m->get_connection()->send_message(new MPing()));
  });
  server.add_dispatcher_head(&server_dispatcher);

  ASSERT_EQ(0, client.start());
  ASSERT_EQ(0, server.start());

  // test DirectMessenger::send_message()
  ASSERT_EQ(0, client.send_message(new MPing(), server.get_myinst()));
  wait_for_reply();

  // test DirectConnection::send_message()
  {
    auto conn = client.get_connection(server.get_myinst());
    ASSERT_EQ(0, conn->send_message(new MPing()));
  }
  wait_for_reply();

  // test DirectMessenger::send_message() with loopback address
  {
    // hold the lock to test that loopback dispatch is asynchronous
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ(0, client.send_message(new MPing(), client.get_myinst()));
  }
  wait_for_reply();

  // test DirectConnection::send_message() with loopback address
  {
    auto conn = client.get_connection(client.get_myinst());
    // hold the lock to test that loopback dispatch is asynchronous
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ(0, conn->send_message(new MPing()));
  }
  wait_for_reply();

  // test DirectConnection::send_message() with loopback connection
  {
    auto conn = client.get_loopback_connection();
    // hold the lock to test that loopback dispatch is asynchronous
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ(0, conn->send_message(new MPing()));
  }
  wait_for_reply();

  ASSERT_EQ(0, client.shutdown());
  client.wait();

  ASSERT_EQ(0, server.shutdown());
  server.wait();
}

/// test that wait() blocks until shutdown()
TEST(DirectMessenger, WaitShutdown)
{
  auto cct = g_ceph_context;

  // test wait() with both Queue- and FastStrategy
  DirectMessenger client(cct, entity_name_t::CLIENT(1),
                         "client", 0, new QueueStrategy(1));
  DirectMessenger server(cct, entity_name_t::CLIENT(2),
                         "server", 0, new FastStrategy());

  ASSERT_EQ(0, client.set_direct_peer(&server));
  ASSERT_EQ(0, server.set_direct_peer(&client));

  ASSERT_EQ(0, client.start());
  ASSERT_EQ(0, server.start());

  std::atomic<bool> client_waiting{false};
  std::atomic<bool> server_waiting{false};

  // spawn threads to wait() on each of the messengers
  std::thread client_thread([&] {
    client_waiting = true;
    client.wait();
    client_waiting = false;
  });
  std::thread server_thread([&] {
    server_waiting = true;
    server.wait();
    server_waiting = false;
  });

  // give them time to start
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  ASSERT_TRUE(client_waiting);
  ASSERT_TRUE(server_waiting);

  // call shutdown to unblock the waiting threads
  ASSERT_EQ(0, client.shutdown());
  ASSERT_EQ(0, server.shutdown());

  client_thread.join();
  server_thread.join();

  ASSERT_FALSE(client_waiting);
  ASSERT_FALSE(server_waiting);
}

/// test connection and messenger interfaces after mark_down()
TEST(DirectMessenger, MarkDown)
{
  auto cct = g_ceph_context;

  DirectMessenger client(cct, entity_name_t::CLIENT(1),
                         "client", 0, new FastStrategy());
  DirectMessenger server(cct, entity_name_t::CLIENT(2),
                         "server", 0, new FastStrategy());

  ASSERT_EQ(0, client.set_direct_peer(&server));
  ASSERT_EQ(0, server.set_direct_peer(&client));

  ASSERT_EQ(0, client.start());
  ASSERT_EQ(0, server.start());

  auto client_to_server = client.get_connection(server.get_myinst());
  auto server_to_client = server.get_connection(client.get_myinst());

  ASSERT_TRUE(client_to_server->is_connected());
  ASSERT_TRUE(server_to_client->is_connected());

  // mark_down() breaks the connection on both sides
  client_to_server->mark_down();

  ASSERT_FALSE(client_to_server->is_connected());
  ASSERT_EQ(-ENOTCONN, client_to_server->send_message(new MPing()));
  ASSERT_EQ(-ENOTCONN, client.send_message(new MPing(), server.get_myinst()));

  ASSERT_FALSE(server_to_client->is_connected());
  ASSERT_EQ(-ENOTCONN, server_to_client->send_message(new MPing()));
  ASSERT_EQ(-ENOTCONN, server.send_message(new MPing(), client.get_myinst()));

  ASSERT_EQ(0, client.shutdown());
  client.wait();

  ASSERT_EQ(0, server.shutdown());
  server.wait();
}

/// test connection and messenger interfaces after shutdown()
TEST(DirectMessenger, SendShutdown)
{
  auto cct = g_ceph_context;

  // put client on the heap so we can free it early
  std::unique_ptr<DirectMessenger> client{
    new DirectMessenger(cct, entity_name_t::CLIENT(1),
                        "client", 0, new FastStrategy())};
  DirectMessenger server(cct, entity_name_t::CLIENT(2),
                         "server", 0, new FastStrategy());

  ASSERT_EQ(0, client->set_direct_peer(&server));
  ASSERT_EQ(0, server.set_direct_peer(client.get()));

  ASSERT_EQ(0, client->start());
  ASSERT_EQ(0, server.start());

  const auto client_inst = client->get_myinst();
  const auto server_inst = server.get_myinst();

  auto client_to_server = client->get_connection(server_inst);
  auto server_to_client = server.get_connection(client_inst);

  ASSERT_TRUE(client_to_server->is_connected());
  ASSERT_TRUE(server_to_client->is_connected());

  // shut down the client to break connections
  ASSERT_EQ(0, client->shutdown());
  client->wait();

  ASSERT_FALSE(client_to_server->is_connected());
  ASSERT_EQ(-ENOTCONN, client_to_server->send_message(new MPing()));
  ASSERT_EQ(-ENOTCONN, client->send_message(new MPing(), server_inst));

  // free the client connection/messenger to test that calls to the server no
  // longer try to dereference them
  client_to_server.reset();
  client.reset();

  ASSERT_FALSE(server_to_client->is_connected());
  ASSERT_EQ(-ENOTCONN, server_to_client->send_message(new MPing()));
  ASSERT_EQ(-ENOTCONN, server.send_message(new MPing(), client_inst));

  ASSERT_EQ(0, server.shutdown());
  server.wait();
}

/// test connection and messenger interfaces after bind()
TEST(DirectMessenger, Bind)
{
  auto cct = g_ceph_context;

  DirectMessenger client(cct, entity_name_t::CLIENT(1),
                         "client", 0, new FastStrategy());
  DirectMessenger server(cct, entity_name_t::CLIENT(2),
                         "server", 0, new FastStrategy());

  entity_addr_t client_addr;
  client_addr.set_family(AF_INET);
  client_addr.set_port(1);

  // client bind succeeds before set_direct_peer()
  ASSERT_EQ(0, client.bind(client_addr));

  ASSERT_EQ(0, client.set_direct_peer(&server));
  ASSERT_EQ(0, server.set_direct_peer(&client));

  // server bind fails after set_direct_peer()
  entity_addr_t empty_addr;
  ASSERT_EQ(-EINVAL, server.bind(empty_addr));

  ASSERT_EQ(0, client.start());
  ASSERT_EQ(0, server.start());

  auto client_to_server = client.get_connection(server.get_myinst());
  auto server_to_client = server.get_connection(client.get_myinst());

  ASSERT_TRUE(client_to_server->is_connected());
  ASSERT_TRUE(server_to_client->is_connected());

  // no address in connection to server
  ASSERT_EQ(empty_addr, client_to_server->get_peer_addr());
  // bind address is reflected in connection to client
  ASSERT_EQ(client_addr, server_to_client->get_peer_addr());

  // mark_down() with bind address breaks the connection
  server.mark_down(client_addr);

  ASSERT_FALSE(client_to_server->is_connected());
  ASSERT_FALSE(server_to_client->is_connected());

  ASSERT_EQ(0, client.shutdown());
  client.wait();

  ASSERT_EQ(0, server.shutdown());
  server.wait();
}

/// test connection and messenger interfaces before calls to set_direct_peer()
TEST(DirectMessenger, StartWithoutPeer)
{
  auto cct = g_ceph_context;

  DirectMessenger client(cct, entity_name_t::CLIENT(1),
                         "client", 0, new FastStrategy());
  DirectMessenger server(cct, entity_name_t::CLIENT(2),
                         "server", 0, new FastStrategy());

  // can't start until set_direct_peer()
  ASSERT_EQ(-EINVAL, client.start());
  ASSERT_EQ(-EINVAL, server.start());

  ASSERT_EQ(0, client.set_direct_peer(&server));

  // only client can start
  ASSERT_EQ(0, client.start());
  ASSERT_EQ(-EINVAL, server.start());

  // client has a connection but can't send
  auto conn = client.get_connection(server.get_myinst());
  ASSERT_NE(nullptr, conn);
  ASSERT_FALSE(conn->is_connected());
  ASSERT_EQ(-ENOTCONN, conn->send_message(new MPing()));
  ASSERT_EQ(-ENOTCONN, client.send_message(new MPing(), server.get_myinst()));

  ASSERT_EQ(0, client.shutdown());
  client.wait();
}

int main(int argc, char **argv)
{
  // command-line arguments
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_ANY,
                         CODE_ENVIRONMENT_DAEMON,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(cct.get());

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
