// Targeted perf test: runs ONE messenger type, ONE test mode
// Usage: ./bin/ceph_test_msgr_v3_perf <v2|v3> <throughput|pingpong> [msgs] [threads]
//
// Designed to be wrapped with `perf stat` for per-run hardware counters.

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "messages/MPing.h"
#include "msg/Messenger.h"
#include "msg/Dispatcher.h"
#include "auth/DummyAuth.h"

#define dout_subsys ceph_subsys_ms

class BenchDispatcher : public Dispatcher {
public:
  std::atomic<int64_t> recv_count{0};
  std::atomic<bool> got_accept{false};
  bool do_echo;

  explicit BenchDispatcher(bool echo)
    : Dispatcher(g_ceph_context), do_echo(echo) {}

  bool ms_can_fast_dispatch_any() const override { return true; }
  bool ms_can_fast_dispatch(const Message *m) const override {
    return m->get_type() == CEPH_MSG_PING;
  }
  void ms_fast_dispatch(Message *m) override {
    recv_count.fetch_add(1, std::memory_order_relaxed);
    if (do_echo) m->get_connection()->send_message(new MPing());
    m->put();
  }
  bool ms_dispatch(Message *m) override {
    recv_count.fetch_add(1, std::memory_order_relaxed);
    if (do_echo) m->get_connection()->send_message(new MPing());
    m->put();
    return true;
  }
  void ms_handle_fast_connect(Connection *con) override {}
  void ms_handle_fast_accept(Connection *con) override { got_accept = true; }
  bool ms_handle_reset(Connection *con) override { return true; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; }
  bool ms_handle_fast_authentication(Connection *con) override { return true; }
};

int main(int argc, const char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_MON_CONFIG);
  g_ceph_context->_conf.set_val("auth_cluster_required", "none");
  g_ceph_context->_conf.set_val("auth_service_required", "none");
  g_ceph_context->_conf.set_val("auth_client_required", "none");
  g_ceph_context->_conf.set_val("ms_die_on_old_message", "false");
  g_ceph_context->_conf.set_val("ms_die_on_skipped_message", "false");
  g_ceph_context->_conf.set_val("enable_experimental_unrecoverable_data_corrupting_features", "*");
  g_ceph_context->_conf.set_val("ms_async_op_threads", "3");
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.apply_changes(nullptr);

  if (args.size() < 2) {
    std::cerr << "Usage: " << argv[0] << " <v2|v3> <throughput|pingpong> [msgs] [threads]" << std::endl;
    return 1;
  }

  std::string ms_type = (std::string(args[0]) == "v2") ? "async+posix" : "v3+posix";
  std::string mode = args[1];
  int num_msgs = (args.size() > 2) ? atoi(args[2]) : 200000;
  int num_threads = (args.size() > 3) ? atoi(args[3]) : 4;
  bool is_pingpong = (mode == "pingpong");

  DummyAuthClientServer auth(g_ceph_context);
  auth.auth_registry.refresh_config();

  auto* server = Messenger::create(g_ceph_context, ms_type,
    entity_name_t::OSD(0), "server", getpid());
  auto* client = Messenger::create(g_ceph_context, ms_type,
    entity_name_t::CLIENT(-1), "client", getpid());

  server->set_default_policy(Messenger::Policy::stateless_server(0));
  client->set_default_policy(Messenger::Policy::lossy_client(0));
  server->set_auth_client(&auth); server->set_auth_server(&auth);
  client->set_auth_client(&auth); client->set_auth_server(&auth);
  server->set_require_authorizer(false);

  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1");
  server->bind(bind_addr);

  BenchDispatcher server_disp(is_pingpong);
  BenchDispatcher client_disp(false);
  server->add_dispatcher_head(&server_disp);
  client->add_dispatcher_head(&client_disp);

  server->start();
  client->start();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto conn = client->connect_to(server->get_mytype(), server->get_myaddrs());

  // Wait for connection
  for (int i = 0; i < 50; i++) {
    if (server_disp.got_accept.load() || conn->is_connected()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (!server_disp.got_accept.load()) {
    conn->send_message(new MPing());
    for (int i = 0; i < 50; i++) {
      if (server_disp.recv_count > 0) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  server_disp.recv_count = 0;
  client_disp.recv_count = 0;

  auto start = std::chrono::high_resolution_clock::now();

  if (is_pingpong) {
    for (int i = 0; i < num_msgs; i++)
      conn->send_message(new MPing());
    for (int i = 0; i < 600; i++) {
      if (client_disp.recv_count >= num_msgs) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  } else {
    int per_thread = num_msgs / num_threads;
    std::vector<std::thread> senders;
    for (int t = 0; t < num_threads; t++) {
      senders.emplace_back([&]() {
        for (int i = 0; i < per_thread; i++)
          conn->send_message(new MPing());
      });
    }
    for (auto& t : senders) t.join();
    int64_t expected = (int64_t)num_threads * per_thread;
    for (int i = 0; i < 600; i++) {
      if (server_disp.recv_count >= expected) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  auto elapsed = std::chrono::duration<double>(
    std::chrono::high_resolution_clock::now() - start).count();

  int64_t recv = is_pingpong ? client_disp.recv_count.load() : server_disp.recv_count.load();
  std::cerr << args[0] << " " << mode << ": "
            << recv << "/" << num_msgs << " in "
            << std::fixed << std::setprecision(3) << elapsed << "s = "
            << (int)(recv / elapsed) << " msg/s\n";

  client->shutdown(); server->shutdown();
  client->wait(); server->wait();
  delete client; delete server;
  return (recv >= num_msgs) ? 0 : 1;
}
