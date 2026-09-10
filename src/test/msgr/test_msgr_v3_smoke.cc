// Messenger V3 vs V2 Benchmark
// Compares async+posix (V2) vs v3+posix (V3) using real Ceph Messenger APIs
//
// Build: ninja ceph_test_msgr_v3_smoke
// Run:   ./bin/ceph_test_msgr_v3_smoke [num_msgs] [num_threads]

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

// Dispatcher that optionally echoes and counts messages
class BenchDispatcher : public Dispatcher {
public:
  std::atomic<int64_t> recv_count{0};
  std::atomic<bool> got_connect{false};
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
  void ms_handle_fast_connect(Connection *con) override { got_connect = true; }
  void ms_handle_fast_accept(Connection *con) override { got_accept = true; }
  bool ms_handle_reset(Connection *con) override { return true; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; }
  bool ms_handle_fast_authentication(Connection *con) override { return true; }
};

struct BenchResult {
  double elapsed_sec = 0;
  int64_t total_sent = 0;
  int64_t server_recv = 0;
  int64_t client_recv = 0;
  bool ok = false;
};

// Create, bind, configure a messenger pair. Returns connected conn.
struct MessengerPair {
  Messenger* server = nullptr;
  Messenger* client = nullptr;
  DummyAuthClientServer auth;
  BenchDispatcher server_disp;
  BenchDispatcher client_disp;
  ConnectionRef conn;

  MessengerPair(bool server_echo)
    : auth(g_ceph_context), server_disp(server_echo), client_disp(false) {}

  ~MessengerPair() {
    if (client) { client->shutdown(); client->wait(); delete client; }
    if (server) { server->shutdown(); server->wait(); delete server; }
  }

  bool setup(const std::string& ms_type) {
    auth.auth_registry.refresh_config();

    server = Messenger::create(g_ceph_context, ms_type,
      entity_name_t::OSD(0), "server", getpid());
    client = Messenger::create(g_ceph_context, ms_type,
      entity_name_t::CLIENT(-1), "client", getpid());
    if (!server || !client) return false;

    server->set_default_policy(Messenger::Policy::stateless_server(0));
    client->set_default_policy(Messenger::Policy::lossy_client(0));
    server->set_auth_client(&auth); server->set_auth_server(&auth);
    client->set_auth_client(&auth); client->set_auth_server(&auth);
    server->set_require_authorizer(false);

    entity_addr_t bind_addr;
    bind_addr.parse("v2:127.0.0.1");
    if (server->bind(bind_addr) < 0) return false;

    server->add_dispatcher_head(&server_disp);
    client->add_dispatcher_head(&client_disp);
    server->start();
    client->start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    conn = client->connect_to(server->get_mytype(), server->get_myaddrs());
    if (!conn) return false;

    // Wait for connection
    for (int i = 0; i < 50; i++) {
      if (server_disp.got_accept.load() || conn->is_connected()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!conn->is_connected() && !server_disp.got_accept.load()) {
      conn->send_message(new MPing());
      for (int i = 0; i < 50; i++) {
        if (server_disp.recv_count > 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
    return server_disp.recv_count > 0 || server_disp.got_accept.load();
  }
};

// ============================================================================
// Test 1: Ping-Pong Latency (single-threaded, measures round-trip)
// ============================================================================

static BenchResult run_pingpong(const std::string& ms_type, int num_msgs) {
  BenchResult result;
  MessengerPair mp(true);  // server echoes
  if (!mp.setup(ms_type)) {
    std::cerr << "  FAIL: setup\n";
    return result;
  }

  // Reset counters after connection setup
  mp.server_disp.recv_count = 0;
  mp.client_disp.recv_count = 0;

  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < num_msgs; i++) {
    mp.conn->send_message(new MPing());
  }
  for (int i = 0; i < 300; i++) {
    if (mp.client_disp.recv_count >= num_msgs) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  auto end = std::chrono::high_resolution_clock::now();

  result.elapsed_sec = std::chrono::duration<double>(end - start).count();
  result.total_sent = num_msgs;
  result.server_recv = mp.server_disp.recv_count.load();
  result.client_recv = mp.client_disp.recv_count.load();
  result.ok = (result.client_recv >= num_msgs);
  return result;
}

// ============================================================================
// Test 2: Multi-Threaded Throughput (contention test)
// N threads blast messages through ONE connection to a sink server
// ============================================================================

static BenchResult run_throughput(const std::string& ms_type,
                                  int total_msgs, int num_threads) {
  BenchResult result;
  MessengerPair mp(false);  // server sinks, no echo
  if (!mp.setup(ms_type)) {
    std::cerr << "  FAIL: setup\n";
    return result;
  }

  mp.server_disp.recv_count = 0;
  int per_thread = total_msgs / num_threads;

  std::atomic<int64_t> sent_total{0};
  std::vector<std::thread> senders;

  auto start = std::chrono::high_resolution_clock::now();

  for (int t = 0; t < num_threads; t++) {
    senders.emplace_back([&]() {
      for (int i = 0; i < per_thread; i++) {
        mp.conn->send_message(new MPing());
      }
      sent_total.fetch_add(per_thread, std::memory_order_relaxed);
    });
  }

  for (auto& t : senders) t.join();
  // Wait for server to receive all
  int64_t expected = (int64_t)num_threads * per_thread;
  for (int i = 0; i < 600; i++) {
    if (mp.server_disp.recv_count >= expected) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  auto end = std::chrono::high_resolution_clock::now();

  result.elapsed_sec = std::chrono::duration<double>(end - start).count();
  result.total_sent = sent_total.load();
  result.server_recv = mp.server_disp.recv_count.load();
  result.client_recv = 0;
  result.ok = (result.server_recv >= expected);
  return result;
}

// ============================================================================
// Main
// ============================================================================

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

  int num_msgs = 50000;
  int num_threads = 4;
  if (args.size() > 0) num_msgs = atoi(args[0]);
  if (args.size() > 1) num_threads = atoi(args[1]);

  std::cerr << "\n";
  std::cerr << "================================================================\n";
  std::cerr << "  Messenger V3 vs V2 Benchmark\n";
  std::cerr << "  Real Ceph Messenger APIs, MPing messages, msgr2 wire format\n";
  std::cerr << "================================================================\n";

  // ---- Test 1: Ping-Pong ----
  std::cerr << "\n--- Test 1: Ping-Pong (" << num_msgs << " round-trips) ---\n";
  std::cerr << "    Single thread, send-wait-reply. Tests per-message latency.\n\n";

  std::cerr << "  V2 (async+posix): ";
  auto pp_v2 = run_pingpong("async+posix", num_msgs);
  if (pp_v2.ok)
    std::cerr << pp_v2.total_sent << " msgs in "
              << std::fixed << std::setprecision(3) << pp_v2.elapsed_sec
              << "s = " << (int)(pp_v2.total_sent / pp_v2.elapsed_sec) << " msg/s\n";
  else
    std::cerr << "FAIL (recv=" << pp_v2.client_recv << "/" << pp_v2.total_sent << ")\n";

  std::cerr << "  V3 (v3+posix):    ";
  auto pp_v3 = run_pingpong("v3+posix", num_msgs);
  if (pp_v3.ok)
    std::cerr << pp_v3.total_sent << " msgs in "
              << std::fixed << std::setprecision(3) << pp_v3.elapsed_sec
              << "s = " << (int)(pp_v3.total_sent / pp_v3.elapsed_sec) << " msg/s\n";
  else
    std::cerr << "FAIL (recv=" << pp_v3.client_recv << "/" << pp_v3.total_sent << ")\n";

  if (pp_v2.ok && pp_v3.ok) {
    double r = (pp_v3.total_sent / pp_v3.elapsed_sec) / (pp_v2.total_sent / pp_v2.elapsed_sec);
    std::cerr << "  Ratio: " << std::fixed << std::setprecision(2) << r << "x\n";
  }

  // ---- Test 2: Multi-Threaded Throughput ----
  std::cerr << "\n--- Test 2: Throughput (" << num_msgs
            << " msgs, " << num_threads << " sender threads) ---\n";
  std::cerr << "    All threads send through ONE connection. Tests write contention.\n\n";

  int thread_counts[] = {1, 2, 4, 8, 16, 32};
  std::cerr << "  " << std::setw(8) << "Threads"
            << "  " << std::setw(18) << "V2 (async+posix)"
            << "  " << std::setw(18) << "V3 (v3+posix)"
            << "  " << std::setw(10) << "V3/V2" << "\n";
  std::cerr << "  " << std::setw(8) << "-------"
            << "  " << std::setw(18) << "------------------"
            << "  " << std::setw(18) << "------------------"
            << "  " << std::setw(10) << "-----" << "\n";

  for (int nt : thread_counts) {
    if (nt > num_threads) break;

    auto v2 = run_throughput("async+posix", num_msgs, nt);
    auto v3 = run_throughput("v3+posix", num_msgs, nt);

    double v2_rate = v2.ok ? v2.server_recv / v2.elapsed_sec : 0;
    double v3_rate = v3.ok ? v3.server_recv / v3.elapsed_sec : 0;

    std::cerr << "  " << std::setw(8) << nt;

    if (v2.ok)
      std::cerr << "  " << std::setw(14) << (int)v2_rate << " m/s";
    else
      std::cerr << "  " << std::setw(18) << "FAIL";

    if (v3.ok)
      std::cerr << "  " << std::setw(14) << (int)v3_rate << " m/s";
    else
      std::cerr << "  " << std::setw(18) << "FAIL";

    if (v2.ok && v3.ok && v2_rate > 0)
      std::cerr << "  " << std::setw(8) << std::fixed << std::setprecision(1)
                << (v3_rate / v2_rate) << "x";

    std::cerr << "\n";
  }

  std::cerr << "\n";
  return 0;
}
