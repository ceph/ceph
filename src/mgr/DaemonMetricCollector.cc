#include "DaemonMetricCollector.h"
#include "common/admin_socket_client.h"

#include <iostream>
#include <string>
#include <filesystem>


#include <iostream>
#include <string>
#include <filesystem>

void DaemonMetricCollector::main() {
  std::cout << "metric" << std::endl;
  while (1) {
    update_sockets();
   }
}

void update_sockets() {
  for (const auto &entry : fs::recursive_directory_iterator(socketdir)) {
    if (entry.path().extension() == ".asok") {
      if (clients.find(entry.path()) == clients.end()) {
      AdminSocketClient sock(entry.path());
      clients[entry.path()] = sock;
    }
    }
  }
}

void DaemonMetricCollector::send_request_per_client() {
  AdminSocketClient mgr_sock_client("/var/run/ceph/whatever");
  std::string request("{\"prefix\":\"perf dump\"}");
  std::string path = "/run/"
  for (const auto & entry : std::filesystem::directory_iterator(path)) {
    if (clients.find(entry.path()) == clients.end()) {
      AdminSocketClient sock(entry.path());
      clients[entry.path()] = sock;
    }
  }
}

void DaemonMetricCollector::start_mgr_connection() {
  AdminSocketClient mgr_sock_client("/var/run/ceph/whatever");
  std::string request("{\"prefix\":\"help\"}");
  std::string response;
  mgr_sock_client.do_request(request, &response);
  std::cout << response << std::endl;
}
