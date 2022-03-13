#pragma once

#include "common/admin_socket_client.h"
#include <string>
#include <map>
#include <vector>

#include <string>
#include <map>
#include <vector>

class DaemonMetricCollector {
 public:
  int i;
  std::string main();

private:
  // TODO: add clients
  //       check removed sockets
  //       list dir of sockets
  std::map<std::string, AdminSocketClient> clients;
  std::string result;
  void update_sockets();
  std::string send_requests();
  void start_mgr_connection();
};
