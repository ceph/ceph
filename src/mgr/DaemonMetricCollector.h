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
  void main();

private:
  // TODO: add clients
  //       check removed sockets
  //       list dir of sockets
  std::map<std::string, AdminSocketClient> clients;
  void update_sockets();
  void send_requests();
  void start_mgr_connection();
};
