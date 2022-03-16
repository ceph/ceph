#include "exporter/DaemonMetricCollector.h"
#include "exporter/http_server.h"
#include <iostream>
#include <map>
#include <iostream>
#include <string>
#include <boost/thread/thread.hpp>



// FIXME: lets save collector instance here for now.
DaemonMetricCollector collector;


int main(int argc, char** argv) {
  // TODO: daemonize
  std::cout << "inside exporter" << std::endl;

  std::cout << "Starting http server thread..." << std::endl;
  boost::thread server_thread(http_server_thread_entrypoint);
  std::cout << "Starting collector..." << std::endl;
  DaemonMetricCollector &collector = collector_instance();
  collector.main();
  server_thread.join();
}
