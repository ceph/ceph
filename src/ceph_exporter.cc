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
  boost::thread server_thread(http_server_thread_entrypoint);
  DaemonMetricCollector &collector = collector_instance();
  collector.main();
  server_thread.join();
}
