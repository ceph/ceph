
#include "rgw_prometheus.h"
#include "rgw_exporter.h"

// this is a temp file , which will not be needed 
// Since we are defining endpoint in rgw_rest.cc
//

// External reference to the RGWExporter instance
extern RGWExporter* rgw_exporter;

void handle_prometheus_request(struct req_state* s, RGWClientIO* cio) {
    std::string metrics = rgw_exporter->get_prometheus_metrics();
    // Send the metrics data as the HTTP response
    cio->send_data(metrics.c_str(), metrics.size());
}
