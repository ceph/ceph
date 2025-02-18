#ifndef RGW_PROMETHEUS_H
#define RGW_PROMETHEUS_H

#include <string>

// Function to handle Prometheus metrics request
void handle_prometheus_request(struct req_state* s, class RGWClientIO* cio);

#endif // RGW_PROMETHEUS_H
