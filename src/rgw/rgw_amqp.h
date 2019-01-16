// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <functional>
#include <stdexcept>

namespace rgw::amqp {
// farward declaration of coonection object
struct connection_t;

typedef std::function<void(int)> reply_callback_t;

// connect to an amqp endpoint
connection_t& connect(const std::string& url, const std::string& exchange);

// publish a message over a connection that was already created
int publish(connection_t& conn,
    const std::string& topic,
    const std::string& message);

// publish a message over a connection that was already created
// and pass a callback that will be invoked (async) when broker confirms
// receiving the message
int publish_with_confirm(connection_t& conn, 
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb);

// convert the integer status returned from the "publish" function to a string
std::string status_to_string(int s);

// number of connections
size_t get_connection_number();
  
// return the number of messages that were sent
// to broker, but were not yet acked/nacked/timedout
size_t get_inflight();

// running counter of successfully queued messages
size_t get_queued();

// running counter of dequeued messages
size_t get_dequeued();

// number of maximum allowed connections
size_t get_max_connections();

// number of maximum allowed inflight messages
size_t get_max_inflight();

// maximum number of messages in the queue
size_t get_max_queue();

// disconnect from an amqp broker
bool disconnect(connection_t& conn);

// exception object for connection establishment error
struct connection_error : public std::runtime_error {
  connection_error(const std::string& what_arg) : 
    std::runtime_error("amqp connection error: " + what_arg) {}
};
}

