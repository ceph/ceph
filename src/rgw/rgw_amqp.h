// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <functional>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "include/common_fwd.h"

namespace rgw::amqp {
// forward declaration of connection object
struct connection_t;

typedef boost::intrusive_ptr<connection_t> connection_ptr_t;

// required interfaces needed so that connection_t could be used inside boost::intrusive_ptr
void intrusive_ptr_add_ref(const connection_t* p);
void intrusive_ptr_release(const connection_t* p);

// the reply callback is expected to get an integer parameter
// indicating the result, and not to return anything
typedef std::function<void(int)> reply_callback_t;

// initialize the amqp manager
bool init(CephContext* cct);

// shutdown the amqp manager
void shutdown();

// connect to an amqp endpoint
connection_ptr_t connect(const std::string& url, const std::string& exchange, bool mandatory_delivery);

// publish a message over a connection that was already created
int publish(connection_ptr_t& conn,
    const std::string& topic,
    const std::string& message);

// publish a message over a connection that was already created
// and pass a callback that will be invoked (async) when broker confirms
// receiving the message
int publish_with_confirm(connection_ptr_t& conn, 
    const std::string& topic,
    const std::string& message,
    reply_callback_t cb);

// convert the integer status returned from the "publish" function to a string
std::string status_to_string(int s);

// number of connections
size_t get_connection_count();
  
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
bool disconnect(connection_ptr_t& conn);

}

