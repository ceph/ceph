// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <functional>
#include <boost/optional.hpp>

#include "include/common_fwd.h"

namespace rgw::kafka {

// the reply callback is expected to get an integer parameter
// indicating the result, and not to return anything
typedef std::function<void(int)> reply_callback_t;

// initialize the kafka manager
bool init(CephContext* cct);

// shutdown the kafka manager
void shutdown();

// key class for the connection list
struct connection_id_t {
  std::string broker;
  std::string user;
  std::string password;
  std::string ca_location;
  std::string mechanism;
  bool ssl = false;
  connection_id_t() = default;
  connection_id_t(const std::string& _broker,
                  const std::string& _user,
                  const std::string& _password,
                  const boost::optional<const std::string&>& _ca_location,
                  const boost::optional<const std::string&>& _mechanism,
                  bool _ssl);
};

std::string to_string(const connection_id_t& id);

// connect to a kafka endpoint
bool connect(connection_id_t& conn_id,
             const std::string& url,
             bool use_ssl,
             bool verify_ssl,
             boost::optional<const std::string&> ca_location,
             boost::optional<const std::string&> mechanism,
             boost::optional<const std::string&> user_name,
             boost::optional<const std::string&> password,
             boost::optional<const std::string&> brokers);

// publish a message over a connection that was already created
int publish(const connection_id_t& conn_id,
            const std::string& topic,
            const std::string& message);

// publish a message over a connection that was already created
// and pass a callback that will be invoked (async) when broker confirms
// receiving the message
int publish_with_confirm(const connection_id_t& conn_id,
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

}

