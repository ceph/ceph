// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <stdexcept>

namespace amqp {
// farward declaration of coonection object
struct connection_t;

// connect to an amqp endpoint
const connection_t& connect(const std::string& url, const std::string& exchange);

// publish a message over a connection that was already created
int publish(const connection_t& conn,
    const std::string& topic,
    const std::string& message);

// convert the integer status returned from the "publish" function to a string
std::string status_to_string(int s);

// exception object for connection establishment error
struct connection_error : public std::runtime_error {
  connection_error(const std::string& what_arg) : 
    std::runtime_error("amqp connection error: " + what_arg) {}
};
}

