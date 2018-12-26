// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <amqp.h>
#include <string>

namespace amqp {


// connect to an amqp endpoint
amqp_connection_state_t connect(const std::string& url, const std::string& exchange);

// publish a message over a connection that was already created
bool publish(amqp_connection_state_t conn, 
    const std::string& exchange,
    const std::string& topic,
    const std::string& message);
}

