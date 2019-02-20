// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>

namespace amqp_mock {
void set_valid_port(int port);
void set_valid_host(const std::string& host);
void set_valid_vhost(const std::string& vhost);
void set_valid_user(const std::string& user, const std::string& password);
  
extern bool FAIL_NEXT_WRITE;        // default "false"
extern bool FAIL_NEXT_READ;         // default "false"
extern bool REPLY_ACK;              // default "true"
}

