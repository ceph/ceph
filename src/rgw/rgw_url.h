// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
namespace rgw {
// parse a URL of the form: http|https|amqp|amqps|kafka://[user:password@]<host>[:port]
bool parse_url_authority(const std::string& url, std::string& host, std::string& user, std::string& password);
bool parse_url_userinfo(const std::string& url, std::string& user, std::string& password);
}

