// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <map>
#include <string>

#include "rgw_client_io.h"


struct RGWLoadGenRequestEnv {
  int port;
  uint64_t content_length;
  std::string content_type;
  std::string request_method;
  std::string uri;
  std::string query_string;
  std::string date_str;

  std::map<std::string, std::string> headers;

  RGWLoadGenRequestEnv()
    : port(0),
      content_length(0) {
  }

  void set_date(utime_t& tm);
  int sign(const DoutPrefixProvider *dpp, RGWAccessKey& access_key);
};

/* XXX does RGWLoadGenIO actually want to perform stream/HTTP I/O,
 * or (e.g) are these NOOPs? */
class RGWLoadGenIO : public rgw::io::RestfulClient
{
  uint64_t left_to_read;
  RGWLoadGenRequestEnv* req;
  RGWEnv env;

  int init_env(CephContext *cct) override;
  size_t read_data(char *buf, size_t len);
  size_t write_data(const char *buf, size_t len);

public:
  explicit RGWLoadGenIO(RGWLoadGenRequestEnv* const req)
    : left_to_read(0),
      req(req) {
  }

  size_t send_status(int status, const char *status_name) override;
  size_t send_100_continue() override;
  size_t send_header(const std::string_view& name,
                     const std::string_view& value) override;
  size_t complete_header() override;
  size_t send_content_length(uint64_t len) override;

  size_t recv_body(char* buf, size_t max) override {
    return read_data(buf, max);
  }

  size_t send_body(const char* buf, size_t len) override {
    return write_data(buf, len);
  }

  void flush() override;

  RGWEnv& get_env() noexcept override {
    return env;
  }

  size_t complete_request() override;
};
