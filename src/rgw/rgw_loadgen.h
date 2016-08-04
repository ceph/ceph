// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_LOADGEN_H
#define CEPH_RGW_LOADGEN_H

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
  int sign(RGWAccessKey& access_key);
};

/* XXX does RGWLoadGenIO actually want to perform stream/HTTP I/O,
 * or (e.g) are these NOOPs? */
class RGWLoadGenIO : public RGWStreamIOEngine
{
  uint64_t left_to_read;
  RGWLoadGenRequestEnv* req;
  RGWEnv env;

public:
  explicit RGWLoadGenIO(RGWLoadGenRequestEnv* const req)
    : left_to_read(0),
      req(req) {
  }

  void init_env(CephContext *cct);
  int read_data(char *buf, int len);
  int write_data(const char *buf, int len);

  int send_status(int status, const char *status_name);
  int send_100_continue();
  int complete_header();
  int send_content_length(uint64_t len);

  void flush();

  RGWEnv& get_env() override {
    return env;
  }

  int complete_request();
};

#endif
