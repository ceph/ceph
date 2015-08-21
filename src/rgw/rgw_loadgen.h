// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_LOADGEN_H
#define CEPH_RGW_LOADGEN_H

#include "rgw_client_io.h"


struct RGWLoadGenRequestEnv {
  int port;
  uint64_t content_length;
  string content_type;
  string request_method;
  string uri;
  string query_string;
  string date_str;

  map<string, string> headers;

  RGWLoadGenRequestEnv() : port(0), content_length(0) {}

  void set_date(utime_t& tm);
  int sign(RGWAccessKey& access_key);
};

class RGWLoadGenIO : public RGWClientIOEngine
{
  uint64_t left_to_read;
  RGWLoadGenRequestEnv *req;
  RGWEnv env;

public:
  RGWLoadGenIO(RGWLoadGenRequestEnv *_re) : left_to_read(0), req(_re) {}

  void init_env(CephContext *cct);

  int write_data(const char *buf, int len);
  int read_data(char *buf, int len);

  int send_status(RGWClientIO& controller,
                  const char *status,
                  const char *status_name);
  int send_100_continue(RGWClientIO& controller);
  int complete_header(RGWClientIO& controller);
  int complete_request(RGWClientIO& controller);
  int send_content_length(RGWClientIO& controller, uint64_t len);
  void flush(RGWClientIO& controller);

  RGWEnv& get_env() override {
    return env;
  }
};


#endif
