// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_FCGI_H
#define CEPH_RGW_FCGI_H

#include "acconfig.h"
#include <fcgiapp.h>

#include "rgw_client_io.h"

struct FCGX_Request;

class RGWFCGX : public RGWStreamIOEngine
{
  FCGX_Request *fcgx;
  RGWEnv env;

public:
  explicit RGWFCGX(FCGX_Request* const fcgx)
    : fcgx(fcgx) {
  }

  void init_env(CephContext* cct) override;
  int read_data(char* buf, int len) override;
  int write_data(const char* buf, int len) override;

  int send_status(int status, const char* status_name) override;
  int send_100_continue() override;
  int send_content_length(uint64_t len) override;
  int complete_header() override;

  void flush();

  RGWEnv& get_env() override {
    return env;
  }

  int complete_request() override {
    return 0;
  }
};

#endif
