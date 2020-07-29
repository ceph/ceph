// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_FCGI_H
#define CEPH_RGW_FCGI_H

#include "acconfig.h"
#include <fcgiapp.h>

#include "rgw_client_io.h"

struct FCGX_Request;

class RGWFCGX : public rgw::io::RestfulClient,
                public rgw::io::BuffererSink {
  FCGX_Request *fcgx;
  RGWEnv env;

  rgw::io::StaticOutputBufferer<> txbuf;

  size_t read_data(char* buf, size_t len);
  size_t write_data(const char* buf, size_t len) override;

public:
  explicit RGWFCGX(FCGX_Request* const fcgx)
    : fcgx(fcgx),
      txbuf(*this) {
  }

  int init_env(CephContext* cct) override;
  size_t send_status(int status, const char* status_name) override;
  size_t send_100_continue() override;
  size_t send_header(const std::string_view& name,
                     const std::string_view& value) override;
  size_t send_content_length(uint64_t len) override;
  size_t complete_header() override;

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

  size_t complete_request() override {
    return 0;
  }
};

#endif
