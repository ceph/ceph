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
  std::size_t read_data(char* buf, std::size_t len) override;
  std::size_t write_data(const char* buf, std::size_t len) override;

  std::size_t send_status(int status, const char* status_name) override;
  std::size_t send_100_continue() override;
  std::size_t send_header(const boost::string_ref& name,
                          const boost::string_ref& value) override;
  std::size_t send_content_length(uint64_t len) override;
  std::size_t complete_header() override;

  void flush();

  RGWEnv& get_env() noexcept override {
    return env;
  }

  int complete_request() override {
    return 0;
  }
};

#endif
