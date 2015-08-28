// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_FCGI_H
#define CEPH_RGW_FCGI_H

#include "rgw_client_io.h"


struct FCGX_Request;


class RGWFCGX : public RGWClientIOEngine
{
  FCGX_Request *fcgx;
  RGWEnv env;

public:
  RGWFCGX(FCGX_Request *_fcgx) : fcgx(_fcgx) {}

  void init_env(CephContext *cct) override;
  int write_data(const char *buf, int len) override;
  int read_data(char *buf, int len) override;

  int send_status(RGWClientIO& controller,
                  const char *status,
                  const char *status_name) override;
  int send_100_continue(RGWClientIO& controller) override;
  int complete_header(RGWClientIO& controller) override;
  int send_content_length(RGWClientIO& controller,
                          uint64_t len) override;
  void flush(RGWClientIO& controller) override;

  int complete_request(RGWClientIO& controller) override {
    return 0;
  }

  int send_content_length(RGWClientIO& controller,
                          const RGWDynamicLengthMode) override {
    return 0;
  }

  RGWEnv& get_env() override {
    return env;
  }
};


#endif
