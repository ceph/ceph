// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_FCGI_H
#define CEPH_RGW_FCGI_H

#include "rgw_client_io.h"


struct FCGX_Request;


class RGWFCGX : public RGWClientIO
{
  FCGX_Request *fcgx;

  int status_num;

protected:
  void init_env(CephContext *cct);
  int write_data(const char *buf, int len);
  int read_data(char *buf, int len);

  int send_status(const char *status, const char *status_name);
  int send_100_continue();
  int complete_header();
  int complete_request() { return 0; }
  int send_content_length(uint64_t len);
public:
  RGWFCGX(FCGX_Request *_fcgx) : fcgx(_fcgx) {}
  void flush();
};


#endif
