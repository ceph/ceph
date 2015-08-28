// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_MONGOOSE_H
#define CEPH_RGW_MONGOOSE_H
#define TIME_BUF_SIZE 128

#include "rgw_client_io.h"


struct mg_connection;


class RGWMongoose : public RGWClientIOEngine
{
  mg_connection * const conn;
  RGWEnv env;

  int port;

  bool has_content_length;
  bool explicit_keepalive;
  bool explicit_conn_close;

public:
  RGWMongoose(mg_connection *_conn, int _port);

  void init_env(CephContext *cct) override;
  int write_data(const char *buf, int len) override;
  int read_data(char *buf, int len) override;

  void flush(RGWClientIO& controller) override;
  int send_status(RGWClientIO& controller,
                  const char *status,
                  const char *status_name) override;
  int send_100_continue(RGWClientIO& controller) override;
  int complete_header(RGWClientIO& controller) override;
  int send_content_length(RGWClientIO& controller,
                          uint64_t len) override;

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
