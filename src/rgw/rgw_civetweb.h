// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_MONGOOSE_H
#define CEPH_RGW_MONGOOSE_H
#define TIME_BUF_SIZE 128

#include "rgw_client_io.h"


struct mg_connection;

class RGWCivetWeb : public RGWStreamIOEngine,
                    private RGWStreamIOFacade
{
  RGWEnv env;
  mg_connection *conn;

  int port;

  bool explicit_keepalive;
  bool explicit_conn_close;

  int dump_date_header();
public:
  void init_env(CephContext *cct);

  int write_data(const char *buf, int len) override;
  int read_data(char *buf, int len) override;

  int send_status(int status, const char *status_name) override;
  int send_100_continue() override;
  int send_content_length(uint64_t len) override;
  int complete_header() override;
  int complete_request() override;

  void flush() override;

  RGWEnv& get_env() override {
    return env;
  }

  RGWCivetWeb(mg_connection *_conn, int _port);
  RGWCivetWeb(const RGWCivetWeb& rhs)
    : RGWStreamIOFacade(this),
      env(rhs.env),
      conn(rhs.conn),
      explicit_keepalive(rhs.explicit_keepalive),
      explicit_conn_close(rhs.explicit_conn_close) {
  }
};

#endif
