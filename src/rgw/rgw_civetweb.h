// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_MONGOOSE_H
#define CEPH_RGW_MONGOOSE_H
#define TIME_BUF_SIZE 128

#include "rgw_client_io.h"


struct mg_connection;

class RGWCivetWeb : public RGWStreamIOEngine
{
  RGWEnv env;
  mg_connection *conn;

  int port;

  bool explicit_keepalive;
  bool explicit_conn_close;

  std::size_t dump_date_header();
public:
  void init_env(CephContext *cct);

  std::size_t write_data(const char *buf, std::size_t len) override;
  std::size_t read_data(char *buf, std::size_t len) override;

  std::size_t send_status(int status, const char *status_name) override;
  std::size_t send_100_continue() override;
  std::size_t send_content_length(uint64_t len) override;
  std::size_t complete_header() override;
  int complete_request() override;

  void flush() override;

  RGWEnv& get_env() noexcept override {
    return env;
  }

  RGWCivetWeb(mg_connection *_conn, int _port);
};

#endif
