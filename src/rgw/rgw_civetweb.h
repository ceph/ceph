// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_MONGOOSE_H
#define CEPH_RGW_MONGOOSE_H
#define TIME_BUF_SIZE 128

#include "rgw_client_io.h"


struct mg_connection;

class RGWCivetWeb : public rgw::io::RestfulClient,
                    public rgw::io::BuffererSink {
  RGWEnv env;
  mg_connection *conn;

  int port;

  bool explicit_keepalive;
  bool explicit_conn_close;

  rgw::io::StaticOutputBufferer<> txbuf;

  size_t write_data(const char *buf, size_t len) override;
  size_t read_data(char *buf, size_t len);
  size_t dump_date_header();

public:
  void init_env(CephContext *cct);

  size_t send_status(int status, const char *status_name) override;
  size_t send_100_continue() override;
  size_t send_header(const boost::string_ref& name,
                     const boost::string_ref& value) override;
  size_t send_content_length(uint64_t len) override;
  size_t complete_header() override;

  size_t recv_body(char* buf, size_t max) override {
    return read_data(buf, max);
  }

  size_t send_body(const char* buf, size_t len) override {
    return write_data(buf, len);
  }

  size_t complete_request() override;

  void flush() override;

  RGWEnv& get_env() noexcept override {
    return env;
  }

  RGWCivetWeb(mg_connection *_conn, int _port);
};

#endif
