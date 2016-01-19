// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_MONGOOSE_H
#define CEPH_RGW_MONGOOSE_H
#define TIME_BUF_SIZE 128

#include "rgw_client_io.h"


struct mg_connection;


class RGWMongoose : public RGWClientIO
{
  mg_connection *conn;

  bufferlist header_data;
  bufferlist data;

  int port;
  int status_num;

  bool header_done;
  bool sent_header;
  bool has_content_length;
  bool explicit_keepalive;
  bool explicit_conn_close;

public:
  void init_env(CephContext *cct);

  int write_data(const char *buf, int len);
  int read_data(char *buf, int len);

  int send_status(int status, const char *status_name);
  int send_100_continue();
  int complete_header();
  int complete_request();
  int send_content_length(uint64_t len);

  RGWMongoose(mg_connection *_conn, int _port);
  void flush();
};


#endif
