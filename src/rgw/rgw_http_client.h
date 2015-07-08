// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_HTTP_CLIENT_H
#define CEPH_RGW_HTTP_CLIENT_H

#include "rgw_common.h"

class RGWHTTPClient
{
  friend class RGWHTTPManager;

  bufferlist send_bl;
  bufferlist::iterator send_iter;
  size_t send_len;
  bool has_send_len;
protected:
  CephContext *cct;

  list<pair<string, string> > headers;
  int init_async(const char *method, const char *url, void **handle);
public:
  virtual ~RGWHTTPClient() {}
  RGWHTTPClient(CephContext *_cct): send_len (0), has_send_len(false), cct(_cct) {}

  void append_header(const string& name, const string& val) {
    headers.push_back(pair<string, string>(name, val));
  }

  virtual int receive_header(void *ptr, size_t len) = 0;
  virtual int receive_data(void *ptr, size_t len) = 0;
  virtual int send_data(void *ptr, size_t len) = 0;

  void set_send_length(size_t len) {
    send_len = len;
    has_send_len = true;
  }

  int process(const char *method, const char *url);
  int process(const char *url) { return process("GET", url); }
};

class RGWHTTPManager {
  CephContext *cct;
  void *multi_handle;
public:
  RGWHTTPManager(CephContext *_cct);
  ~RGWHTTPManager();

  int init_async(RGWHTTPClient *client, const char *method, const char *url, void **handle);
  int process_requests(bool wait_for_data, bool *done);
  int complete_requests();
};

#endif
