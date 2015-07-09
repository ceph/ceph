// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_HTTP_CLIENT_H
#define CEPH_RGW_HTTP_CLIENT_H

#include "common/RWLock.h"
#include "include/atomic.h"
#include "rgw_common.h"

struct rgw_http_req_data;

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
  int init_request(const char *method, const char *url, rgw_http_req_data *req_data);
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
  bool is_threaded;
  atomic_t going_down;

  RWLock reqs_lock;
  map<uint64_t, rgw_http_req_data *> reqs;
  int64_t num_reqs;
  int64_t max_threaded_req;

  void register_request(rgw_http_req_data *req_data);
  void unregister_request(rgw_http_req_data *req_data);
  void finish_request(rgw_http_req_data *req_data);
  int link_request(rgw_http_req_data *req_data);

  void link_pending_requests();

  class ReqsThread : public Thread {
    RGWHTTPManager *manager;

  public:
    ReqsThread(RGWHTTPManager *_m) : manager(_m) {}
    void *entry();
  };

  ReqsThread *reqs_thread;

  void *reqs_thread_entry();

public:
  RGWHTTPManager(CephContext *_cct);
  ~RGWHTTPManager();

  void set_threaded();
  void stop();

  int add_request(RGWHTTPClient *client, const char *method, const char *url);

  /* only for non threaded case */
  int process_requests(bool wait_for_data, bool *done);

  int complete_requests();
};

#endif
