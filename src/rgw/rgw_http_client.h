// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "common/async/yield_context.h"
#include "common/Cond.h"
#include "rgw_common.h"
#include "rgw_string.h"
#include "rgw_http_client_types.h"

#include <atomic>

using param_pair_t = std::pair<std::string, std::string>;
using param_vec_t = std::vector<param_pair_t>;

void rgw_http_client_init(CephContext *cct);
void rgw_http_client_cleanup();

struct rgw_http_req_data;
class RGWHTTPManager;

class RGWHTTPClient : public RGWIOProvider,
                      public NoDoutPrefix
{
  friend class RGWHTTPManager;

  bufferlist send_bl;
  bufferlist::iterator send_iter;
  bool has_send_len;
  long http_status;
  bool send_data_hint{false};
  size_t receive_pause_skip{0}; /* how many bytes to skip next time receive_data is called
                                   due to being paused */

  void *user_info{nullptr};

  rgw_http_req_data *req_data;

  bool verify_ssl; // Do not validate self signed certificates, default to false

  std::string ca_path;

  std::string client_cert;

  std::string client_key;

  std::atomic<unsigned> stopped { 0 };


protected:
  CephContext *cct;

  std::string method;
  std::string url_orig;
  std::string url;

  std::string protocol;
  std::string host;
  std::string resource_prefix;

  size_t send_len{0};

  param_vec_t headers;

  long  req_timeout{0L};

  void init();

  RGWHTTPManager *get_manager();

  int init_request(rgw_http_req_data *req_data);

  virtual int receive_header(void *ptr, size_t len) {
    return 0;
  }
  virtual int receive_data(void *ptr, size_t len, bool *pause) {
    return 0;
  }

  virtual int send_data(void *ptr, size_t len, bool *pause=nullptr) {
    return 0;
  }

  /* Callbacks for libcurl. */
  static size_t receive_http_header(void *ptr,
                                    size_t size,
                                    size_t nmemb,
                                    void *_info);

  static size_t receive_http_data(void *ptr,
                                  size_t size,
                                  size_t nmemb,
                                  void *_info);

  static size_t send_http_data(void *ptr,
                               size_t size,
                               size_t nmemb,
                               void *_info);

  ceph::mutex& get_req_lock();

  /* needs to be called under req_lock() */
  void _set_write_paused(bool pause);
  void _set_read_paused(bool pause);
public:
  static const long HTTP_STATUS_NOSTATUS     = 0;
  static const long HTTP_STATUS_UNAUTHORIZED = 401;
  static const long HTTP_STATUS_NOTFOUND     = 404;

  static constexpr int HTTPCLIENT_IO_READ    = 0x1;
  static constexpr int HTTPCLIENT_IO_WRITE   = 0x2;
  static constexpr int HTTPCLIENT_IO_CONTROL = 0x4;

  virtual ~RGWHTTPClient();
  explicit RGWHTTPClient(CephContext *cct,
                         const std::string& _method,
                         const std::string& _url);

  std::ostream& gen_prefix(std::ostream& out) const override;


  void append_header(const std::string& name, const std::string& val) {
    headers.push_back(std::pair<std::string, std::string>(name, val));
  }

  void set_send_length(size_t len) {
    send_len = len;
    has_send_len = true;
  }

  void set_send_data_hint(bool hint) {
    send_data_hint = hint;
  }

  long get_http_status() const {
    return http_status;
  }

  void set_http_status(long _http_status) {
    http_status = _http_status;
  }

  void set_verify_ssl(bool flag) {
    verify_ssl = flag;
  }

  // set request timeout in seconds
  // zero (default) mean that request will never timeout
  void set_req_timeout(long timeout) {
    req_timeout = timeout;
  }

  int process(optional_yield y);

  int wait(optional_yield y);
  void cancel();
  bool is_done();

  rgw_http_req_data *get_req_data() { return req_data; }

  std::string to_str();

  int get_req_retcode();

  void set_url(const std::string& _url) {
    url = _url;
  }

  const std::string& get_url_orig() const {
    return url_orig;
  }

  void set_method(const std::string& _method) {
    method = _method;
  }

  void set_io_user_info(void *_user_info) override {
    user_info = _user_info;
  }

  void *get_io_user_info() override {
    return user_info;
  }

  void set_ca_path(const std::string& _ca_path) {
    ca_path = _ca_path;
  }

  void set_client_cert(const std::string& _client_cert) {
    client_cert = _client_cert;
  }

  void set_client_key(const std::string& _client_key) {
    client_key = _client_key;
  }
};


class RGWHTTPHeadersCollector : public RGWHTTPClient {
public:
  typedef std::string header_name_t;
  typedef std::string header_value_t;
  typedef std::set<header_name_t, ltstr_nocase> header_spec_t;

  RGWHTTPHeadersCollector(CephContext * const cct,
                          const std::string& method,
                          const std::string& url,
                          const header_spec_t &relevant_headers)
    : RGWHTTPClient(cct, method, url),
      relevant_headers(relevant_headers) {
  }

  std::map<header_name_t, header_value_t, ltstr_nocase> get_headers() const {
    return found_headers;
  }

  /* Throws std::out_of_range */
  const header_value_t& get_header_value(const header_name_t& name) const {
    return found_headers.at(name);
  }

protected:
  int receive_header(void *ptr, size_t len) override;

private:
  const std::set<header_name_t, ltstr_nocase> relevant_headers;
  std::map<header_name_t, header_value_t, ltstr_nocase> found_headers;
};


class RGWHTTPTransceiver : public RGWHTTPHeadersCollector {
  bufferlist * const read_bl;
  std::string post_data;
  size_t post_data_index;

public:
  RGWHTTPTransceiver(CephContext * const cct,
                     const std::string& method,
                     const std::string& url,
                     bufferlist * const read_bl,
                     const header_spec_t intercept_headers = {})
    : RGWHTTPHeadersCollector(cct, method, url, intercept_headers),
      read_bl(read_bl),
      post_data_index(0) {
  }

  RGWHTTPTransceiver(CephContext * const cct,
                     const std::string& method,
                     const std::string& url,
                     bufferlist * const read_bl,
                     const bool verify_ssl,
                     const header_spec_t intercept_headers = {})
    : RGWHTTPHeadersCollector(cct, method, url, intercept_headers),
      read_bl(read_bl),
      post_data_index(0) {
    set_verify_ssl(verify_ssl);
  }

  void set_post_data(const std::string& _post_data) {
    this->post_data = _post_data;
  }

protected:
  int send_data(void* ptr, size_t len, bool *pause=nullptr) override;

  int receive_data(void *ptr, size_t len, bool *pause) override {
    read_bl->append((char *)ptr, len);
    return 0;
  }
};

typedef RGWHTTPTransceiver RGWPostHTTPData;


class RGWCompletionManager;

enum RGWHTTPRequestSetState {
  SET_NOP = 0,
  SET_WRITE_PAUSED = 1,
  SET_WRITE_RESUME = 2,
  SET_READ_PAUSED  = 3,
  SET_READ_RESUME  = 4,
};

class RGWHTTPManager {
  struct set_state {
    rgw_http_req_data *req;
    int bitmask;

    set_state(rgw_http_req_data *_req, int _bitmask) : req(_req), bitmask(_bitmask) {}
  };
  CephContext *cct;
  RGWCompletionManager *completion_mgr;
  void *multi_handle;
  bool is_started = false;
  std::atomic<unsigned> going_down { 0 };
  std::atomic<unsigned> is_stopped { 0 };

  ceph::shared_mutex reqs_lock = ceph::make_shared_mutex("RGWHTTPManager::reqs_lock");
  std::map<uint64_t, rgw_http_req_data *> reqs;
  std::list<rgw_http_req_data *> unregistered_reqs;
  std::list<set_state> reqs_change_state;
  std::map<uint64_t, rgw_http_req_data *> complete_reqs;
  int64_t num_reqs = 0;
  int64_t max_threaded_req = 0;
  int thread_pipe[2];

  void register_request(rgw_http_req_data *req_data);
  void complete_request(rgw_http_req_data *req_data);
  void _complete_request(rgw_http_req_data *req_data);
  bool unregister_request(rgw_http_req_data *req_data);
  void _unlink_request(rgw_http_req_data *req_data);
  void unlink_request(rgw_http_req_data *req_data);
  void finish_request(rgw_http_req_data *req_data, int r, long http_status = -1);
  void _finish_request(rgw_http_req_data *req_data, int r);
  void _set_req_state(set_state& ss);
  int link_request(rgw_http_req_data *req_data);

  void manage_pending_requests();

  class ReqsThread : public Thread {
    RGWHTTPManager *manager;

  public:
    explicit ReqsThread(RGWHTTPManager *_m) : manager(_m) {}
    void *entry() override;
  };

  ReqsThread *reqs_thread = nullptr;

  void *reqs_thread_entry();

  int signal_thread();

public:
  RGWHTTPManager(CephContext *_cct, RGWCompletionManager *completion_mgr = NULL);
  ~RGWHTTPManager();

  int start();
  void stop();

  int add_request(RGWHTTPClient *client);
  int remove_request(RGWHTTPClient *client);
  int set_request_state(RGWHTTPClient *client, RGWHTTPRequestSetState state);
};

class RGWHTTP
{
public:
  static int send(RGWHTTPClient *req);
  static int process(RGWHTTPClient *req, optional_yield y);
};
