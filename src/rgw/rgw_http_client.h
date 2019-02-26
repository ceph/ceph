// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_HTTP_CLIENT_H
#define CEPH_RGW_HTTP_CLIENT_H

#include "common/RWLock.h"
#include "common/Cond.h"
#include "rgw_common.h"
#include "rgw_string.h"

#include <atomic>

using param_pair_t = pair<string, string>;
using param_vec_t = vector<param_pair_t>;

void rgw_http_client_init(CephContext *cct);
void rgw_http_client_cleanup();

struct rgw_http_req_data;
class RGWHTTPManager;

class RGWIOIDProvider
{
  std::atomic<int64_t> max = {0};

public:
  RGWIOIDProvider() {}
  int64_t get_next() {
    return ++max;
  }
};

struct rgw_io_id {
  int64_t id{0};
  int channels{0};

  rgw_io_id() {}
  rgw_io_id(int64_t _id, int _channels) : id(_id), channels(_channels) {}

  bool intersects(const rgw_io_id& rhs) {
    return (id == rhs.id && ((channels | rhs.channels) != 0));
  }

  bool operator<(const rgw_io_id& rhs) const {
    if (id < rhs.id) {
      return true;
    }
    return (id == rhs.id &&
            channels < rhs.channels);
  }
};

class RGWIOProvider
{
  int64_t id{-1};

public:
  RGWIOProvider() {}
  virtual ~RGWIOProvider() = default; 

  void assign_io(RGWIOIDProvider& io_id_provider, int io_type = -1);
  rgw_io_id get_io_id(int io_type) {
    return rgw_io_id{id, io_type};
  }

  virtual void set_io_user_info(void *_user_info) = 0;
  virtual void *get_io_user_info() = 0;
};

class RGWHTTPClient : public RGWIOProvider
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

  std::atomic<unsigned> stopped { 0 };


protected:
  CephContext *cct;

  string method;
  string url;

  size_t send_len{0};

  param_vec_t headers;

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

  Mutex& get_req_lock();

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
                         const string& _method,
                         const string& _url)
    : has_send_len(false),
      http_status(HTTP_STATUS_NOSTATUS),
      req_data(nullptr),
      verify_ssl(cct->_conf->rgw_verify_ssl),
      cct(cct),
      method(_method),
      url(_url) {
  }

  void append_header(const string& name, const string& val) {
    headers.push_back(pair<string, string>(name, val));
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

  void set_verify_ssl(bool flag) {
    verify_ssl = flag;
  }

  int process();

  int wait();
  void cancel();
  bool is_done();

  rgw_http_req_data *get_req_data() { return req_data; }

  string to_str();

  int get_req_retcode();

  void set_url(const string& _url) {
    url = _url;
  }

  void set_method(const string& _method) {
    method = _method;
  }

  void set_io_user_info(void *_user_info) override {
    user_info = _user_info;
  }

  void *get_io_user_info() override {
    return user_info;
  }
};


class RGWHTTPHeadersCollector : public RGWHTTPClient {
public:
  typedef std::string header_name_t;
  typedef std::string header_value_t;
  typedef std::set<header_name_t, ltstr_nocase> header_spec_t;

  RGWHTTPHeadersCollector(CephContext * const cct,
                          const string& method,
                          const string& url,
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
                     const string& method,
                     const string& url,
                     bufferlist * const read_bl,
                     const header_spec_t intercept_headers = {})
    : RGWHTTPHeadersCollector(cct, method, url, intercept_headers),
      read_bl(read_bl),
      post_data_index(0) {
  }

  RGWHTTPTransceiver(CephContext * const cct,
                     const string& method,
                     const string& url,
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
  bool is_started;
  std::atomic<unsigned> going_down { 0 };
  std::atomic<unsigned> is_stopped { 0 };

  RWLock reqs_lock;
  map<uint64_t, rgw_http_req_data *> reqs;
  list<rgw_http_req_data *> unregistered_reqs;
  list<set_state> reqs_change_state;
  map<uint64_t, rgw_http_req_data *> complete_reqs;
  int64_t num_reqs;
  int64_t max_threaded_req;
  int thread_pipe[2];

  void register_request(rgw_http_req_data *req_data);
  void complete_request(rgw_http_req_data *req_data);
  void _complete_request(rgw_http_req_data *req_data);
  bool unregister_request(rgw_http_req_data *req_data);
  void _unlink_request(rgw_http_req_data *req_data);
  void unlink_request(rgw_http_req_data *req_data);
  void finish_request(rgw_http_req_data *req_data, int r);
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

  ReqsThread *reqs_thread;

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
  static int process(RGWHTTPClient *req);
};
#endif
