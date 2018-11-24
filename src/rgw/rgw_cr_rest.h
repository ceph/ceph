// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_CR_REST_H
#define CEPH_RGW_CR_REST_H

#include <boost/intrusive_ptr.hpp>
#include <mutex>
#include "include/ceph_assert.h" // boost header clobbers our assert.h

#include "rgw_coroutine.h"
#include "rgw_rest_conn.h"


struct rgw_rest_obj {
  rgw_obj_key key;
  uint64_t content_len;
  std::map<string, string> attrs;
  std::map<string, string> custom_attrs;
  RGWAccessControlPolicy acls;

  void init(const rgw_obj_key& _key) {
    key = _key;
  }
};

class RGWReadRawRESTResourceCR : public RGWSimpleCoroutine {
  bufferlist *result;
 protected:
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string path;
  param_vec_t params;
  param_vec_t extra_headers;
public:
  boost::intrusive_ptr<RGWRESTReadResource> http_op;
  RGWReadRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                           RGWHTTPManager *_http_manager, const string& _path,
                           rgw_http_param_pair *params, bufferlist *_result)
    : RGWSimpleCoroutine(_cct), result(_result), conn(_conn), http_manager(_http_manager),
    path(_path), params(make_param_list(params))
  {}

 RGWReadRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                          RGWHTTPManager *_http_manager, const string& _path,
                          rgw_http_param_pair *params)
   : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
    path(_path), params(make_param_list(params))
  {}

  RGWReadRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                           RGWHTTPManager *_http_manager, const string& _path,
                           rgw_http_param_pair *params, param_vec_t &hdrs)
    : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
      path(_path), params(make_param_list(params)),
      extra_headers(hdrs)
  {}

 RGWReadRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                          RGWHTTPManager *_http_manager, const string& _path,
                          rgw_http_param_pair *params,
                          std::map <std::string, std::string> *hdrs)
   : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
    path(_path), params(make_param_list(params)),
    extra_headers(make_param_list(hdrs))
    {}


  ~RGWReadRawRESTResourceCR() override {
    request_cleanup();
  }

  int send_request() override {
    auto op = boost::intrusive_ptr<RGWRESTReadResource>(
        new RGWRESTReadResource(conn, path, params, &extra_headers, http_manager));

    init_new_io(op.get());

    int ret = op->aio_read();
    if (ret < 0) {
      log_error() << "failed to send http operation: " << op->to_str()
          << " ret=" << ret << std::endl;
      op->put();
      return ret;
    }
    std::swap(http_op, op); // store reference in http_op on success
    return 0;
  }



  virtual int wait_result() {
    return http_op->wait(result, null_yield);
  }

  int request_complete() override {
    int ret;

    ret = wait_result();

    auto op = std::move(http_op); // release ref on return
    if (ret < 0) {
      error_stream << "http operation failed: " << op->to_str()
                   << " status=" << op->get_http_status() << std::endl;
      op->put();
      return ret;
    }
    op->put();
    return 0;
  }

  void request_cleanup() override {
    if (http_op) {
      http_op->put();
      http_op = NULL;
    }
  }

};


template <class T>
class RGWReadRESTResourceCR : public RGWReadRawRESTResourceCR {
  T *result;
 public:
 RGWReadRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                       RGWHTTPManager *_http_manager, const string& _path,
                       rgw_http_param_pair *params, T *_result)
   : RGWReadRawRESTResourceCR(_cct, _conn, _http_manager, _path, params), result(_result)
  {}

  RGWReadRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                        RGWHTTPManager *_http_manager, const string& _path,
                        rgw_http_param_pair *params,
                        std::map <std::string, std::string> *hdrs,
                        T *_result)
    : RGWReadRawRESTResourceCR(_cct, _conn, _http_manager, _path, params, hdrs), result(_result)
  {}

  int wait_result() override {
    return http_op->wait(result, null_yield);
  }

};

template <class T, class E = int>
class RGWSendRawRESTResourceCR: public RGWSimpleCoroutine {
 protected:
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string method;
  string path;
  param_vec_t params;
  param_vec_t headers;
  map<string, string> *attrs;
  T *result;
  E *err_result;
  bufferlist input_bl;
  bool send_content_length=false;
  boost::intrusive_ptr<RGWRESTSendResource> http_op;

 public:
 RGWSendRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                          RGWHTTPManager *_http_manager,
                          const string& _method, const string& _path,
                          rgw_http_param_pair *_params,
                          map<string, string> *_attrs,
                          bufferlist& _input, T *_result,
                          bool _send_content_length,
                          E *_err_result = nullptr)
   : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
     method(_method), path(_path), params(make_param_list(_params)),
     headers(make_param_list(_attrs)), attrs(_attrs),
     result(_result), err_result(_err_result),
     input_bl(_input), send_content_length(_send_content_length) {}

  RGWSendRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                          RGWHTTPManager *_http_manager,
                          const string& _method, const string& _path,
                          rgw_http_param_pair *_params, map<string, string> *_attrs,
                          T *_result, E *_err_result = nullptr)
   : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
    method(_method), path(_path), params(make_param_list(_params)), headers(make_param_list(_attrs)), attrs(_attrs), result(_result),
    err_result(_err_result) {}

  ~RGWSendRawRESTResourceCR() override {
    request_cleanup();
  }

  int send_request() override {
    auto op = boost::intrusive_ptr<RGWRESTSendResource>(
        new RGWRESTSendResource(conn, method, path, params, &headers, http_manager));

    init_new_io(op.get());

    int ret = op->aio_send(input_bl);
    if (ret < 0) {
      lsubdout(cct, rgw, 0) << "ERROR: failed to send request" << dendl;
      op->put();
      return ret;
    }
    std::swap(http_op, op); // store reference in http_op on success
    return 0;
  }

  int request_complete() override {
    int ret;
    if (result || err_result) {
      ret = http_op->wait(result, null_yield, err_result);
    } else {
      bufferlist bl;
      ret = http_op->wait(&bl, null_yield);
    }
    auto op = std::move(http_op); // release ref on return
    if (ret < 0) {
      error_stream << "http operation failed: " << op->to_str()
          << " status=" << op->get_http_status() << std::endl;
      lsubdout(cct, rgw, 5) << "failed to wait for op, ret=" << ret
          << ": " << op->to_str() << dendl;
      op->put();
      return ret;
    }
    op->put();
    return 0;
  }

  void request_cleanup() override {
    if (http_op) {
      http_op->put();
      http_op = NULL;
    }
  }
};

template <class S, class T, class E = int>
class RGWSendRESTResourceCR : public RGWSendRawRESTResourceCR<T, E> {
 public:
  RGWSendRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                           RGWHTTPManager *_http_manager,
                           const string& _method, const string& _path,
                        rgw_http_param_pair *_params, map<string, string> *_attrs,
                        S& _input, T *_result, E *_err_result = nullptr)
    : RGWSendRawRESTResourceCR<T, E>(_cct, _conn, _http_manager, _method, _path, _params, _attrs, _result, _err_result) {

    JSONFormatter jf;
    encode_json("data", _input, &jf);
    std::stringstream ss;
    jf.flush(ss);
    //bufferlist bl;
    this->input_bl.append(ss.str());
  }

};

template <class S, class T, class E = int>
class RGWPostRESTResourceCR : public RGWSendRESTResourceCR<S, T, E> {
public:
  RGWPostRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                        RGWHTTPManager *_http_manager,
                        const string& _path,
                        rgw_http_param_pair *_params, S& _input,
                        T *_result, E *_err_result = nullptr)
    : RGWSendRESTResourceCR<S, T, E>(_cct, _conn, _http_manager,
                            "POST", _path,
                            _params, nullptr, _input,
                            _result, _err_result) {}
};

template <class T, class E = int>
class RGWPutRawRESTResourceCR: public RGWSendRawRESTResourceCR <T, E> {
 public:
  RGWPutRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                          RGWHTTPManager *_http_manager,
                          const string& _path,
                          rgw_http_param_pair *_params, bufferlist& _input,
                          T *_result, E *_err_result = nullptr)
    : RGWSendRawRESTResourceCR<T, E>(_cct, _conn, _http_manager, "PUT", _path,
                                  _params, nullptr, _input, _result, true, _err_result) {}

};

template <class T, class E = int>
class RGWPostRawRESTResourceCR: public RGWSendRawRESTResourceCR <T, E> {
 public:
  RGWPostRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                          RGWHTTPManager *_http_manager,
                          const string& _path,
                          rgw_http_param_pair *_params,
                          map<string, string> * _attrs,
                          bufferlist& _input,
                          T *_result, E *_err_result = nullptr)
    : RGWSendRawRESTResourceCR<T, E>(_cct, _conn, _http_manager, "POST", _path,
                                  _params, _attrs, _input, _result, true, _err_result) {}

};


template <class S, class T, class E = int>
class RGWPutRESTResourceCR : public RGWSendRESTResourceCR<S, T, E> {
public:
  RGWPutRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                        RGWHTTPManager *_http_manager,
                        const string& _path,
                        rgw_http_param_pair *_params, S& _input,
                        T *_result, E *_err_result = nullptr)
    : RGWSendRESTResourceCR<S, T, E>(_cct, _conn, _http_manager,
                                  "PUT", _path,
                                  _params, nullptr, _input,
                                  _result, _err_result) {}

  RGWPutRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                       RGWHTTPManager *_http_manager,
                       const string& _path,
                       rgw_http_param_pair *_params,
                       map <string, string> *_attrs,
                       S& _input, T *_result, E *_err_result = nullptr)
    : RGWSendRESTResourceCR<S, T, E>(_cct, _conn, _http_manager,
                                  "PUT", _path,
                                  _params, _attrs, _input,
                                  _result, _err_result) {}

};

class RGWDeleteRESTResourceCR : public RGWSimpleCoroutine {
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string path;
  param_vec_t params;

  boost::intrusive_ptr<RGWRESTDeleteResource> http_op;

public:
  RGWDeleteRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                        RGWHTTPManager *_http_manager,
                        const string& _path,
                        rgw_http_param_pair *_params)
    : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
      path(_path), params(make_param_list(_params))
  {}

  ~RGWDeleteRESTResourceCR() override {
    request_cleanup();
  }

  int send_request() override {
    auto op = boost::intrusive_ptr<RGWRESTDeleteResource>(
        new RGWRESTDeleteResource(conn, path, params, nullptr, http_manager));

    init_new_io(op.get());

    bufferlist bl;

    int ret = op->aio_send(bl);
    if (ret < 0) {
      lsubdout(cct, rgw, 0) << "ERROR: failed to send DELETE request" << dendl;
      op->put();
      return ret;
    }
    std::swap(http_op, op); // store reference in http_op on success
    return 0;
  }

  int request_complete() override {
    int ret;
    bufferlist bl;
    ret = http_op->wait(&bl, null_yield);
    auto op = std::move(http_op); // release ref on return
    if (ret < 0) {
      error_stream << "http operation failed: " << op->to_str()
          << " status=" << op->get_http_status() << std::endl;
      lsubdout(cct, rgw, 5) << "failed to wait for op, ret=" << ret
          << ": " << op->to_str() << dendl;
      op->put();
      return ret;
    }
    op->put();
    return 0;
  }

  void request_cleanup() override {
    if (http_op) {
      http_op->put();
      http_op = NULL;
    }
  }
};

class RGWCRHTTPGetDataCB : public RGWHTTPStreamRWRequest::ReceiveCB {
  Mutex lock;
  RGWCoroutinesEnv *env;
  RGWCoroutine *cr;
  RGWHTTPStreamRWRequest *req;
  rgw_io_id io_id;
  bufferlist data;
  bufferlist extra_data;
  bool got_all_extra_data{false};
  bool paused{false};
  bool notified{false};
public:
  RGWCRHTTPGetDataCB(RGWCoroutinesEnv *_env, RGWCoroutine *_cr, RGWHTTPStreamRWRequest *_req);

  int handle_data(bufferlist& bl, bool *pause) override;

  void claim_data(bufferlist *dest, uint64_t max);

  bufferlist& get_extra_data() {
    return extra_data;
  }

  bool has_data() {
    return (data.length() > 0);
  }

  bool has_all_extra_data() {
    return got_all_extra_data;
  }
};


class RGWStreamReadResourceCRF {
protected:
  boost::asio::coroutine read_state;

public:
  virtual int init() = 0;
  virtual int read(bufferlist *data, uint64_t max, bool *need_retry) = 0; /* reentrant */
  virtual int decode_rest_obj(map<string, string>& headers, bufferlist& extra_data) = 0;
  virtual bool has_attrs() = 0;
  virtual void get_attrs(std::map<string, string> *attrs) = 0;
  virtual ~RGWStreamReadResourceCRF() = default;
};

class RGWStreamWriteResourceCRF {
protected:
  boost::asio::coroutine write_state;
  boost::asio::coroutine drain_state;

public:
  virtual int init() = 0;
  virtual void send_ready(const rgw_rest_obj& rest_obj) = 0;
  virtual int send() = 0;
  virtual int write(bufferlist& data, bool *need_retry) = 0; /* reentrant */
  virtual int drain_writes(bool *need_retry) = 0; /* reentrant */
  
  virtual ~RGWStreamWriteResourceCRF() = default;
};

class RGWStreamReadHTTPResourceCRF : public RGWStreamReadResourceCRF {
  CephContext *cct;
  RGWCoroutinesEnv *env;
  RGWCoroutine *caller;
  RGWHTTPManager *http_manager;

  RGWHTTPStreamRWRequest *req{nullptr};

  std::optional<RGWCRHTTPGetDataCB> in_cb;

  bufferlist extra_data;

  bool got_attrs{false};
  bool got_extra_data{false};

  rgw_io_id io_read_mask;

protected:
  rgw_rest_obj rest_obj;

  struct range_info {
    bool is_set{false};
    uint64_t ofs;
    uint64_t size;
  } range;

  ceph::real_time mtime;
  string etag;

public:
  RGWStreamReadHTTPResourceCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWHTTPManager *_http_manager,
                               const rgw_obj_key& _src_key) : cct(_cct),
                                                                env(_env),
                                                                caller(_caller),
                                                                http_manager(_http_manager) {
    rest_obj.init(_src_key);
  }
  ~RGWStreamReadHTTPResourceCRF();

  int init() override;
  int read(bufferlist *data, uint64_t max, bool *need_retry) override; /* reentrant */
  int decode_rest_obj(map<string, string>& headers, bufferlist& extra_data) override;
  bool has_attrs() override;
  void get_attrs(std::map<string, string> *attrs) override;
  bool is_done();
  virtual bool need_extra_data() { return false; }

  void set_req(RGWHTTPStreamRWRequest *r) {
    req = r;
  }

  rgw_rest_obj& get_rest_obj() {
    return rest_obj;
  }

  void set_range(uint64_t ofs, uint64_t size) {
    range.is_set = true;
    range.ofs = ofs;
    range.size = size;
  }
};

class RGWStreamWriteHTTPResourceCRF : public RGWStreamWriteResourceCRF {
protected:
  RGWCoroutinesEnv *env;
  RGWCoroutine *caller;
  RGWHTTPManager *http_manager;

  using lock_guard = std::lock_guard<std::mutex>;

  std::mutex blocked_lock;
  bool is_blocked;

  RGWHTTPStreamRWRequest *req{nullptr};

  struct multipart_info {
    bool is_multipart{false};
    string upload_id;
    int part_num{0};
    uint64_t part_size;
  } multipart;

  class WriteDrainNotify : public RGWWriteDrainCB {
    RGWStreamWriteHTTPResourceCRF *crf;
  public:
    explicit WriteDrainNotify(RGWStreamWriteHTTPResourceCRF *_crf) : crf(_crf) {}
    void notify(uint64_t pending_size) override;
  } write_drain_notify_cb;

public:
  RGWStreamWriteHTTPResourceCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWHTTPManager *_http_manager) : env(_env),
                                                               caller(_caller),
                                                               http_manager(_http_manager),
                                                               write_drain_notify_cb(this) {}
  virtual ~RGWStreamWriteHTTPResourceCRF();

  int init() override {
    return 0;
  }
  void send_ready(const rgw_rest_obj& rest_obj) override;
  int send() override;
  int write(bufferlist& data, bool *need_retry) override; /* reentrant */
  void write_drain_notify(uint64_t pending_size);
  int drain_writes(bool *need_retry) override; /* reentrant */

  virtual void handle_headers(const std::map<string, string>& headers) {}

  void set_req(RGWHTTPStreamRWRequest *r) {
    req = r;
  }

  void set_multipart(const string& upload_id, int part_num, uint64_t part_size) {
    multipart.is_multipart = true;
    multipart.upload_id = upload_id;
    multipart.part_num = part_num;
    multipart.part_size = part_size;
  }
};

class RGWStreamSpliceCR : public RGWCoroutine {
  CephContext *cct;
  RGWHTTPManager *http_manager;
  string url;
  std::shared_ptr<RGWStreamReadHTTPResourceCRF> in_crf;
  std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;
  bufferlist bl;
  bool need_retry{false};
  bool sent_attrs{false};
  uint64_t total_read{0};
  int ret{0};
public:
  RGWStreamSpliceCR(CephContext *_cct, RGWHTTPManager *_mgr,
                    std::shared_ptr<RGWStreamReadHTTPResourceCRF>& _in_crf,
                    std::shared_ptr<RGWStreamWriteHTTPResourceCRF>& _out_crf);
  ~RGWStreamSpliceCR();

  int operate() override;
};

#endif
