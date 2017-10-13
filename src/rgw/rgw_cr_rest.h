#ifndef CEPH_RGW_CR_REST_H
#define CEPH_RGW_CR_REST_H

#include <boost/intrusive_ptr.hpp>
#include "include/assert.h" // boost header clobbers our assert.h

#include "rgw_coroutine.h"
#include "rgw_rest_conn.h"


struct rgw_rest_obj {
  rgw_obj_key key;
  uint64_t content_len;
  std::map<string, string> attrs;
  std::map<string, string> custom_attrs;
  RGWAccessControlPolicy acls;
};

class RGWReadRawRESTResourceCR : public RGWSimpleCoroutine {
  bufferlist *result;
 protected:
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string path;
  param_vec_t params;
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


  ~RGWReadRawRESTResourceCR() override {
    request_cleanup();
  }

  int send_request() override {
    auto op = boost::intrusive_ptr<RGWRESTReadResource>(
        new RGWRESTReadResource(conn, path, params, NULL, http_manager));

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
    return http_op->wait(result);
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

  int wait_result() override {
    return http_op->wait(result);
  }

};

template <class T>
class RGWSendRawRESTResourceCR: public RGWSimpleCoroutine {
 protected:
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string method;
  string path;
  param_vec_t params;
  T *result;
  bufferlist input_bl;
  bool send_content_length=false;
  boost::intrusive_ptr<RGWRESTSendResource> http_op;

 public:
 RGWSendRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                          RGWHTTPManager *_http_manager,
                          const string& _method, const string& _path,
                          rgw_http_param_pair *_params, bufferlist& _input, T *_result, bool _send_content_length)
   : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
    method(_method), path(_path), params(make_param_list(_params)), result(_result),
    input_bl(_input), send_content_length(_send_content_length)
    {}

 RGWSendRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                          RGWHTTPManager *_http_manager,
                          const string& _method, const string& _path,
                          rgw_http_param_pair *_params, T *_result)
   : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
    method(_method), path(_path), params(make_param_list(_params)), result(_result)
    {}



  ~RGWSendRawRESTResourceCR() override {
    request_cleanup();
  }

  void set_input_bl(bufferlist bl){
    input_bl = std::move(bl);
  }

  int send_request() override {
    auto op = boost::intrusive_ptr<RGWRESTSendResource>(
        new RGWRESTSendResource(conn, method, path, params, nullptr, http_manager));

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
    if (result) {
      ret = http_op->wait(result);
    } else {
      bufferlist bl;
      ret = http_op->wait(&bl);
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

template <class S, class T>
class RGWSendRESTResourceCR : public RGWSendRawRESTResourceCR<T> {
 public:
  RGWSendRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                           RGWHTTPManager *_http_manager,
                           const string& _method, const string& _path,
                        rgw_http_param_pair *_params,S& _input, T *_result)
    : RGWSendRawRESTResourceCR<T>(_cct, _conn, _http_manager, _method, _path, _params, _result){

    JSONFormatter jf;
    encode_json("data", _input, &jf);
    std::stringstream ss;
    jf.flush(ss);
    //bufferlist bl;
    this->input_bl.append(ss.str());
    //set_input_bl(std::move(bl));
  }

};

template <class S, class T>
class RGWPostRESTResourceCR : public RGWSendRESTResourceCR<S, T> {
public:
  RGWPostRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                        RGWHTTPManager *_http_manager,
                        const string& _path,
                        rgw_http_param_pair *_params, S& _input, T *_result)
    : RGWSendRESTResourceCR<S, T>(_cct, _conn, _http_manager,
                            "POST", _path,
                            _params, _input, _result) {}
};

template <class T>
class RGWPutRawRESTResourceCR: public RGWSendRawRESTResourceCR <T> {
 public:
  RGWPutRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                          RGWHTTPManager *_http_manager,
                          const string& _path,
                          rgw_http_param_pair *_params, bufferlist& _input, T *_result)
    : RGWSendRawRESTResourceCR<T>(_cct, _conn, _http_manager, "PUT", _path, _params, _input, _result, true){}

};

template <class T>
class RGWPostRawRESTResourceCR: public RGWSendRawRESTResourceCR <T> {
 public:
  RGWPostRawRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                          RGWHTTPManager *_http_manager,
                          const string& _path,
                          rgw_http_param_pair *_params, bufferlist& _input, T *_result)
    : RGWSendRawRESTResourceCR<T>(_cct, _conn, _http_manager, "POST", _path, _params, _input, _result, true){}

};


template <class S, class T>
class RGWPutRESTResourceCR : public RGWSendRESTResourceCR<S, T> {
public:
  RGWPutRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                        RGWHTTPManager *_http_manager,
                        const string& _path,
                        rgw_http_param_pair *_params, S& _input, T *_result)
    : RGWSendRESTResourceCR<S, T>(_cct, _conn, _http_manager,
                                  "PUT", _path,
                                  _params, _input, _result) {}
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
    ret = http_op->wait(&bl);
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

class RGWCRHTTPGetDataCB;

class RGWStreamReadResourceCRF {
protected:
  boost::asio::coroutine read_state;

public:
  virtual int init() = 0;
  virtual int read(bufferlist *data, uint64_t max, bool *need_retry) = 0; /* reentrant */
  virtual int decode_rest_obj(map<string, string>& headers, bufferlist& extra_data, rgw_rest_obj *info) = 0;
  virtual bool has_attrs() = 0;
  virtual void get_attrs(std::map<string, string> *attrs) = 0;
};

class RGWStreamWriteResourceCRF {
protected:
  boost::asio::coroutine write_state;
  boost::asio::coroutine drain_state;

public:
  virtual int init() = 0;
  virtual void send_ready(const rgw_rest_obj& rest_obj) = 0;
  virtual int send() = 0;
  virtual int write(bufferlist& data) = 0; /* reentrant */
  virtual int drain_writes(bool *need_retry) = 0; /* reentrant */
};

class RGWStreamReadHTTPResourceCRF : public RGWStreamReadResourceCRF {
  CephContext *cct;
  RGWCoroutinesEnv *env;
  RGWCoroutine *caller;
  RGWHTTPManager *http_manager;

  RGWHTTPStreamRWRequest *req{nullptr};

  RGWCRHTTPGetDataCB *in_cb{nullptr};

  bufferlist extra_data;

  bool got_attrs{false};
  bool got_extra_data{false};

protected:
  rgw_rest_obj rest_obj;

public:
  RGWStreamReadHTTPResourceCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWHTTPManager *_http_manager) : cct(_cct),
                                                                env(_env),
                                                                caller(_caller),
                                                                http_manager(_http_manager) {}
  virtual ~RGWStreamReadHTTPResourceCRF();

  int init() override;
  int read(bufferlist *data, uint64_t max, bool *need_retry) override; /* reentrant */
  int decode_rest_obj(map<string, string>& headers, bufferlist& extra_data, rgw_rest_obj *info);
  bool has_attrs() override;
  void get_attrs(std::map<string, string> *attrs);
  bool is_done();
  virtual bool need_extra_data() { return false; }

  void set_req(RGWHTTPStreamRWRequest *r) {
    req = r;
  }

  rgw_rest_obj& get_rest_obj() {
    return rest_obj;
  }
};

class RGWStreamWriteHTTPResourceCRF : public RGWStreamWriteResourceCRF {
protected:
  RGWCoroutinesEnv *env;
  RGWCoroutine *caller;
  RGWHTTPManager *http_manager;

  RGWHTTPStreamRWRequest *req{nullptr};

public:
  RGWStreamWriteHTTPResourceCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWHTTPManager *_http_manager) : env(_env),
                                                               caller(_caller),
                                                               http_manager(_http_manager) {}
  virtual ~RGWStreamWriteHTTPResourceCRF() {}

  int init() override {
    return 0;
  }
  void send_ready(const rgw_rest_obj& rest_obj) override;
  int send() override;
  int write(bufferlist& data) override; /* reentrant */
  int drain_writes(bool *need_retry) override; /* reentrant */

  void set_req(RGWHTTPStreamRWRequest *r) {
    req = r;
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

  int operate();
};

#endif
