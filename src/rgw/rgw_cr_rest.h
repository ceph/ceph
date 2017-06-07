#ifndef CEPH_RGW_CR_REST_H
#define CEPH_RGW_CR_REST_H

#include <boost/intrusive_ptr.hpp>
#include "include/assert.h" // boost header clobbers our assert.h

#include "rgw_coroutine.h"
#include "rgw_rest_conn.h"

template <class T>
class RGWReadRESTResourceCR : public RGWSimpleCoroutine {
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string path;
  param_vec_t params;
  T *result;

  boost::intrusive_ptr<RGWRESTReadResource> http_op;

public:
  RGWReadRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                        RGWHTTPManager *_http_manager, const string& _path,
                        rgw_http_param_pair *params, T *_result)
    : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
      path(_path), params(make_param_list(params)), result(_result)
  {}

  ~RGWReadRESTResourceCR() override {
    request_cleanup();
  }

  int send_request() override {
    auto op = boost::intrusive_ptr<RGWRESTReadResource>(
        new RGWRESTReadResource(conn, path, params, NULL, http_manager));

    op->set_user_info((void *)stack);

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

  int request_complete() override {
    int ret = http_op->wait(result);
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

template <class S, class T>
class RGWSendRESTResourceCR : public RGWSimpleCoroutine {
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string method;
  string path;
  param_vec_t params;
  T *result;
  S input;

  boost::intrusive_ptr<RGWRESTSendResource> http_op;

public:
  RGWSendRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                        RGWHTTPManager *_http_manager,
                        const string& _method, const string& _path,
                        rgw_http_param_pair *_params, S& _input, T *_result)
    : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
      method(_method), path(_path), params(make_param_list(_params)), result(_result),
      input(_input)
  {}

  ~RGWSendRESTResourceCR() override {
    request_cleanup();
  }

  int send_request() override {
    auto op = boost::intrusive_ptr<RGWRESTSendResource>(
        new RGWRESTSendResource(conn, method, path, params, NULL, http_manager));

    op->set_user_info((void *)stack);

    JSONFormatter jf;
    encode_json("data", input, &jf);
    std::stringstream ss;
    jf.flush(ss);
    bufferlist bl;
    bl.append(ss.str());

    int ret = op->aio_send(bl);
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
      ret = http_op->wait_bl(&bl);
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

    op->set_user_info((void *)stack);

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
    ret = http_op->wait_bl(&bl);
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

#endif
