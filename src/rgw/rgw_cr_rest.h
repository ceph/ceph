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

  ~RGWReadRESTResourceCR() {
    request_cleanup();
  }

  int send_request() {
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

  int request_complete() {
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

  void request_cleanup() {
    if (http_op) {
      http_op->put();
      http_op = NULL;
    }
  }
};

template <class S, class T>
class RGWPostRESTResourceCR : public RGWSimpleCoroutine {
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string path;
  param_vec_t params;
  T *result;
  S input;

  boost::intrusive_ptr<RGWRESTPostResource> http_op;

public:
  RGWPostRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                        RGWHTTPManager *_http_manager, const string& _path,
                        rgw_http_param_pair *_params, S& _input, T *_result)
    : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
      path(_path), params(make_param_list(_params)), result(_result),
      input(_input)
  {}

  ~RGWPostRESTResourceCR() {
    request_cleanup();
  }

  int send_request() {
    auto op = boost::intrusive_ptr<RGWRESTPostResource>(
        new RGWRESTPostResource(conn, path, params, NULL, http_manager));

    op->set_user_info((void *)stack);

    JSONFormatter jf;
    encode_json("data", input, &jf);
    std::stringstream ss;
    jf.flush(ss);
    bufferlist bl;
    bl.append(ss.str());

    int ret = op->aio_send(bl);
    if (ret < 0) {
      lsubdout(cct, rgw, 0) << "ERROR: failed to send post request" << dendl;
      op->put();
      return ret;
    }
    std::swap(http_op, op); // store reference in http_op on success
    return 0;
  }

  int request_complete() {
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
      lsubdout(cct, rgw, 0) << "ERROR: failed to wait for op, ret=" << ret
          << ": " << op->to_str() << dendl;
      op->put();
      return ret;
    }
    op->put();
    return 0;
  }

  void request_cleanup() {
    if (http_op) {
      http_op->put();
      http_op = NULL;
    }
  }
};

#endif
