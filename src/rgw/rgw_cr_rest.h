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
  bool raw; // raw result, T should be a bufferlist
  bufferlist* result_bl;
  boost::intrusive_ptr<RGWRESTReadResource> http_op;

public:
  RGWReadRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                        RGWHTTPManager *_http_manager, const string& _path,
                        rgw_http_param_pair *params, T *_result)
    : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
    path(_path), params(make_param_list(params)), result(_result), raw(false)
  {}

 RGWReadRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn,
                       RGWHTTPManager *_http_manager, const string& _path,
                       rgw_http_param_pair *params, T *_result, bool _raw, bufferlist* _result_bl)
   : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
    path(_path), params(make_param_list(params)), result(_result), raw(_raw), result_bl(_result_bl)
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
    int ret;
    if (!raw)
      ret = http_op->wait(result);
    else{
      // forgive me
      ret = http_op->wait_bl(result_bl);
      //bl->encode(result);
    }


    auto op = std::move(http_op); // release ref on return
    if (ret < 0) {
      error_stream << "http operation failed: " << op->to_str()
                   << " status=" << op->get_http_status() << std::endl;
      op->put();
      return ret;
    }
    op->put();
    return 0;

  void request_cleanup() override {
    if (http_op) {
      http_op->put();
      http_op = NULL;
    }
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
    param_vec_t p;
    if (send_content_length){
      lsubdout(cct, rgw, 0) << "abhi: sending content length of " << input_bl.length() << dendl;
      string content_length = to_string(input_bl.length());
      p.push_back(param_pair_t("CONTENT_LENGTH",content_length));
    }

    auto op = boost::intrusive_ptr<RGWRESTSendResource>(
        new RGWRESTSendResource(conn, method, path, params, &p, http_manager));

    op->set_user_info((void *)stack);

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
