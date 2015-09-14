#ifndef CEPH_RGW_CR_REST_H
#define CEPH_RGW_CR_REST_H

#include "rgw_coroutine.h"

template <class T>
class RGWReadRESTResourceCR : public RGWSimpleCoroutine {
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string path;
  rgw_http_param_pair *params;
  T *result;

  RGWRESTReadResource *http_op;

public:
  RGWReadRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn, RGWHTTPManager *_http_manager,
			const string& _path, rgw_http_param_pair *_params,
			T *_result) : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
                                      path(_path), params(_params), result(_result), http_op(NULL) {}

  int send_request() {
    http_op = new RGWRESTReadResource(conn, path, params, NULL, http_manager);

    http_op->set_user_info((void *)stack);

    int ret = http_op->aio_read();
    if (ret < 0) {
      log_error() << "failed to send http operation: " << http_op->to_str() << " ret=" << ret << std::endl;
      http_op->put();
      return ret;
    }
    return 0;
  }

  int request_complete() {
    int ret = http_op->wait(result);
    http_op->put();
    if (ret < 0) {
      error_stream << "http operation failed: " << http_op->to_str() << " status=" << http_op->get_http_status() << std::endl;
      return ret;
    }
    return 0;
  }
};


#endif
