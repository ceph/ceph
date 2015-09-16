#ifndef CEPH_RGW_CR_REST_H
#define CEPH_RGW_CR_REST_H

#include "rgw_coroutine.h"

#include <list>

template <class T>
class RGWReadRESTResourceCR : public RGWSimpleCoroutine {
  RGWRESTConn *conn;
  RGWHTTPManager *http_manager;
  string path;
  std::list<pair<string, string> > params_list;
  T *result;

  RGWRESTReadResource *http_op;

public:
  RGWReadRESTResourceCR(CephContext *_cct, RGWRESTConn *_conn, RGWHTTPManager *_http_manager,
			const string& _path, rgw_http_param_pair *params,
			T *_result) : RGWSimpleCoroutine(_cct), conn(_conn), http_manager(_http_manager),
                                      path(_path), result(_result), http_op(NULL) {
     rgw_http_param_pair *pp = params;
     while (pp && pp->key) {
      string k = pp->key;
      string v = (pp->val ? pp->val : "");
      params_list.push_back(make_pair(k, v));
      ++pp;
    }
  }

  int send_request() {
    http_op = new RGWRESTReadResource(conn, path, params_list, NULL, http_manager);

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
    if (ret < 0) {
      error_stream << "http operation failed: " << http_op->to_str() << " status=" << http_op->get_http_status() << std::endl;
      http_op->put();
      return ret;
    }
    http_op->put();
    return 0;
  }
};


#endif
