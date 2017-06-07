// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_REQUEST_H
#define RGW_REQUEST_H

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_op.h"
#if defined(WITH_RADOSGW_FCGI_FRONTEND)
#include "rgw_fcgi.h"
#endif

#include "common/QueueRing.h"

#include <atomic>

struct RGWRequest
{
  uint64_t id;
  struct req_state *s;
  string req_str;
  RGWOp *op;
  utime_t ts;

  explicit RGWRequest(uint64_t id) : id(id), s(NULL), op(NULL) {}

  virtual ~RGWRequest() {}

  void init_state(req_state *_s) {
    s = _s;
  }

  void log_format(struct req_state *s, const char *fmt, ...);
  void log_init();
  void log(struct req_state *s, const char *msg);
}; /* RGWRequest */

#if defined(WITH_RADOSGW_FCGI_FRONTEND)
struct RGWFCGXRequest : public RGWRequest {
  FCGX_Request *fcgx;
  QueueRing<FCGX_Request *> *qr;

  RGWFCGXRequest(uint64_t req_id, QueueRing<FCGX_Request *> *_qr)
	  : RGWRequest(req_id), qr(_qr) {
    qr->dequeue(&fcgx);
  }

  ~RGWFCGXRequest() override {
    FCGX_Finish_r(fcgx);
    qr->enqueue(fcgx);
  }
};
#endif

struct RGWLoadGenRequest : public RGWRequest {
	string method;
	string resource;
	int content_length;
	std::atomic<bool>* fail_flag = nullptr;

RGWLoadGenRequest(uint64_t req_id, const string& _m, const  string& _r, int _cl,
		std::atomic<bool> *ff)
	: RGWRequest(req_id), method(_m), resource(_r), content_length(_cl),
		fail_flag(ff) {}
};

#endif /* RGW_REQUEST_H */
