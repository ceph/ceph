// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_REQUEST_H
#define RGW_REQUEST_H

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_fcgi.h"

#include "common/QueueRing.h"

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

struct RGWFCGXRequest : public RGWRequest {
  FCGX_Request *fcgx;
  QueueRing<FCGX_Request *> *qr;

  RGWFCGXRequest(uint64_t req_id, QueueRing<FCGX_Request *> *_qr)
	  : RGWRequest(req_id), qr(_qr) {
    qr->dequeue(&fcgx);
  }

  ~RGWFCGXRequest() {
    FCGX_Finish_r(fcgx);
    qr->enqueue(fcgx);
  }
};

struct RGWLoadGenRequest : public RGWRequest {
	string method;
	string resource;
	int content_length;
	atomic_t* fail_flag;

RGWLoadGenRequest(uint64_t req_id, const string& _m, const  string& _r, int _cl,
		atomic_t *ff)
	: RGWRequest(req_id), method(_m), resource(_r), content_length(_cl),
		fail_flag(ff) {}
};

#endif /* RGW_REQUEST_H */
