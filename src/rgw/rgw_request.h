// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_common.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_op.h"

#include "common/QueueRing.h"

#include <atomic>

struct RGWRequest
{
  uint64_t id;
  req_state *s;
  RGWOp *op;

  explicit RGWRequest(uint64_t id) : id(id), s(NULL), op(NULL) {}

  virtual ~RGWRequest() {}

  void init_state(req_state *_s) {
    s = _s;
  }
}; /* RGWRequest */

struct RGWLoadGenRequest : public RGWRequest {
	std::string method;
	std::string resource;
	int content_length;
	std::atomic<bool>* fail_flag = nullptr;

RGWLoadGenRequest(uint64_t req_id, const std::string& _m, const std::string& _r, int _cl,
		std::atomic<bool> *ff)
	: RGWRequest(req_id), method(_m), resource(_r), content_length(_cl),
		fail_flag(ff) {}
};
