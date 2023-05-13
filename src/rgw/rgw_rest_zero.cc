// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rest_zero.h"

#include <mutex>
#include <vector>
#include "common/strtol.h"

namespace rgw {

// all paths refer to a single resource that contains only a size
class ZeroResource {
 public:
  std::mutex mutex;
  std::size_t size = 0;
};

// base op
class ZeroOp : public RGWOp {
 protected:
  ZeroResource* const resource;
  const char* response_content_type = nullptr;
  int64_t response_content_length = NO_CONTENT_LENGTH;
 public:
  explicit ZeroOp(ZeroResource* resource) : resource(resource) {}
  int verify_permission(optional_yield y) override { return 0; }
  void send_response() override;
};

void ZeroOp::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, response_content_type, response_content_length);
}


// DELETE op resets resource size to 0
class ZeroDeleteOp : public ZeroOp {
 public:
  explicit ZeroDeleteOp(ZeroResource* resource) : ZeroOp(resource) {}
  const char* name() const override { return "zero_delete"; }
  void execute(optional_yield y) override;
};

void ZeroDeleteOp::execute(optional_yield y)
{
  auto lock = std::scoped_lock(resource->mutex);
  resource->size = 0;
}


// GET op returns a request body of all zeroes
class ZeroGetOp : public ZeroOp {
 public:
  explicit ZeroGetOp(ZeroResource* resource) : ZeroOp(resource) {}
  const char* name() const override { return "zero_get"; }
  void execute(optional_yield y) override;
  void send_response() override;
};

void ZeroGetOp::execute(optional_yield y)
{
  response_content_type = "application/octet-stream";

  auto lock = std::scoped_lock(resource->mutex);
  response_content_length = resource->size;
}

void ZeroGetOp::send_response()
{
  // send the response header
  ZeroOp::send_response();

  // write zeroes for the entire response body
  size_t remaining = static_cast<size_t>(response_content_length);
  const size_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
  std::vector<char> zeroes;
  zeroes.resize(std::min(remaining, chunk_size), '\0');

  try {
    while (remaining) {
      const size_t count = std::min(zeroes.size(), remaining);
      const int bytes = dump_body(s, zeroes.data(), count);
      remaining -= bytes;
    }
  } catch (const std::exception& e) {
    ldpp_dout(this, 0) << "recv_body failed with " << e.what() << dendl;
    op_ret = -EIO;
    return;
  }
}


// HEAD op returns the current content length
class ZeroHeadOp : public ZeroOp {
 public:
  explicit ZeroHeadOp(ZeroResource* resource) : ZeroOp(resource) {}
  const char* name() const override { return "zero_head"; }
  void execute(optional_yield y) override;
};

void ZeroHeadOp::execute(optional_yield y)
{
  response_content_type = "application/octet-stream";

  auto lock = std::scoped_lock(resource->mutex);
  response_content_length = resource->size;
}


// PUT op discards the entire request body then updates the content length
class ZeroPutOp : public ZeroOp {
 public:
  explicit ZeroPutOp(ZeroResource* resource) : ZeroOp(resource) {}
  const char* name() const override { return "zero_put"; }
  void execute(optional_yield y) override;
};

void ZeroPutOp::execute(optional_yield y)
{
  if (!s->length) {
    ldpp_dout(this, 0) << "missing content length" << dendl;
    op_ret = -ERR_LENGTH_REQUIRED;
    return;
  }
  const auto content_length = ceph::parse<size_t>(s->length);
  if (!content_length) {
    ldpp_dout(this, 0) << "failed to parse content length \""
        << s->length << '"' << dendl;
    op_ret = -EINVAL;
    return;
  }

  // read and discard the entire request body
  size_t remaining = *content_length;
  const size_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
  std::vector<char> buffer;
  buffer.resize(std::min(remaining, chunk_size));

  try {
    while (remaining) {
      const size_t count = std::min(buffer.size(), remaining);
      const int bytes = recv_body(s, buffer.data(), count);
      remaining -= bytes;
    }
  } catch (const std::exception& e) {
    ldpp_dout(this, 0) << "recv_body failed with " << e.what() << dendl;
    op_ret = -EIO;
    return;
  }

  // on success, update the resource size
  auto lock = std::scoped_lock(resource->mutex);
  resource->size = *content_length;
}


class ZeroHandler : public RGWHandler_REST {
  ZeroResource* const resource;
 public:
  explicit ZeroHandler(ZeroResource* resource) : resource(resource) {}

  int init_permissions(RGWOp*, optional_yield) override { return 0; }
  int read_permissions(RGWOp*, optional_yield) override { return 0; }
  int authorize(const DoutPrefixProvider* dpp, optional_yield y) override { return 0; }
  int postauth_init(optional_yield y) override { return 0; }

  // op factory functions
  RGWOp* op_delete() override { return new ZeroDeleteOp(resource); }
  RGWOp* op_get() override { return new ZeroGetOp(resource); }
  RGWOp* op_head() override { return new ZeroHeadOp(resource); }
  RGWOp* op_put() override { return new ZeroPutOp(resource); }
};

RESTMgr_Zero::RESTMgr_Zero() : resource(std::make_unique<ZeroResource>()) {}

RGWHandler_REST* RESTMgr_Zero::get_handler(sal::Driver* driver, req_state* s,
                                           const auth::StrategyRegistry& auth,
                                           const std::string& prefix)
{
  return new ZeroHandler(resource.get());
}

} // namespace rgw
