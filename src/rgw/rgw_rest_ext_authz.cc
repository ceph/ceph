// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rest_ext_authz.h"

#include "rgw_ext_authz.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace {

class RGWOp_ExtAuthz_Get : public RGWRESTOp {
 public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("ext-authz", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    RGWExtAuthzConfig config;
    op_ret = driver->load_ext_authz_config(this, y, config, nullptr);
    if (op_ret < 0) {
      return;
    }
    flusher.start(0);
    s->formatter->open_object_section("ext_authz");
    config.dump(s->formatter, true); // never expose the credential
    s->formatter->close_section();
    flusher.flush();
  }

  const char* name() const override { return "get_ext_authz"; }
};

class RGWOp_ExtAuthz_Set : public RGWRESTOp {
 public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("ext-authz", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    const auto max_size = s->cct->_conf->rgw_max_put_param_size;
    bufferlist body;
    std::tie(op_ret, body) = rgw_rest_read_all_input(s, max_size, false);
    if (op_ret < 0) {
      return;
    }

    // configuration is owned by the metadata master. Forward it there and
    // let metadata sync deliver the result to this zone.
    if (!driver->is_meta_master()) {
      op_ret = rgw_forward_request_to_master(this, *s->penv.site,
                                             s->user->get_id(), &body,
                                             nullptr, s->info, s->err, y);
      if (op_ret < 0) {
        ldpp_dout(this, 0) << "forward_request_to_master returned ret="
            << op_ret << dendl;
      }
      return;
    }

    JSONParser parser;
    if (!parser.parse(body.c_str(), body.length()) || !parser.is_object()) {
      s->err.message = "the body must be a json object";
      op_ret = -EINVAL;
      return;
    }
    RGWExtAuthzConfig config;
    try {
      config.decode_json(&parser);
    } catch (const JSONDecoder::err& e) {
      s->err.message = e.what();
      op_ret = -EINVAL;
      return;
    }
    op_ret = rgw::ext_authz::store_config(this, y, driver, config);
    if (op_ret == -EINVAL) {
      s->err.message = "invalid external authorizer configuration";
    }
  }

  const char* name() const override { return "set_ext_authz"; }
};

class RGWOp_ExtAuthz_Delete : public RGWRESTOp {
 public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("ext-authz", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    if (!driver->is_meta_master()) {
      bufferlist data;
      op_ret = rgw_forward_request_to_master(this, *s->penv.site,
                                             s->user->get_id(), &data,
                                             nullptr, s->info, s->err, y);
      if (op_ret < 0) {
        ldpp_dout(this, 0) << "forward_request_to_master returned ret="
            << op_ret << dendl;
      }
      return;
    }
    op_ret = driver->delete_ext_authz_config(this, y);
  }

  const char* name() const override { return "delete_ext_authz"; }
};

} // anonymous namespace

RGWOp* RGWHandler_ExtAuthz::op_get()
{
  return new RGWOp_ExtAuthz_Get;
}

RGWOp* RGWHandler_ExtAuthz::op_put()
{
  return new RGWOp_ExtAuthz_Set;
}

RGWOp* RGWHandler_ExtAuthz::op_delete()
{
  return new RGWOp_ExtAuthz_Delete;
}
