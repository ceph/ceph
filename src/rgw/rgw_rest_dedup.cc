// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_rest_dedup.h"
#include "rgw_op.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "rgw_dedup_cluster.h"
#include "rgw_dedup_utils.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static rgw::sal::RadosStore* get_rados_store(rgw::sal::Driver* driver)
{
  return dynamic_cast<rgw::sal::RadosStore*>(driver);
}

// GET /admin/dedup?op=stats
class RGWOp_Dedup_Stats : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("dedup", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    auto store = get_rados_store(driver);
    if (!store) {
      op_ret = -EPERM;
      return;
    }

    // Send HTTP headers and reset formatter before writing the stats body;
    // send_response() will only flush the body (did_start() is true).
    flusher.start(0);
    op_ret = rgw::dedup::cluster::collect_all_shard_stats(
      store, s->formatter, this);
  }

  const char* name() const override { return "get_dedup_stats"; }
};

// GET /admin/dedup?op=throttle
class RGWOp_Dedup_Throttle_Get : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("dedup", RGW_CAP_READ);
  }

  void execute(optional_yield y) override {
    using namespace rgw::dedup;
    auto store = get_rados_store(driver);
    if (!store) {
      op_ret = -EPERM;
      return;
    }

    bufferlist urgent_msg_bl;
    urgent_msg_t urgent_msg = URGENT_MSG_THROTTLE;
    ceph::encode(urgent_msg, urgent_msg_bl);
    throttle_msg_t throttle_msg;
    encode(throttle_msg, urgent_msg_bl);

    // Same as stats: headers first, then throttle JSON from dedup_control_bl().
    flusher.start(0);
    op_ret = cluster::dedup_control_bl(store, this, urgent_msg, urgent_msg_bl,
                                        s->formatter, y);
  }

  const char* name() const override { return "get_dedup_throttle"; }
};

// POST /admin/dedup?op=estimate|exec
class RGWOp_Dedup_Scan : public RGWRESTOp {
  rgw::dedup::dedup_req_type_t dedup_type;
public:
  RGWOp_Dedup_Scan(rgw::dedup::dedup_req_type_t dedup_type)
    : dedup_type(dedup_type) {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("dedup", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    auto store = get_rados_store(driver);
    if (!store) {
      op_ret = -EPERM;
      return;
    }

    if (dedup_type == rgw::dedup::dedup_req_type_t::DEDUP_TYPE_EXEC) {
      bool confirmed = false;
      RESTArgs::get_bool(s, "yes-i-really-mean-it", false, &confirmed);
      if (!confirmed) {
        op_ret = -EINVAL;
        return;
      }
#ifndef FULL_DEDUP_SUPPORT
      op_ret = -EPERM;
      return;
#endif
    }

    op_ret = rgw::dedup::cluster::dedup_restart_scan(store, dedup_type, this,
                                                    nullptr, y);
  }

  const char* name() const override {
    return dedup_type == rgw::dedup::dedup_req_type_t::DEDUP_TYPE_EXEC
      ? "run dedup_exec" : "run dedup_estimate";
  }
};

// POST /admin/dedup?op=abort|pause|resume
class RGWOp_Dedup_Control : public RGWRESTOp {
  rgw::dedup::urgent_msg_t msg;
public:
  RGWOp_Dedup_Control(rgw::dedup::urgent_msg_t msg) : msg(msg) {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("dedup", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    auto store = get_rados_store(driver);
    if (!store) {
      op_ret = -EPERM;
      return;
    }

    op_ret = rgw::dedup::cluster::dedup_control(store, this, msg, y);
  }

  const char* name() const override {
    switch (msg) {
    case rgw::dedup::URGENT_MSG_ABORT:  return "abort dedup";
    case rgw::dedup::URGENT_MSG_PASUE:  return "pause dedup";
    case rgw::dedup::URGENT_MSG_RESUME: return "resume dedup";
    default:                            return "dedup control";
    }
  }
};

// POST /admin/dedup?op=throttle&max-bucket-index-ops=N&max-metadata-ops=M
class RGWOp_Dedup_Throttle_Set : public RGWRESTOp {
public:
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("dedup", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override {
    using namespace rgw::dedup;
    auto store = get_rados_store(driver);
    if (!store) {
      op_ret = -EPERM;
      return;
    }

    bufferlist urgent_msg_bl;
    urgent_msg_t urgent_msg = URGENT_MSG_THROTTLE;
    ceph::encode(urgent_msg, urgent_msg_bl);
    throttle_msg_t throttle_msg;
    string err;

    auto parse_limit = [&](const char* param, op_type_t op_type) {
      string val;
      RESTArgs::get_string(s, param, "", &val);
      if (!val.empty()) {
        int64_t limit = strict_strtoll(val.c_str(), 10, &err);
        if (!err.empty()) {
          return -EINVAL;
        }
        throttle_msg.vec.push_back({ .op_type = op_type,
                                     .limit = (uint32_t)limit });
      }
      return 0;
    };

    op_ret = parse_limit("max-bucket-index-ops", BUCKET_INDEX_OP);
    if (op_ret) return;
    op_ret = parse_limit("max-metadata-ops", METADATA_ACCESS_OP);
    if (op_ret) return;

    if (throttle_msg.vec.empty()) {
      op_ret = -EINVAL;
      return;
    }

    // After validation: headers first, then JSON echo of applied throttle state.
    flusher.start(0);
    encode(throttle_msg, urgent_msg_bl);
    op_ret = cluster::dedup_control_bl(store, this, urgent_msg, urgent_msg_bl,
                                        s->formatter, y);
  }

  const char* name() const override { return "set_dedup_throttle"; }
};

RGWOp *RGWHandler_Dedup::op_get()
{
  string op;
  RESTArgs::get_string(s, "op", "", &op);

  if (op == "stats") {
    return new RGWOp_Dedup_Stats;
  }
  if (op == "throttle") {
    return new RGWOp_Dedup_Throttle_Get;
  }

  return nullptr;
}

RGWOp *RGWHandler_Dedup::op_post()
{
  string op;
  RESTArgs::get_string(s, "op", "", &op);

  if (op == "estimate") {
    return new RGWOp_Dedup_Scan(rgw::dedup::dedup_req_type_t::DEDUP_TYPE_ESTIMATE);
  }
  if (op == "exec") {
    return new RGWOp_Dedup_Scan(rgw::dedup::dedup_req_type_t::DEDUP_TYPE_EXEC);
  }
  if (op == "abort") {
    return new RGWOp_Dedup_Control(rgw::dedup::URGENT_MSG_ABORT);
  }
  if (op == "pause") {
    return new RGWOp_Dedup_Control(rgw::dedup::URGENT_MSG_PASUE);
  }
  if (op == "resume") {
    return new RGWOp_Dedup_Control(rgw::dedup::URGENT_MSG_RESUME);
  }
  if (op == "throttle") {
    return new RGWOp_Dedup_Throttle_Set;
  }

  return nullptr;
}
