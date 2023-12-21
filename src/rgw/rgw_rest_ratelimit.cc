// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
#include "rgw_rest_ratelimit.h"
class RGWOp_Ratelimit_Info : public RGWRESTOp {
int check_caps(const RGWUserCaps& caps) override {
  return caps.check_cap("ratelimit", RGW_CAP_READ);
}
  
  void execute(optional_yield y) override;

  const char* name() const override { return "get_ratelimit_info"; }
};
void RGWOp_Ratelimit_Info::execute(optional_yield y)
{
  ldpp_dout(this, 20) << "" << dendl;
  std::string uid_str;
  std::string ratelimit_scope;
  std::string bucket_name;
  std::string tenant_name;
  bool global = false;
  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "ratelimit-scope", ratelimit_scope, &ratelimit_scope);
  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  RESTArgs::get_string(s, "tenant", tenant_name, &tenant_name);
  // RESTArgs::get_bool default value to true even if global is empty
  bool exists;
  std::string sval = s->info.args.get("global", &exists);
  if (exists) {
    if (!boost::iequals(sval,"true") && !boost::iequals(sval,"false")) {
      op_ret = -EINVAL;
      ldpp_dout(this, 20) << "global is not equal to true or false" << dendl;
      return;
    }
  }
  RESTArgs::get_bool(s, "global", false, &global);

  if (ratelimit_scope == "bucket" && !bucket_name.empty() && !global) {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    int r = driver->load_bucket(s, rgw_bucket(tenant_name, bucket_name),
                                &bucket, y);
    if (r != 0) {
      op_ret = r;
      ldpp_dout(this, 0) << "Error on getting bucket info" << dendl;
      return;
    }
    RGWRateLimitInfo ratelimit_info;
    auto iter = bucket->get_attrs().find(RGW_ATTR_RATELIMIT);
    if (iter != bucket->get_attrs().end()) {
      try {
        bufferlist& bl = iter->second;
        auto biter = bl.cbegin();
        decode(ratelimit_info, biter);
      } catch (buffer::error& err) {
        ldpp_dout(this, 0) << "Error on decoding ratelimit info from bucket" << dendl;
        op_ret = -EIO;
        return;
      }
    }
    flusher.start(0);
    s->formatter->open_object_section("bucket_ratelimit");
    encode_json("bucket_ratelimit", ratelimit_info, s->formatter);
    s->formatter->close_section();
    flusher.flush();
    return;
  }
  if (ratelimit_scope == "user" && !uid_str.empty() && !global) {
    RGWRateLimitInfo ratelimit_info;
    rgw_user user(uid_str);
    std::unique_ptr<rgw::sal::User> user_sal;
    user_sal = driver->get_user(user);
    if (!rgw::sal::User::empty(user_sal)) {
      op_ret = user_sal->load_user(this, y);
      if (op_ret) {
        ldpp_dout(this, 0) << "Cannot load user info" << dendl;
        return;
      }
    } else {
      ldpp_dout(this, 0) << "User does not exist" << dendl;
      op_ret = -ENOENT;
      return;
    }

    auto iter = user_sal->get_attrs().find(RGW_ATTR_RATELIMIT);
    if(iter != user_sal->get_attrs().end()) {
      try {
        bufferlist& bl = iter->second;
        auto biter = bl.cbegin();
        decode(ratelimit_info, biter);
      } catch (buffer::error& err) {
        ldpp_dout(this, 0) << "Error on decoding ratelimit info from user" << dendl;
        op_ret = -EIO;
        return;
      }
    }
    flusher.start(0);
    s->formatter->open_object_section("user_ratelimit");
    encode_json("user_ratelimit", ratelimit_info, s->formatter);
    s->formatter->close_section();
    flusher.flush();
  }
  if (global) {
    std::string realm_id = driver->get_zone()->get_realm_id();
    RGWPeriodConfig period_config;
    op_ret = period_config.read(this, static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj, realm_id, y);
    if (op_ret && op_ret != -ENOENT) {
      ldpp_dout(this, 0) << "Error on period config read" << dendl;
      return;
    }
    flusher.start(0);
    s->formatter->open_object_section("period_config");
    encode_json("bucket_ratelimit", period_config.bucket_ratelimit, s->formatter);
    encode_json("user_ratelimit", period_config.user_ratelimit, s->formatter);
    encode_json("anonymous_ratelimit", period_config.anon_ratelimit, s->formatter);
    s->formatter->close_section();
    flusher.flush();
    return;
  }
  op_ret = -EINVAL;
  return;
}

class RGWOp_Ratelimit_Set : public RGWRESTOp {
  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("ratelimit", RGW_CAP_WRITE);
  }

  void execute(optional_yield y) override;

  const char* name() const override { return "put_ratelimit_info"; }

  void set_ratelimit_info(bool have_max_read_ops, int64_t max_read_ops, bool have_max_write_ops, int64_t max_write_ops,
                          bool have_max_read_bytes, int64_t max_read_bytes, bool have_max_write_bytes, int64_t max_write_bytes,
                          bool have_enabled, bool enabled, bool& ratelimit_configured, RGWRateLimitInfo& ratelimit_info);
};


  void RGWOp_Ratelimit_Set::set_ratelimit_info(bool have_max_read_ops, int64_t max_read_ops, bool have_max_write_ops, int64_t max_write_ops,
                          bool have_max_read_bytes, int64_t max_read_bytes, bool have_max_write_bytes, int64_t max_write_bytes,
                          bool have_enabled, bool enabled, bool& ratelimit_configured, RGWRateLimitInfo& ratelimit_info) 
  {
    if (have_max_read_ops) {
      if (max_read_ops >= 0) {
        ratelimit_info.max_read_ops = max_read_ops;
        ratelimit_configured = true;
      }
    }
    if (have_max_write_ops) {
      if (max_write_ops >= 0) {
        ratelimit_info.max_write_ops = max_write_ops;
        ratelimit_configured = true;
      }
    }
    if (have_max_read_bytes) {
      if (max_read_bytes >= 0) {
        ratelimit_info.max_read_bytes = max_read_bytes;
        ratelimit_configured = true;
      }
    }
    if (have_max_write_bytes) {
      if (max_write_bytes >= 0) {
        ratelimit_info.max_write_bytes = max_write_bytes;
        ratelimit_configured = true;
      }
    }
    if (have_enabled) {
      ratelimit_info.enabled = enabled;
      ratelimit_configured = true;
    }
    if (!ratelimit_configured) {
      ldpp_dout(this, 0) << "No rate limit configuration arguments have been sent" << dendl;
      op_ret = -EINVAL;
      return;
    }

  }


void RGWOp_Ratelimit_Set::execute(optional_yield y)
{
  std::string uid_str;
  std::string ratelimit_scope;
  std::string bucket_name;
  std::string tenant_name;
  RGWRateLimitInfo ratelimit_info;
  bool ratelimit_configured = false;
  bool enabled = false;
  bool have_enabled = false;
  bool global = false;
  int64_t max_read_ops = 0;
  bool have_max_read_ops = false;
  int64_t max_write_ops = 0;
  bool have_max_write_ops = false;
  int64_t max_read_bytes = 0;
  bool have_max_read_bytes = false;
  int64_t max_write_bytes = 0;
  bool have_max_write_bytes = false;
  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "ratelimit-scope", ratelimit_scope, &ratelimit_scope);
  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  RESTArgs::get_string(s, "tenant", tenant_name, &tenant_name);
  // check there was no -EINVAL coming from get_int64
  op_ret = RESTArgs::get_int64(s, "max-read-ops", 0, &max_read_ops, &have_max_read_ops);
  op_ret |= RESTArgs::get_int64(s, "max-write-ops", 0, &max_write_ops, &have_max_write_ops);
  op_ret |= RESTArgs::get_int64(s, "max-read-bytes", 0, &max_read_bytes, &have_max_read_bytes);
  op_ret |= RESTArgs::get_int64(s, "max-write-bytes", 0, &max_write_bytes, &have_max_write_bytes);
  if (op_ret) {
    ldpp_dout(this, 0) << "one of the maximum arguments could not be parsed" << dendl;
    return;
  }
  // RESTArgs::get_bool default value to true even if enabled or global are empty
  std::string sval = s->info.args.get("enabled", &have_enabled);
  if (have_enabled) {
    if (!boost::iequals(sval,"true") && !boost::iequals(sval,"false")) {
      ldpp_dout(this, 20) << "enabled is not equal to true or false" << dendl;
      op_ret = -EINVAL;
      return;
    }
  }
  RESTArgs::get_bool(s, "enabled", false, &enabled, &have_enabled);
  bool exists;
  sval = s->info.args.get("global", &exists);
  if (exists) {
    if (!boost::iequals(sval,"true") && !boost::iequals(sval,"false")) {
      ldpp_dout(this, 20) << "global is not equal to true or false" << dendl;
      op_ret = -EINVAL;
      return;
    }
  }
  RESTArgs::get_bool(s, "global", false, &global, nullptr);
  set_ratelimit_info(have_max_read_ops, max_read_ops, have_max_write_ops, max_write_ops,
                     have_max_read_bytes, max_read_bytes, have_max_write_bytes, max_write_bytes,
                     have_enabled, enabled, ratelimit_configured, ratelimit_info);
  if (op_ret) {
    return;
  }
  if (ratelimit_scope == "user" && !uid_str.empty() && !global) {
    rgw_user user(uid_str);
    std::unique_ptr<rgw::sal::User> user_sal;
    user_sal = driver->get_user(user);
    if (!rgw::sal::User::empty(user_sal)) {
      op_ret = user_sal->load_user(this, y);
      if (op_ret) {
        ldpp_dout(this, 0) << "Cannot load user info" << dendl;
        return;
      }
    } else {
      ldpp_dout(this, 0) << "User does not exist" << dendl;
      op_ret = -ENOENT;
      return;
    }
    auto iter = user_sal->get_attrs().find(RGW_ATTR_RATELIMIT);
    if (iter != user_sal->get_attrs().end()) {
      try {
        bufferlist& bl = iter->second;
        auto biter = bl.cbegin();
        decode(ratelimit_info, biter);
      } catch (buffer::error& err) {
        ldpp_dout(this, 0) << "Error on decoding ratelimit info from user" << dendl;
        op_ret = -EIO;
        return;
      }
    }
    set_ratelimit_info(have_max_read_ops, max_read_ops, have_max_write_ops, max_write_ops,
                       have_max_read_bytes, max_read_bytes, have_max_write_bytes, max_write_bytes,
                       have_enabled, enabled, ratelimit_configured, ratelimit_info);
    bufferlist bl;
    ratelimit_info.encode(bl);
    rgw::sal::Attrs attr;
    attr[RGW_ATTR_RATELIMIT] = bl;
    op_ret = user_sal->merge_and_store_attrs(this, attr, y);
    return;
  }

  if (ratelimit_scope == "bucket" && !bucket_name.empty() && !global) {
    ldpp_dout(this, 0) << "getting bucket info" << dendl;
    std::unique_ptr<rgw::sal::Bucket> bucket;
    op_ret = driver->load_bucket(this, rgw_bucket(tenant_name, bucket_name),
                                 &bucket, y);
    if (op_ret) {
      ldpp_dout(this, 0) << "Error on getting bucket info" << dendl;
      return;
    }
    auto iter = bucket->get_attrs().find(RGW_ATTR_RATELIMIT);
    if (iter != bucket->get_attrs().end()) {
      try {
        bufferlist& bl = iter->second;
        auto biter = bl.cbegin();
        decode(ratelimit_info, biter);
      } catch (buffer::error& err) {
        ldpp_dout(this, 0) << "Error on decoding ratelimit info from bucket" << dendl;
        op_ret = -EIO;
        return;
      }
    }
    bufferlist bl;
    set_ratelimit_info(have_max_read_ops, max_read_ops, have_max_write_ops, max_write_ops,
                       have_max_read_bytes, max_read_bytes, have_max_write_bytes, max_write_bytes,
                       have_enabled, enabled, ratelimit_configured, ratelimit_info);
    ratelimit_info.encode(bl);
    rgw::sal::Attrs attr;
    attr[RGW_ATTR_RATELIMIT] = bl;
    op_ret = bucket->merge_and_store_attrs(this, attr, y);
    return;
  }
  if (global) {
    std::string realm_id = driver->get_zone()->get_realm_id();
    RGWPeriodConfig period_config;
    op_ret = period_config.read(s, static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj, realm_id, y);
    if (op_ret && op_ret != -ENOENT) {
      ldpp_dout(this, 0) << "Error on period config read" << dendl;
      return;
    }
    if (ratelimit_scope == "bucket") {
      ratelimit_info = period_config.bucket_ratelimit;
      set_ratelimit_info(have_max_read_ops, max_read_ops, have_max_write_ops, max_write_ops,
                         have_max_read_bytes, max_read_bytes, have_max_write_bytes, max_write_bytes,
                         have_enabled, enabled, ratelimit_configured, ratelimit_info);
      period_config.bucket_ratelimit = ratelimit_info;
      op_ret = period_config.write(s, static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj, realm_id, y);
      return;
    }
    if (ratelimit_scope == "anon") {
      ratelimit_info = period_config.anon_ratelimit;
      set_ratelimit_info(have_max_read_ops, max_read_ops, have_max_write_ops, max_write_ops,
                         have_max_read_bytes, max_read_bytes, have_max_write_bytes, max_write_bytes,
                         have_enabled, enabled, ratelimit_configured, ratelimit_info);
      period_config.anon_ratelimit = ratelimit_info;
      op_ret = period_config.write(s, static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj, realm_id, y);
      return;
    }
    if (ratelimit_scope == "user") {
      ratelimit_info = period_config.user_ratelimit;
      set_ratelimit_info(have_max_read_ops, max_read_ops, have_max_write_ops, max_write_ops,
                         have_max_read_bytes, max_read_bytes, have_max_write_bytes, max_write_bytes,
                         have_enabled, enabled, ratelimit_configured, ratelimit_info);
      period_config.user_ratelimit = ratelimit_info;
      op_ret = period_config.write(s, static_cast<rgw::sal::RadosStore*>(driver)->svc()->sysobj, realm_id, y);
      return;
    }
  }
  op_ret = -EINVAL;
  return;
}
RGWOp* RGWHandler_Ratelimit::op_get()
{
  return new RGWOp_Ratelimit_Info;
}
RGWOp* RGWHandler_Ratelimit::op_post()
{
  return new RGWOp_Ratelimit_Set;
}
