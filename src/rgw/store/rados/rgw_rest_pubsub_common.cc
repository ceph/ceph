// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_rest_pubsub_common.h"
#include "common/dout.h"
#include "rgw_url.h"
#include "rgw_sal_rados.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

bool verify_transport_security(CephContext *cct, const RGWEnv& env) {
  const auto is_secure = rgw_transport_is_secure(cct, env);
  if (!is_secure && g_conf().get_val<bool>("rgw_allow_notification_secrets_in_cleartext")) {
    ldout(cct, 0) << "WARNING: bypassing endpoint validation, allow sending password over insecure transport" << dendl;
    return true;
  }
  return is_secure;
}

bool validate_and_update_endpoint_secret(rgw_pubsub_sub_dest& dest, CephContext *cct, const RGWEnv& env) {
  if (dest.push_endpoint.empty()) {
      return true;
  }
  std::string user;
  std::string password;
  if (!rgw::parse_url_userinfo(dest.push_endpoint, user, password)) {
    ldout(cct, 1) << "endpoint validation error: malformed endpoint URL:" << dest.push_endpoint << dendl;
    return false;
  }
  // this should be verified inside parse_url()
  ceph_assert(user.empty() == password.empty());
  if (!user.empty()) {
      dest.stored_secret = true;
      if (!verify_transport_security(cct, env)) {
        ldout(cct, 1) << "endpoint validation error: sending password over insecure transport" << dendl;
        return false;
      }
  }
  return true;
}

bool subscription_has_endpoint_secret(const rgw_pubsub_sub_config& sub) {
    return sub.dest.stored_secret;
}

bool topic_has_endpoint_secret(const rgw_pubsub_topic_subs& topic) {
    return topic.topic.dest.stored_secret;
}

bool topics_has_endpoint_secret(const rgw_pubsub_topics& topics) {
    for (const auto& topic : topics.topics) {
        if (topic_has_endpoint_secret(topic.second)) return true;
    }
    return false;
}

void RGWPSCreateTopicOp::execute(optional_yield y) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ps.emplace(static_cast<rgw::sal::RadosStore*>(store), s->owner.get_id().tenant);
  op_ret = ps->create_topic(this, topic_name, dest, topic_arn, opaque_data, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to create topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 20) << "successfully created topic '" << topic_name << "'" << dendl;
}

void RGWPSListTopicsOp::execute(optional_yield y) {
  ps.emplace(static_cast<rgw::sal::RadosStore*>(store), s->owner.get_id().tenant);
  op_ret = ps->get_topics(&result);
  // if there are no topics it is not considered an error
  op_ret = op_ret == -ENOENT ? 0 : op_ret;
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get topics, ret=" << op_ret << dendl;
    return;
  }
  if (topics_has_endpoint_secret(result) && !verify_transport_security(s->cct, *(s->info.env))) {
    ldpp_dout(this, 1) << "topics contain secret and cannot be sent over insecure transport" << dendl;
    op_ret = -EPERM;
    return;
  }
  ldpp_dout(this, 20) << "successfully got topics" << dendl;
}

void RGWPSGetTopicOp::execute(optional_yield y) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ps.emplace(static_cast<rgw::sal::RadosStore*>(store), s->owner.get_id().tenant);
  op_ret = ps->get_topic(topic_name, &result);
  if (topic_has_endpoint_secret(result) && !verify_transport_security(s->cct, *(s->info.env))) {
    ldpp_dout(this, 1) << "topic '" << topic_name << "' contain secret and cannot be sent over insecure transport" << dendl;
    op_ret = -EPERM;
    return;
  }
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 1) << "successfully got topic '" << topic_name << "'" << dendl;
}

void RGWPSDeleteTopicOp::execute(optional_yield y) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ps.emplace(static_cast<rgw::sal::RadosStore*>(store), s->owner.get_id().tenant);
  op_ret = ps->remove_topic(this, topic_name, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to remove topic '" << topic_name << ", ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 1) << "successfully removed topic '" << topic_name << "'" << dendl;
}

void RGWPSCreateSubOp::execute(optional_yield y) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ps.emplace(static_cast<rgw::sal::RadosStore*>(store), s->owner.get_id().tenant);
  auto sub = ps->get_sub(sub_name);
  op_ret = sub->subscribe(this, topic_name, dest, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to create subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 20) << "successfully created subscription '" << sub_name << "'" << dendl;
}

void RGWPSGetSubOp::execute(optional_yield y) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ps.emplace(static_cast<rgw::sal::RadosStore*>(store), s->owner.get_id().tenant);
  auto sub = ps->get_sub(sub_name);
  op_ret = sub->get_conf(&result);
  if (subscription_has_endpoint_secret(result) && !verify_transport_security(s->cct, *(s->info.env))) {
    ldpp_dout(this, 1) << "subscription '" << sub_name << "' contain secret and cannot be sent over insecure transport" << dendl;
    op_ret = -EPERM;
    return;
  }
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 20) << "successfully got subscription '" << sub_name << "'" << dendl;
}

void RGWPSDeleteSubOp::execute(optional_yield y) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ps.emplace(static_cast<rgw::sal::RadosStore*>(store), s->owner.get_id().tenant);
  auto sub = ps->get_sub(sub_name);
  op_ret = sub->unsubscribe(this, topic_name, y);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to remove subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 20) << "successfully removed subscription '" << sub_name << "'" << dendl;
}

void RGWPSAckSubEventOp::execute(optional_yield y) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ps.emplace(static_cast<rgw::sal::RadosStore*>(store), s->owner.get_id().tenant);
  auto sub = ps->get_sub_with_events(sub_name);
  op_ret = sub->remove_event(s, event_id);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to ack event on subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 20) << "successfully acked event on subscription '" << sub_name << "'" << dendl;
}

void RGWPSPullSubEventsOp::execute(optional_yield y) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ps.emplace(static_cast<rgw::sal::RadosStore*>(store), s->owner.get_id().tenant);
  sub = ps->get_sub_with_events(sub_name);
  if (!sub) {
    op_ret = -ENOENT;
    ldpp_dout(this, 1) << "failed to get subscription '" << sub_name << "' for events, ret=" << op_ret << dendl;
    return;
  }
  op_ret = sub->list_events(s, marker, max_entries);
  if (op_ret < 0) {
    ldpp_dout(this, 1) << "failed to get events from subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldpp_dout(this, 20) << "successfully got events from subscription '" << sub_name << "'" << dendl;
}


int RGWPSCreateNotifOp::verify_permission(optional_yield y) {
  int ret = get_params();
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::User> user = store->get_user(s->owner.get_id());
  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = store->get_bucket(this, user.get(), s->owner.get_id().tenant, bucket_name, &bucket, y);
  if (ret < 0) {
    ldpp_dout(this, 1) << "failed to get bucket info, cannot verify ownership" << dendl;
    return ret;
  }
  bucket_info = bucket->get_info();

  if (bucket_info.owner != s->owner.get_id()) {
    ldpp_dout(this, 1) << "user doesn't own bucket, not allowed to create notification" << dendl;
    return -EPERM;
  }
  return 0;
}

int RGWPSDeleteNotifOp::verify_permission(optional_yield y) {
  int ret = get_params();
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::User> user = store->get_user(s->owner.get_id());
  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = store->get_bucket(this, user.get(), s->owner.get_id().tenant, bucket_name, &bucket, y);
  if (ret < 0) {
    return ret;
  }
  bucket_info = bucket->get_info();

  if (bucket_info.owner != s->owner.get_id()) {
    ldpp_dout(this, 1) << "user doesn't own bucket, cannot remove notification" << dendl;
    return -EPERM;
  }
  return 0;
}

int RGWPSListNotifsOp::verify_permission(optional_yield y) {
  int ret = get_params();
  if (ret < 0) {
    return ret;
  }

  std::unique_ptr<rgw::sal::User> user = store->get_user(s->owner.get_id());
  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = store->get_bucket(this, user.get(), s->owner.get_id().tenant, bucket_name, &bucket, y);
  if (ret < 0) {
    return ret;
  }
  bucket_info = bucket->get_info();

  if (bucket_info.owner != s->owner.get_id()) {
    ldpp_dout(this, 1) << "user doesn't own bucket, cannot get notification list" << dendl;
    return -EPERM;
  }

  return 0;
}

