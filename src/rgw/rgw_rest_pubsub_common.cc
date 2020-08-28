// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_rest_pubsub_common.h"
#include "common/dout.h"
#include "rgw_url.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

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
      if (!rgw_transport_is_secure(cct, env)) {
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

bool topics_has_endpoint_secret(const rgw_pubsub_user_topics& topics) {
    for (const auto& topic : topics.topics) {
        if (topic_has_endpoint_secret(topic.second)) return true;
    }
    return false;
}
void RGWPSCreateTopicOp::execute(const Span& parent_span) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups.emplace(store, s->owner.get_id());
  op_ret = ups->create_topic(topic_name, dest, topic_arn, opaque_data);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to create topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully created topic '" << topic_name << "'" << dendl;
}

void RGWPSListTopicsOp::execute(const Span& parent_span) {
  ups.emplace(store, s->owner.get_id());
  op_ret = ups->get_user_topics(&result);
  // if there are no topics it is not considered an error
  op_ret = op_ret == -ENOENT ? 0 : op_ret;
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topics, ret=" << op_ret << dendl;
    return;
  }
  if (topics_has_endpoint_secret(result) && !rgw_transport_is_secure(s->cct, *(s->info.env))) {
    ldout(s->cct, 1) << "topics contain secret and cannot be sent over insecure transport" << dendl;
    op_ret = -EPERM;
    return;
  }
  ldout(s->cct, 20) << "successfully got topics" << dendl;
}

void RGWPSGetTopicOp::execute(const Span& parent_span) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups.emplace(store, s->owner.get_id());
  op_ret = ups->get_topic(topic_name, &result);
  if (topic_has_endpoint_secret(result) && !rgw_transport_is_secure(s->cct, *(s->info.env))) {
    ldout(s->cct, 1) << "topic '" << topic_name << "' contain secret and cannot be sent over insecure transport" << dendl;
    op_ret = -EPERM;
    return;
  }
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 1) << "successfully got topic '" << topic_name << "'" << dendl;
}

void RGWPSDeleteTopicOp::execute(const Span& parent_span) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups.emplace(store, s->owner.get_id());
  op_ret = ups->remove_topic(topic_name);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to remove topic '" << topic_name << ", ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 1) << "successfully removed topic '" << topic_name << "'" << dendl;
}

void RGWPSCreateSubOp::execute(const Span& parent_span) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups.emplace(store, s->owner.get_id());
  auto sub = ups->get_sub(sub_name);
  op_ret = sub->subscribe(topic_name, dest);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to create subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully created subscription '" << sub_name << "'" << dendl;
}

void RGWPSGetSubOp::execute(const Span& parent_span) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups.emplace(store, s->owner.get_id());
  auto sub = ups->get_sub(sub_name);
  op_ret = sub->get_conf(&result);
  if (subscription_has_endpoint_secret(result) && !rgw_transport_is_secure(s->cct, *(s->info.env))) {
    ldout(s->cct, 1) << "subscription '" << sub_name << "' contain secret and cannot be sent over insecure transport" << dendl;
    op_ret = -EPERM;
    return;
  }
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully got subscription '" << sub_name << "'" << dendl;
}

void RGWPSDeleteSubOp::execute(const Span& parent_span) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups.emplace(store, s->owner.get_id());
  auto sub = ups->get_sub(sub_name);
  op_ret = sub->unsubscribe(topic_name);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to remove subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully removed subscription '" << sub_name << "'" << dendl;
}

void RGWPSAckSubEventOp::execute(const Span& parent_span) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups.emplace(store, s->owner.get_id());
  auto sub = ups->get_sub_with_events(sub_name);
  op_ret = sub->remove_event(event_id);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to ack event on subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully acked event on subscription '" << sub_name << "'" << dendl;
}

void RGWPSPullSubEventsOp::execute(const Span& parent_span) {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups.emplace(store, s->owner.get_id());
  sub = ups->get_sub_with_events(sub_name);
  if (!sub) {
    op_ret = -ENOENT;
    ldout(s->cct, 1) << "failed to get subscription '" << sub_name << "' for events, ret=" << op_ret << dendl;
    return;
  }
  op_ret = sub->list_events(marker, max_entries);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get events from subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully got events from subscription '" << sub_name << "'" << dendl;
}


int RGWPSCreateNotifOp::verify_permission(const Span& parent_span) {
  int ret = get_params();
  if (ret < 0) {
    return ret;
  }

  const auto& id = s->owner.get_id();

  ret = store->getRados()->get_bucket_info(store->svc(), id.tenant, bucket_name,
                               bucket_info, nullptr, null_yield, nullptr);
  if (ret < 0) {
    ldout(s->cct, 1) << "failed to get bucket info, cannot verify ownership" << dendl;
    return ret;
  }

  if (bucket_info.owner != id) {
    ldout(s->cct, 1) << "user doesn't own bucket, not allowed to create notification" << dendl;
    return -EPERM;
  }
  return 0;
}

int RGWPSDeleteNotifOp::verify_permission(const Span& parent_span) {
  int ret = get_params();
  if (ret < 0) {
    return ret;
  }

  ret = store->getRados()->get_bucket_info(store->svc(), s->owner.get_id().tenant, bucket_name,
                               bucket_info, nullptr, null_yield, nullptr);
  if (ret < 0) {
    return ret;
  }

  if (bucket_info.owner != s->owner.get_id()) {
    ldout(s->cct, 1) << "user doesn't own bucket, cannot remove notification" << dendl;
    return -EPERM;
  }
  return 0;
}

int RGWPSListNotifsOp::verify_permission(const Span& parent_span) {
  int ret = get_params();
  if (ret < 0) {
    return ret;
  }

  ret = store->getRados()->get_bucket_info(store->svc(), s->owner.get_id().tenant, bucket_name,
                               bucket_info, nullptr, null_yield, nullptr);
  if (ret < 0) {
    return ret;
  }

  if (bucket_info.owner != s->owner.get_id()) {
    ldout(s->cct, 1) << "user doesn't own bucket, cannot get notification list" << dendl;
    return -EPERM;
  }

  return 0;
}

