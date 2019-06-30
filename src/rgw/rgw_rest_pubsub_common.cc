// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rest_pubsub_common.h"
#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

void RGWPSCreateTopicOp::execute() {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ups.emplace(store, s->owner.get_id());
  op_ret = ups->create_topic(topic_name, dest, topic_arn);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to create topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully created topic '" << topic_name << "'" << dendl;
}

void RGWPSListTopicsOp::execute() {
  ups.emplace(store, s->owner.get_id());
  op_ret = ups->get_user_topics(&result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topics, ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully got topics" << dendl;
}

void RGWPSGetTopicOp::execute() {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups.emplace(store, s->owner.get_id());
  op_ret = ups->get_topic(topic_name, &result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get topic '" << topic_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 1) << "successfully got topic '" << topic_name << "'" << dendl;
}

void RGWPSDeleteTopicOp::execute() {
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

void RGWPSCreateSubOp::execute() {
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

void RGWPSGetSubOp::execute() {
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  ups.emplace(store, s->owner.get_id());
  auto sub = ups->get_sub(sub_name);
  op_ret = sub->get_conf(&result);
  if (op_ret < 0) {
    ldout(s->cct, 1) << "failed to get subscription '" << sub_name << "', ret=" << op_ret << dendl;
    return;
  }
  ldout(s->cct, 20) << "successfully got subscription '" << sub_name << "'" << dendl;
}

void RGWPSDeleteSubOp::execute() {
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

void RGWPSAckSubEventOp::execute() {
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

void RGWPSPullSubEventsOp::execute() {
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


int RGWPSCreateNotifOp::verify_permission() {
  int ret = get_params();
  if (ret < 0) {
    return ret;
  }

  const auto& id = s->owner.get_id();

  ret = store->get_bucket_info(*s->sysobj_ctx, id.tenant, bucket_name,
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

int RGWPSDeleteNotifOp::verify_permission() {
  int ret = get_params();
  if (ret < 0) {
    return ret;
  }

  ret = store->get_bucket_info(*s->sysobj_ctx, s->owner.get_id().tenant, bucket_name,
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

int RGWPSListNotifsOp::verify_permission() {
  int ret = get_params();
  if (ret < 0) {
    return ret;
  }

  ret = store->get_bucket_info(*s->sysobj_ctx, s->owner.get_id().tenant, bucket_name,
                               bucket_info, nullptr, null_yield, nullptr);
  if (ret < 0) {
    return ret;
  }

  if (bucket_info.owner != s->owner.get_id()) {
    ldout(s->cct, 1) << "user doesn't own bucket, cannot get topic list" << dendl;
    return -EPERM;
  }

  return 0;
}

