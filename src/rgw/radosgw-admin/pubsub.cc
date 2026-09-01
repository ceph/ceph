// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "radosgw-admin/pubsub.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <string_view>

#include "cls/2pc_queue/cls_2pc_queue_client.h"
#include "cls/2pc_queue/cls_2pc_queue_types.h"
#include "common/ceph_json.h"
#include "common/errno.h"
#include "driver/rados/rgw_notify.h"
#include "driver/rados/rgw_sal_rados.h"
#include "rgw_pubsub.h"
#include "rgw_sal.h"
#include "rgw_zone.h"

using ceph::Formatter;

#define dout_subsys ceph_subsys_rgw

namespace {

void show_topics_info_v2(const rgw_pubsub_topic& topic,
                         const std::set<std::string>& subscribed_buckets,
                         Formatter* formatter)
{
  formatter->open_object_section("topic");
  topic.dump(formatter);
  encode_json("subscribed_buckets", subscribed_buckets, formatter);
  formatter->close_section();
}

int load_bucket(const DoutPrefixProvider* dpp,
                rgw::sal::Driver* driver,
                std::string_view tenant,
                std::string_view bucket_name,
                std::string_view bucket_id,
                std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  rgw_bucket b{std::string(tenant), std::string(bucket_name), std::string(bucket_id)};
  return driver->load_bucket(dpp, b, bucket, null_yield);
}

} // anonymous namespace

int rgw_admin_pubsub(const DoutPrefixProvider* dpp,
                     rgw::sal::Driver* driver,
                     const rgw::SiteConfig& site,
                     rgw::sal::User* user,
                     Formatter* formatter,
                     const rgw_admin_pubsub_options& opts)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = 0;

  switch (opts.command) {
  case rgw_admin::OPT::PUBSUB_NOTIFICATION_LIST:
    if (opts.bucket_name.empty()) {
      std::cerr << "ERROR: bucket name was not provided (via --bucket)" << std::endl;
      return EINVAL;
    }

    {
      rgw_pubsub_bucket_topics result;
      ret = load_bucket(dpp, driver, opts.tenant, opts.bucket_name,
                        opts.bucket_id, &bucket);
      if (ret < 0) {
        std::cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      if (rgw::all_zonegroups_support(site, rgw::zone_features::notification_v2) &&
          driver->stat_topics_v1(std::string(opts.tenant), null_yield, dpp) == -ENOENT) {
        ret = get_bucket_notifications(dpp, bucket.get(), result);
        if (ret < 0) {
          std::cerr << "ERROR: could not get topics: " << cpp_strerror(-ret)
                    << std::endl;
          return -ret;
        }
      } else {
        const std::string account = !opts.account_id.empty()
          ? std::string(opts.account_id) : std::string(opts.tenant);
        RGWPubSub ps(driver, account, site);
        const RGWPubSub::Bucket b(ps, bucket.get());
        ret = b.get_topics(dpp, result, null_yield);
        if (ret < 0 && ret != -ENOENT) {
          std::cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      }
      encode_json("result", result, formatter);
      formatter->flush(std::cout);
    }
    break;

  case rgw_admin::OPT::PUBSUB_TOPIC_LIST:
    {
      const std::string account = !opts.account_id.empty()
        ? std::string(opts.account_id) : std::string(opts.tenant);
      RGWPubSub ps(driver, account, site);
      std::string next_token(opts.marker);

      std::optional<rgw_owner> owner;
      if (user && !rgw::sal::User::empty(user)) {
        owner = user->get_id();
      } else if (!opts.account_id.empty()) {
        owner = rgw_account_id{std::string(opts.account_id)};
      }

      rgw_pubsub_topics result;
      if (rgw::all_zonegroups_support(site, rgw::zone_features::notification_v2) &&
          driver->stat_topics_v1(std::string(opts.tenant), null_yield, dpp) == -ENOENT) {
        formatter->open_array_section("topics");
        int max_entries = opts.max_entries;
        do {
          ret = ps.get_topics_v2(dpp, next_token, max_entries,
                                 result, next_token, null_yield);
          if (ret < 0 && ret != -ENOENT) {
            std::cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          for (const auto& [_, topic] : result.topics) {
            if (owner && *owner != topic.owner) {
              continue;
            }
            std::set<std::string> subscribed_buckets;
            ret = driver->get_bucket_topic_mapping(topic, subscribed_buckets,
                                                   null_yield, dpp);
            if (ret < 0) {
              std::cerr << "failed to fetch bucket topic mapping info for topic: "
                        << topic.name << ", ret=" << ret << std::endl;
            }
            show_topics_info_v2(topic, subscribed_buckets, formatter);
            if (opts.max_entries_specified) {
              --max_entries;
            }
          }
          result.topics.clear();
        } while (!next_token.empty() && max_entries > 0);
        formatter->close_section(); // topics
      } else { // v1, list all topics
        ret = ps.get_topics_v1(dpp, result, null_yield);
        if (ret < 0 && ret != -ENOENT) {
          std::cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        encode_json("result", result, formatter);
      }
      if (opts.max_entries_specified) {
        encode_json("truncated", !next_token.empty(), formatter);
        if (!next_token.empty()) {
          encode_json("marker", next_token, formatter);
        }
      }
      formatter->flush(std::cout);
    }
    break;

  case rgw_admin::OPT::PUBSUB_TOPIC_GET:
    if (opts.topic_name.empty()) {
      std::cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }
    {
      const std::string account = !opts.account_id.empty()
        ? std::string(opts.account_id) : std::string(opts.tenant);
      RGWPubSub ps(driver, account, site);

      rgw_pubsub_topic topic;
      std::set<std::string> subscribed_buckets;
      ret = ps.get_topic(dpp, std::string(opts.topic_name), topic, null_yield, &subscribed_buckets);
      if (ret < 0) {
        std::cerr << "ERROR: could not get topic: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      if (rgw::all_zonegroups_support(site, rgw::zone_features::notification_v2) &&
          driver->stat_topics_v1(std::string(opts.tenant), null_yield, dpp) == -ENOENT) {
        show_topics_info_v2(topic, subscribed_buckets, formatter);
      } else {
        encode_json("topic", topic, formatter);
      }
      formatter->flush(std::cout);
    }
    break;

  case rgw_admin::OPT::PUBSUB_NOTIFICATION_GET:
    if (opts.notification_id.empty()) {
      std::cerr << "ERROR: notification-id was not provided (via --notification-id)" << std::endl;
      return EINVAL;
    }
    if (opts.bucket_name.empty()) {
      std::cerr << "ERROR: bucket name was not provided (via --bucket)" << std::endl;
      return EINVAL;
    }
    {
      ret = load_bucket(dpp, driver, opts.tenant, opts.bucket_name,
                        opts.bucket_id, &bucket);
      if (ret < 0) {
        std::cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      rgw_pubsub_bucket_topics bucket_topics;
      if (rgw::all_zonegroups_support(site, rgw::zone_features::notification_v2) &&
          driver->stat_topics_v1(std::string(opts.tenant), null_yield, dpp) == -ENOENT) {
        ret = get_bucket_notifications(dpp, bucket.get(), bucket_topics);
        if (ret < 0) {
          std::cerr << "ERROR: could not get bucket notifications: "
                    << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      } else {
        const std::string account = !opts.account_id.empty()
          ? std::string(opts.account_id) : std::string(opts.tenant);
        RGWPubSub ps(driver, account, site);
        const RGWPubSub::Bucket b(ps, bucket.get());
        ret = b.get_topics(dpp, bucket_topics, null_yield);
        if (ret < 0 && ret != -ENOENT) {
          std::cerr << "ERROR: could not get bucket notifications: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      }
      auto iter = find_unique_topic(bucket_topics, std::string(opts.notification_id));
      if (!iter) {
        std::cerr << "ERROR: notification was not found" << std::endl;
        return ENOENT;
      }
      encode_json("notification", *iter, formatter);
      formatter->flush(std::cout);
    }
    break;

  case rgw_admin::OPT::PUBSUB_TOPIC_RM:
    if (opts.topic_name.empty()) {
      std::cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }
    if (!driver->is_meta_master()) {
      std::cerr << "ERROR: Run 'topic rm' from master zone " << std::endl;
      return -EINVAL;
    }
    {
      const std::string account = !opts.account_id.empty()
        ? std::string(opts.account_id) : std::string(opts.tenant);
      RGWPubSub ps(driver, account, site);

      ret = ps.remove_topic(dpp, std::string(opts.topic_name), null_yield);
      if (ret < 0) {
        std::cerr << "ERROR: could not remove topic: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
    break;

  case rgw_admin::OPT::PUBSUB_NOTIFICATION_RM:
    if (opts.bucket_name.empty()) {
      std::cerr << "ERROR: bucket name was not provided (via --bucket)" << std::endl;
      return EINVAL;
    }
    if (!driver->is_meta_master()) {
      std::cerr << "ERROR: Run 'notification rm' from master zone " << std::endl;
      return -EINVAL;
    }
    {
      ret = load_bucket(dpp, driver, opts.tenant, opts.bucket_name,
                        opts.bucket_id, &bucket);
      if (ret < 0) {
        std::cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      if (rgw::all_zonegroups_support(site, rgw::zone_features::notification_v2)) {
        if (ret = driver->stat_topics_v1(std::string(opts.tenant), null_yield, dpp); ret != -ENOENT) {
          std::cerr << "WARNING: " << (ret == 0 ? "topic migration in process" : "cannot determine topic migration status. ret = " + std::to_string(ret))
                    << ". please try again later" << std::endl;
          return -ret;
        }
        ret = remove_notification_v2(dpp, driver, bucket.get(), std::string(opts.notification_id),
                                     null_yield);
      } else {
        const std::string account = !opts.account_id.empty()
          ? std::string(opts.account_id) : std::string(opts.tenant);
        RGWPubSub ps(driver, account, site);

        rgw_pubsub_bucket_topics bucket_topics;
        const RGWPubSub::Bucket b(ps, bucket.get());
        ret = b.get_topics(dpp, bucket_topics, null_yield);
        if (ret < 0 && ret != -ENOENT) {
          std::cerr << "ERROR: could not get bucket notifications: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        if (opts.notification_id.empty()) {
          ret = b.remove_notifications(dpp, null_yield);
        } else {
          ret = b.remove_notification_by_id(dpp, std::string(opts.notification_id), null_yield);
        }
      }
      if (ret < 0 && ret != -ENOENT) {
        std::cerr << "ERROR: could not remove notification: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
    break;

  case rgw_admin::OPT::PUBSUB_TOPIC_STATS:
    if (opts.topic_name.empty()) {
      std::cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }
    {
      const std::string account = !opts.account_id.empty()
        ? std::string(opts.account_id) : std::string(opts.tenant);
      RGWPubSub ps(driver, account, site);

      rgw_pubsub_topic topic;
      ret = ps.get_topic(dpp, std::string(opts.topic_name), topic, null_yield, nullptr);
      if (ret < 0) {
        std::cerr << "ERROR: could not get topic: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      if (topic.dest.persistent_queue.empty()) {
        std::cerr << "This topic does not have a persistent queue." << std::endl;
        return ENOENT;
      }

      auto ioctx = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_notif_pool_ctx();
      rgw::notify::rgw_topic_stats stats;
      ret = rgw::notify::get_persistent_queue_stats(
          dpp, ioctx,
          topic.dest.get_shard_names(), stats, null_yield);
      if (ret < 0) {
        std::cerr << "ERROR: could not get persistent queues: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      encode_json("", stats, formatter);
      formatter->flush(std::cout);
    }
    break;

  case rgw_admin::OPT::PUBSUB_TOPIC_DUMP:
    if (opts.topic_name.empty()) {
      std::cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }
    {
      const std::string account = !opts.account_id.empty()
        ? std::string(opts.account_id) : std::string(opts.tenant);
      RGWPubSub ps(driver, account, site);

      rgw_pubsub_topic topic;
      ret = ps.get_topic(dpp, std::string(opts.topic_name), topic, null_yield, nullptr);
      if (ret < 0) {
        std::cerr << "ERROR: could not get topic. error: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      if (topic.dest.persistent_queue.empty()) {
        std::cerr << "ERROR: topic does not have a persistent queue" << std::endl;
        return ENOENT;
      }

      auto ioctx = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_notif_pool_ctx();
      std::string marker;
      std::string end_marker;
      librados::ObjectReadOperation rop;
      std::vector<cls_queue_entry> queue_entries;
      bool truncated;
      formatter->open_array_section("eventEntries");

      for (const auto& shard_name: topic.dest.get_shard_names()){
        truncated = true;
        marker.clear();
        while (truncated) {
          bufferlist bl;
          int rc;
          cls_2pc_queue_list_entries(rop, marker, opts.max_entries, &bl, &rc);
          ioctx.operate(shard_name, &rop, nullptr);
          if (rc < 0 ) {
            std::cerr << "ERROR: could not list entries from queue. error: " << cpp_strerror(-ret) << std::endl;
            return -rc;
          }
          rc = cls_2pc_queue_list_entries_result(bl, queue_entries, &truncated, end_marker);
          if (rc < 0) {
            std::cerr << "ERROR: failed to parse list entries from queue (skipping). error: " << cpp_strerror(-ret) << std::endl;
            return -rc;
          }
          std::for_each(queue_entries.cbegin(),
            queue_entries.cend(),
            [&formatter](const auto& queue_entry) {
              rgw::notify::event_entry_t event_entry;
              bufferlist::const_iterator iter{&queue_entry.data};
              try {
                event_entry.decode(iter);
                encode_json("", event_entry, formatter);
              } catch (const buffer::error& e) {
                std::cerr << "ERROR: failed to decode queue entry. error: " << e.what() << std::endl;
              }
            });
          formatter->flush(std::cout);
          marker = end_marker;
        }
      }
      formatter->close_section();
      formatter->flush(std::cout);
    }
    break;

  default:
    return EINVAL;
  }

  return 0;
}
