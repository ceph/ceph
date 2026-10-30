// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * A NooBaa bucket's notifications, as RGW topics and the bucket's
 * notification attribute.
 *
 * I10 of the work plan (`NSFS_ADOPTION.md` §5, "Notifications and their
 * destinations").  Runs inside I5's write of one bucket:  the topics
 * first, the attribute in the same `merge_and_store_attrs()` as the
 * rest of the bucket's state, and the topic-to-bucket mapping after the
 * placement row.
 *
 * A CONNECTION BECOMES A TOPIC OF THE BUCKET'S ACCOUNT (DECISIONS.md
 * 2026-10-29, 2026-10-30).  An HTTP topic is named after the
 * connection;  a Kafka topic after its Kafka topic, since RGW publishes
 * to the Kafka topic its own topic is named for.  NooBaa hands
 * `agent_request_object` and `request_options_object` to Node's HTTP
 * client, and `kafka_options_object` to librdkafka, as they stand, so
 * the translation carries each member RGW has a counterpart for and
 * reports each it has not.
 *
 * WHAT IS CARRIED AS NOOBAA HAD IT, AND REPORTED.  A credential to an
 * insecure destination (DECISIONS.md 2026-10-29), and a Kafka setting
 * `rgw_kafka_config_allowed_keys` does not list (2026-10-30):  the
 * topic is written, and delivery is refused until the operator acts.
 *
 * Not persistent:  persistent delivery is deferred (work plan, Future
 * work 6), and nsfs refuses a persistent topic.
 */

#pragma once

#include <string>
#include <vector>

#include "rgw_pubsub.h"
#include "rgw_sal_fwd.h"

#include "noobaa_config_store.h"

namespace rgw { namespace sal {

class NSFSDriver;

namespace nsfs {

/* One connection, as an RGW topic's name and destination. */
struct TranslatedTopic {
  std::string name;
  rgw_pubsub_dest dest;
  /* a member NooBaa used that the topic does not carry */
  std::vector<std::string> not_carried;
  /* carried, and needing the operator:  an insecure credential, or a
   * Kafka setting the allowlist does not list */
  std::vector<std::string> warnings;
};

/* A connection as a topic.  `kafka_topic` is the topic a notification
 * named itself (`kafka:::topic/<connection>/<topic>`), or empty for the
 * connection's own.
 *
 * Returns 0, or -EINVAL with `why` when the connection cannot be a
 * topic at all:  an unknown protocol, no broker list, no Kafka topic,
 * or a name RGW's topic names cannot hold.  Pure apart from reading
 * `rgw_kafka_config_allowed_keys` and `rgw_relaxed_topic_names`. */
int translate_connection(CephContext* cct, const NooBaaConnection& conn,
			 const std::string& kafka_topic, TranslatedTopic& out,
			 std::string& why);

/* NooBaa's event names as RGW's.  NooBaa matches them ignoring case
 * (`check_notif_relevant()`), so they are mapped ignoring case;  a name
 * RGW has no event for goes to `unknown`. */
void translate_events(const std::vector<std::string>& names,
		      rgw::notify::EventTypeList& events,
		      std::vector<std::string>& unknown);

struct NotificationImportResult {
  size_t topics_written{0};
  size_t notifications_carried{0};
  /* a notification or a part of one not carried, or carried and needing
   * the operator;  one line each, naming the bucket */
  std::vector<std::string> reported;
  /* the topics the bucket's attribute names, to map to the bucket once
   * it is written */
  std::vector<rgw_pubsub_topic> topics;
};

/* One bucket's `notifications[]`.
 *
 * Writes each topic the bucket's notifications need for `account`, or
 * finds it already there with the same destination, and fills
 * `attrs[RGW_ATTR_BUCKET_NOTIFICATION]`.  A topic of the same name with
 * another destination is not overwritten:  that notification is
 * reported and left out.  Returns a negative errno only when writing a
 * topic fails. */
int import_bucket_notifications(const DoutPrefixProvider* dpp,
				optional_yield y,
				NSFSDriver* driver,
				const std::string& bucket_name,
				const std::string& notifications_json,
				const std::vector<NooBaaConnection>& connections,
				const std::string& account,
				Attrs& attrs,
				NotificationImportResult& result);

}}} // namespace rgw::sal::nsfs
