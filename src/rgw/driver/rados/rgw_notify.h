// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include "common/ceph_time.h"
#include "include/common_fwd.h"
#include "rgw_notify_event_type.h"
#include "common/async/yield_context.h"
#include "cls/2pc_queue/cls_2pc_queue_types.h"
#include "rgw_pubsub.h"

// forward declarations
namespace rgw::sal {
    class RadosStore;
    class RGWObject;
}

class RGWRados;
struct rgw_obj_key;

namespace rgw::notify {

// initialize the notification manager
// notification manager is dequeuing the 2-phase-commit queues
// and send the notifications to the endpoints
bool init(CephContext* cct, rgw::sal::RadosStore* store, const DoutPrefixProvider *dpp);

// shutdown the notification manager
void shutdown();

// create persistent delivery queue for a topic (endpoint)
// this operation also add a topic name to the common (to all RGWs) list of all topics
int add_persistent_topic(const std::string& topic_name, optional_yield y);

// remove persistent delivery queue for a topic (endpoint)
// this operation also remove the topic name from the common (to all RGWs) list of all topics
int remove_persistent_topic(const std::string& topic_name, optional_yield y);

// same as the above, expect you need to provide the IoCtx, the above uses rgw::notify::Manager::rados_ioctx
int remove_persistent_topic(const DoutPrefixProvider* dpp, librados::IoCtx& rados_ioctx, const std::string& topic_name, optional_yield y);

// struct holding reservation information
// populated in the publish_reserve call
// then used to commit or abort the reservation
struct reservation_t {
  struct topic_t {
    topic_t(const std::string& _configurationId, const rgw_pubsub_topic& _cfg,
	    cls_2pc_reservation::id_t _res_id) :
      configurationId(_configurationId), cfg(_cfg), res_id(_res_id) {}

    const std::string configurationId;
    const rgw_pubsub_topic cfg;
    // res_id is reset after topic is committed/aborted
    cls_2pc_reservation::id_t res_id;
  };

  const DoutPrefixProvider* const dpp;
  std::vector<topic_t> topics;
  rgw::sal::RadosStore* const store;
  const req_state* const s;
  size_t size;
  rgw::sal::Object* const object;
  rgw::sal::Object* const src_object; // may differ from object
  rgw::sal::Bucket* const bucket;
  const std::string* const object_name;
  boost::optional<const RGWObjTags&> tagset;
  meta_map_t x_meta_map; // metadata cached by value
  bool metadata_fetched_from_attributes;
  const std::string user_id;
  const std::string user_tenant;
  const std::string req_id;
  optional_yield yield;

  /* ctor for rgw_op callers */
  reservation_t(const DoutPrefixProvider* _dpp,
		rgw::sal::RadosStore* _store,
		const req_state* _s,
		rgw::sal::Object* _object,
		rgw::sal::Object* _src_object,
		const std::string* _object_name,
		optional_yield y);

  /* ctor for non-request caller (e.g., lifecycle) */
  reservation_t(const DoutPrefixProvider* _dpp,
		rgw::sal::RadosStore* _store,
		rgw::sal::Object* _object,
		rgw::sal::Object* _src_object,
		rgw::sal::Bucket* _bucket,
		const std::string& _user_id,
		const std::string& _user_tenant,
		const std::string& _req_id,
		optional_yield y);

  // dtor doing resource leak guarding
  // aborting the reservation if not already committed or aborted
  ~reservation_t();
};



struct rgw_topic_stats {
  std::size_t queue_reservations; // number of reservations
  uint64_t queue_size;            // in bytes
  uint32_t queue_entries;         // number of entries

  void dump(Formatter *f) const;
};

// create a reservation on the 2-phase-commit queue
int publish_reserve(const DoutPrefixProvider *dpp,
		      EventType event_type,
		      reservation_t& reservation,
		      const RGWObjTags* req_tags);

// commit the reservation to the queue
int publish_commit(rgw::sal::Object* obj,
        uint64_t size,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        const std::string& version,
        EventType event_type,
        reservation_t& reservation,
        const DoutPrefixProvider *dpp);

// cancel the reservation
int publish_abort(reservation_t& reservation);

int get_persistent_queue_stats_by_topic_name(const DoutPrefixProvider *dpp, librados::IoCtx &rados_ioctx,
                                             const std::string &topic_name, rgw_topic_stats &stats, optional_yield y);

}

