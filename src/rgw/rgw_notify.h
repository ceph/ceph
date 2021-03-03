// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

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
    class RGWRadosStore;
    class RGWObject;
}

class RGWRados;
struct rgw_obj_key;

namespace rgw::notify {

// initialize the notification manager
// notification manager is dequeing the 2-phase-commit queues
// and send the notifications to the endpoints
bool init(CephContext* cct, rgw::sal::RGWRadosStore* store, const DoutPrefixProvider *dpp);

// shutdown the notification manager
void shutdown();

// create persistent delivery queue for a topic (endpoint)
// this operation also add a topic name to the common (to all RGWs) list of all topics
int add_persistent_topic(const std::string& topic_name, optional_yield y);

// remove persistent delivery queue for a topic (endpoint)
// this operation also remove the topic name from the common (to all RGWs) list of all topics
int remove_persistent_topic(const std::string& topic_name, optional_yield y);

// struct holding reservation information
// populated in the publish_reserve call
// then used to commit or abort the reservation
struct reservation_t {
  struct topic_t {
    topic_t(std::string& _configurationId, rgw_pubsub_topic& _cfg,
	    cls_2pc_reservation::id_t _res_id) :
      configurationId(_configurationId), cfg(_cfg), res_id(_res_id) {}

    std::string configurationId;
    rgw_pubsub_topic cfg;
    // res_id is reset after topic is committed/aborted
    cls_2pc_reservation::id_t res_id;
  };
  
  std::vector<topic_t> topics;
  rgw::sal::RGWRadosStore* store;
  size_t size;

  const DoutPrefixProvider* dpp;
  RGWObjectCtx* obj_ctx;
  rgw::sal::RGWObject* object;
  rgw::sal::RGWObject* /* const */ src_object; // may differ from object
  rgw::sal::RGWBucket* /* const */ bucket;
  boost::optional<RGWObjTags&> tagset;
  boost::optional<meta_map_t&> x_meta_map;
  std::string user_id;
  std::string user_tenant;
  std::string req_id;
  optional_yield yield;

  /* ctor for rgw_op callers */
  reservation_t(rgw::sal::RGWRadosStore* _store,
		req_state* _s,
		const DoutPrefixProvider* _dpp,
		rgw::sal::RGWObject* _object,
		rgw::sal::RGWObject* _src_object) : 
    store(_store), dpp(_s), obj_ctx(_s->obj_ctx),
    object(_object), src_object(_src_object),
    bucket(_s->bucket.get()),
    tagset(_s->tagset),
    x_meta_map(_s->info.x_meta_map),
    user_id(_s->user->get_id().id),
    user_tenant(_s->user->get_id().tenant),
    req_id(_s->req_id),
    yield(_s->yield)
    {}

  /* ctor for non-request caller (e.g., lifecycle) */
  reservation_t(rgw::sal::RGWRadosStore* _store,
		const DoutPrefixProvider* _dpp,
		RGWObjectCtx* _obj_ctx,
		rgw::sal::RGWObject* _object,
		rgw::sal::RGWBucket* _bucket,
		std::string& _user_id,
		std::string& _user_tenant,
		std::string& _req_id,
		optional_yield y) :
    store(_store), dpp(_dpp), obj_ctx(_obj_ctx),
    object(_object), src_object(nullptr),
    bucket(_bucket),
    user_id(_user_id),
    user_tenant(_user_tenant),
    req_id(_req_id),
    yield(y)
    {}

  // dtor doing resource leak guarding
  // aborting the reservation if not already committed or aborted
  ~reservation_t();
};

// create a reservation on the 2-phase-commit queue
int publish_reserve(EventType event_type,
        reservation_t& reservation,
        const RGWObjTags* req_tags);

// commit the reservation to the queue
int publish_commit(rgw::sal::RGWObject* obj,
        uint64_t size,
        const ceph::real_time& mtime, 
        const std::string& etag, 
        EventType event_type,
        reservation_t& reservation,
        const DoutPrefixProvider *dpp);

// cancel the reservation
int publish_abort(reservation_t& reservation);

}

