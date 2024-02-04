#include "svc_topic_rados.h"
#include "rgw_notify.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "svc_meta.h"
#include "svc_meta_be_sobj.h"
#include "svc_zone.h"

#define dout_subsys ceph_subsys_rgw

static std::string topic_oid_prefix = "topic.";
static constexpr char topic_tenant_delim[] = ":";
static std::string bucket_topic_oid_prefix = "buckets.";

std::string get_topic_key(const std::string& topic_name,
                          const std::string& tenant) {
  if (tenant.empty()) {
    return topic_name;
  }
  return tenant + topic_tenant_delim + topic_name;
}

void parse_topic_entry(const std::string& topic_entry,
                       std::string* tenant_name,
                       std::string* topic_name) {
  // expected format: [tenant_name:]topic_name*
  auto pos = topic_entry.find(topic_tenant_delim);
  if (pos != std::string::npos) {
    *tenant_name = topic_entry.substr(0, pos);
    *topic_name = topic_entry.substr(pos + 1);
  } else {
    tenant_name->clear();
    *topic_name = topic_entry;
  }
}

std::string get_bucket_topic_mapping_oid(const rgw_pubsub_topic& topic) {
  return bucket_topic_oid_prefix + get_topic_key(topic.name, topic.user.tenant);
}

class RGWSI_Topic_Module : public RGWSI_MBSObj_Handler_Module {
  RGWSI_Topic_RADOS::Svc& svc;
  const std::string prefix;

 public:
  RGWSI_Topic_Module(RGWSI_Topic_RADOS::Svc& _svc)
      : RGWSI_MBSObj_Handler_Module("topic"),
        svc(_svc),
        prefix(topic_oid_prefix) {}

  void get_pool_and_oid(const std::string& key,
                        rgw_pool* pool,
                        std::string* oid) override {
    if (pool) {
      *pool = svc.zone->get_zone_params().topics_pool;
    }

    if (oid) {
      *oid = key_to_oid(key);
    }
  }

  bool is_valid_oid(const std::string& oid) override {
    return boost::algorithm::starts_with(oid, prefix);
  }

  std::string key_to_oid(const std::string& key) override {
    return prefix + key;
  }

  // This is called after `is_valid_oid` and is assumed to be a valid oid
  std::string oid_to_key(const std::string& oid) override {
    return oid.substr(prefix.size());
  }

  const std::string& get_oid_prefix() { return prefix; }
};

RGWSI_MetaBackend_Handler* RGWSI_Topic_RADOS::get_be_handler() {
  return be_handler;
}

void RGWSI_Topic_RADOS::init(RGWSI_Zone* _zone_svc,
                             RGWSI_Meta* _meta_svc,
                             RGWSI_MetaBackend* _meta_be_svc,
                             RGWSI_SysObj* _sysobj_svc) {
  svc.zone = _zone_svc;
  svc.meta = _meta_svc;
  svc.meta_be = _meta_be_svc;
  svc.sysobj = _sysobj_svc;
}

int RGWSI_Topic_RADOS::do_start(optional_yield y,
                                const DoutPrefixProvider* dpp) {
  int r = svc.meta->create_be_handler(RGWSI_MetaBackend::Type::MDBE_SOBJ,
                                      &be_handler);
  if (r < 0) {
    ldout(ctx(), 0) << "ERROR: failed to create be_handler for Topics: r=" << r
                    << dendl;
    return r;
  }

  auto module = new RGWSI_Topic_Module(svc);
  RGWSI_MetaBackend_Handler_SObj* bh =
      static_cast<RGWSI_MetaBackend_Handler_SObj*>(be_handler);
  be_module.reset(module);
  bh->set_module(module);
  return 0;
}

RGWTopicMetadataHandler::RGWTopicMetadataHandler(rgw::sal::Driver* driver,
                                                 RGWSI_Topic_RADOS* topic_svc) {
  this->driver = driver;
  this->topic_svc = topic_svc;
  base_init(topic_svc->ctx(), topic_svc->get_be_handler());
}

RGWMetadataObject* RGWTopicMetadataHandler::get_meta_obj(
    JSONObj* jo, const obj_version& objv, const ceph::real_time& mtime) {
  rgw_pubsub_topic topic;
  try {
    topic.decode_json(jo);
  } catch (JSONDecoder::err& e) {
    return nullptr;
  }

  return new RGWTopicMetadataObject(topic, objv, mtime, driver);
}

int RGWTopicMetadataHandler::do_get(RGWSI_MetaBackend_Handler::Op* op,
                                    std::string& entry, RGWMetadataObject** obj,
                                    optional_yield y,
                                    const DoutPrefixProvider* dpp) {
  rgw_pubsub_topic result;
  std::string topic_name;
  std::string tenant;
  parse_topic_entry(entry, &tenant, &topic_name);
  RGWObjVersionTracker objv;
  // TODO: read metadata directly from rados, without calling through
  // sal::Driver::read_topic_v2()
  int ret = driver->read_topic_v2(topic_name, tenant, result, &objv, y, dpp);
  if (ret < 0) {
    return ret;
  }
  ceph::real_time mtime;
  RGWTopicMetadataObject* rdo =
      new RGWTopicMetadataObject(result, objv.read_version, mtime, driver);
  *obj = rdo;
  return 0;
}

int RGWTopicMetadataHandler::do_remove(RGWSI_MetaBackend_Handler::Op* op,
                                       std::string& entry,
                                       RGWObjVersionTracker& objv_tracker,
                                       optional_yield y,
                                       const DoutPrefixProvider* dpp) {
  auto ret = rgw::notify::remove_persistent_topic(entry, y);
  if (ret != -ENOENT && ret < 0) {
    return ret;
  }
  std::string topic_name;
  std::string tenant;
  parse_topic_entry(entry, &tenant, &topic_name);
  // TODO: remove metadata directly from rados, without calling through
  // sal::Driver::remove_topic_v2()
  return driver->remove_topic_v2(topic_name, tenant, &objv_tracker, y, dpp);
}

class RGWMetadataHandlerPut_Topic : public RGWMetadataHandlerPut_SObj {
  RGWTopicMetadataHandler* rhandler;
  RGWTopicMetadataObject* mdo;

 public:
  RGWMetadataHandlerPut_Topic(RGWTopicMetadataHandler* handler,
                              RGWSI_MetaBackend_Handler::Op* op,
                              std::string& entry, RGWMetadataObject* obj,
                              RGWObjVersionTracker& objv_tracker,
                              optional_yield y, RGWMDLogSyncType type,
                              bool from_remote_zone)
      : RGWMetadataHandlerPut_SObj(handler, op, entry, obj, objv_tracker, y,
                                   type, from_remote_zone),
        rhandler(handler) {
    mdo = static_cast<RGWTopicMetadataObject*>(obj);
  }

  int put_checked(const DoutPrefixProvider* dpp) override {
    auto& topic = mdo->get_topic_info();
    auto* driver = mdo->get_driver();
    auto ret = rgw::notify::add_persistent_topic(entry, y);
    if (ret < 0) {
      return ret;
    }
    RGWObjVersionTracker objv_tracker;
    // TODO: write metadata directly from rados, without calling through
    // sal::Driver::write_topic_v2()
    ret = driver->write_topic_v2(topic, &objv_tracker, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to write topic info: ret=" << ret
                        << dendl;
    }
    return ret;
  }
};

int RGWTopicMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op* op,
                                    std::string& entry, RGWMetadataObject* obj,
                                    RGWObjVersionTracker& objv_tracker,
                                    optional_yield y,
                                    const DoutPrefixProvider* dpp,
                                    RGWMDLogSyncType type,
                                    bool from_remote_zone) {
  RGWMetadataHandlerPut_Topic put_op(this, op, entry, obj, objv_tracker, y,
                                     type, from_remote_zone);
  return do_put_operate(&put_op, dpp);
}
