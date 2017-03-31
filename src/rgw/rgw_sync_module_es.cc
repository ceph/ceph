#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_boost_asio_yield.h"
#include "rgw_sync_module_es.h"
#include "rgw_sync_module_es_rest.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"
#include "rgw_op.h"
#include "rgw_es_query.h"

#define dout_subsys ceph_subsys_rgw

struct ElasticConfig {
  string id;
  RGWRESTConn *conn{nullptr};
};

static string es_get_index_path(const RGWRealm& realm)
{
  string path = "/rgw-" + realm.get_name();
  return path;
}

static string es_get_obj_path(const RGWRealm& realm, const RGWBucketInfo& bucket_info, const rgw_obj_key& key)
{
  string path = "/rgw-" + realm.get_name() + "/object/" + bucket_info.bucket.bucket_id + ":" + key.name + ":" + key.instance;
  return path;
}

struct es_dump_type {
  const char *type;
  const char *format;

  es_dump_type(const char *t, const char *f = nullptr) : type(t), format(f) {}

  void dump(Formatter *f) const {
    encode_json("type", type, f);
    if (format) {
      encode_json("format", format, f);
    }
  }
};

struct es_index_mappings {
  void dump_custom(Formatter *f, const char *section, const char *type, const char *format) const {
    f->open_object_section(section);
    ::encode_json("type", "nested", f);
    f->open_object_section("properties");
    f->open_object_section("name");
    ::encode_json("type", "string", f);
    ::encode_json("index", "not_analyzed", f);
    f->close_section(); // name
    encode_json("value", es_dump_type(type, format), f);
    f->close_section(); // entry
    f->close_section(); // custom-string
  }
  void dump(Formatter *f) const {
    f->open_object_section("mappings");
    f->open_object_section("object");
    f->open_object_section("properties");
    encode_json("bucket", es_dump_type("string"), f);
    encode_json("name", es_dump_type("string"), f);
    encode_json("instance", es_dump_type("string"), f);
    f->open_object_section("meta");
    f->open_object_section("properties");
    encode_json("cache_control", es_dump_type("string"), f);
    encode_json("content_disposition", es_dump_type("string"), f);
    encode_json("content_encoding", es_dump_type("string"), f);
    encode_json("content_language", es_dump_type("string"), f);
    encode_json("content_type", es_dump_type("string"), f);
    encode_json("etag", es_dump_type("string"), f);
    encode_json("expires", es_dump_type("string"), f);
    f->open_object_section("mtime");
    ::encode_json("type", "date", f);
    ::encode_json("format", "strict_date_optional_time||epoch_millis", f);
    f->close_section(); // mtime
    encode_json("size", es_dump_type("long"), f);
    dump_custom(f, "custom-string", "string", nullptr);
    dump_custom(f, "custom-int", "long", nullptr);
    dump_custom(f, "custom-date", "date", "strict_date_optional_time||epoch_millis");
    f->close_section(); // properties
    f->close_section(); // meta
    f->close_section(); // properties
    f->close_section(); // object
    f->close_section(); // mappings
  }
};

struct es_obj_metadata {
  CephContext *cct;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  uint64_t size;
  map<string, bufferlist> attrs;

  es_obj_metadata(CephContext *_cct, const RGWBucketInfo& _bucket_info,
                  const rgw_obj_key& _key, ceph::real_time& _mtime, uint64_t _size,
                  map<string, bufferlist>& _attrs) : cct(_cct), bucket_info(_bucket_info), key(_key),
                                                     mtime(_mtime), size(_size), attrs(std::move(_attrs)) {}

  void dump(Formatter *f) const {
    map<string, string> out_attrs;
    map<string, string> custom_meta;
    RGWAccessControlPolicy policy;
    set<string> permissions;

    for (auto i : attrs) {
      const string& attr_name = i.first;
      string name;
      bufferlist& val = i.second;

      if (attr_name.compare(0, sizeof(RGW_ATTR_PREFIX) - 1, RGW_ATTR_PREFIX) != 0) {
        continue;
      }

      if (attr_name.compare(0, sizeof(RGW_ATTR_META_PREFIX) - 1, RGW_ATTR_META_PREFIX) == 0) {
        name = attr_name.substr(sizeof(RGW_ATTR_META_PREFIX) - 1);
        custom_meta[name] = string(val.c_str(), (val.length() > 0 ? val.length() - 1 : 0));
        continue;
      }

      name = attr_name.substr(sizeof(RGW_ATTR_PREFIX) - 1);

      if (name == "acl") {
        try {
          auto i = val.begin();
          ::decode(policy, i);
        } catch (buffer::error& err) {
          ldout(cct, 0) << "ERROR: failed to decode acl for " << bucket_info.bucket << "/" << key << dendl;
        }

        const RGWAccessControlList& acl = policy.get_acl();

        permissions.insert(policy.get_owner().get_id().to_str());
        for (auto acliter : acl.get_grant_map()) {
          const ACLGrant& grant = acliter.second;
          if (grant.get_type().get_type() == ACL_TYPE_CANON_USER &&
              ((uint32_t)grant.get_permission().get_permissions() & RGW_PERM_READ) != 0) {
            rgw_user user;
            if (grant.get_id(user)) {
              permissions.insert(user.to_str());
            }
          }
        }
      } else {
        if (name != "pg_ver" &&
            name != "source_zone" &&
            name != "idtag") {
          out_attrs[name] = string(val.c_str(), (val.length() > 0 ? val.length() - 1 : 0));
        }
      }
    }
    ::encode_json("bucket", bucket_info.bucket.name, f);
    ::encode_json("name", key.name, f);
    ::encode_json("instance", key.instance, f);
    ::encode_json("owner", policy.get_owner(), f);
    ::encode_json("permissions", permissions, f);
    f->open_object_section("meta");
    ::encode_json("size", size, f);

    string mtime_str;
    rgw_to_iso8601(mtime, &mtime_str);
    ::encode_json("mtime", mtime_str, f);
    for (auto i : out_attrs) {
      ::encode_json(i.first.c_str(), i.second, f);
    }
    map<string, string> custom_str;
    map<string, string> custom_int;
    map<string, string> custom_date;

    for (auto i : custom_meta) {
      auto config = bucket_info.mdsearch_config.find(i.first);
      if (config == bucket_info.mdsearch_config.end()) {
        ldout(cct, 20) << "custom meta entry key=" << i.first << " not found in bucket mdsearch config: " << bucket_info.mdsearch_config << dendl;
        continue;
      }
      switch (config->second) {
        case ESEntityTypeMap::ES_ENTITY_DATE:
          custom_date[i.first] = i.second;
          break;
        case ESEntityTypeMap::ES_ENTITY_INT:
          custom_int[i.first] = i.second;
          break;
        default:
          custom_str[i.first] = i.second;
      }
    }

    if (!custom_str.empty()) {
      f->open_array_section("custom-string");
      for (auto i : custom_str) {
        f->open_object_section("entity");
        ::encode_json("name", i.first.c_str(), f);
        ::encode_json("value", i.second, f);
        f->close_section();
      }
      f->close_section();
    }
    if (!custom_int.empty()) {
      f->open_array_section("custom-int");
      for (auto i : custom_int) {
        f->open_object_section("entity");
        ::encode_json("name", i.first.c_str(), f);
        ::encode_json("value", i.second, f);
        f->close_section();
      }
      f->close_section();
    }
    if (!custom_date.empty()) {
      f->open_array_section("custom-date");
      for (auto i : custom_date) {
        f->open_object_section("entity");
        ::encode_json("name", i.first.c_str(), f);
        ::encode_json("value", i.second, f);
        f->close_section();
      }
      f->close_section();
    }
    f->close_section();
  }
};

class RGWElasticInitConfigCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  const ElasticConfig& conf;
public:
  RGWElasticInitConfigCBCR(RGWDataSyncEnv *_sync_env,
                          const ElasticConfig& _conf) : RGWCoroutine(_sync_env->cct),
                                                        sync_env(_sync_env),
                                                        conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": init elasticsearch config zone=" << sync_env->source_zone << dendl;
      yield {
        string path = es_get_index_path(sync_env->store->get_realm());

        es_index_mappings doc;

        call(new RGWPutRESTResourceCR<es_index_mappings, int>(sync_env->cct, conf.conn,
                                                              sync_env->http_manager,
                                                              path, nullptr /* params */,
                                                              doc, nullptr /* result */));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};

class RGWElasticHandleRemoteObjCBCR : public RGWStatRemoteObjCBCR {
  const ElasticConfig& conf;
public:
  RGWElasticHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                          const ElasticConfig& _conf) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": stat of remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                              << " attrs=" << attrs << dendl;
      yield {
        string path = es_get_obj_path(sync_env->store->get_realm(), bucket_info, key);
        es_obj_metadata doc(sync_env->cct, bucket_info, key, mtime, size, attrs);

        call(new RGWPutRESTResourceCR<es_obj_metadata, int>(sync_env->cct, conf.conn,
                                                            sync_env->http_manager,
                                                            path, nullptr /* params */,
                                                            doc, nullptr /* result */));

      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};

class RGWElasticHandleRemoteObjCR : public RGWCallStatRemoteObjCR {
  const ElasticConfig& conf;
public:
  RGWElasticHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                        const ElasticConfig& _conf) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                           conf(_conf) {
  }

  ~RGWElasticHandleRemoteObjCR() override {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWElasticHandleRemoteObjCBCR(sync_env, bucket_info, key, conf);
  }
};

class RGWElasticRemoveRemoteObjCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  const ElasticConfig& conf;
public:
  RGWElasticRemoveRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                          const ElasticConfig& _conf) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                        bucket_info(_bucket_info), key(_key),
                                                        mtime(_mtime), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": remove remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield {
        string path = es_get_obj_path(sync_env->store->get_realm(), bucket_info, key);

        call(new RGWDeleteRESTResourceCR(sync_env->cct, conf.conn,
                                         sync_env->http_manager,
                                         path, nullptr /* params */));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};

class RGWElasticDataSyncModule : public RGWDataSyncModule {
  ElasticConfig conf;
public:
  RGWElasticDataSyncModule(CephContext *cct, const string& elastic_endpoint) {
    conf.id = string("elastic:") + elastic_endpoint;
    conf.conn = new RGWRESTConn(cct, nullptr, conf.id, { elastic_endpoint });
  }
  ~RGWElasticDataSyncModule() override {
    delete conf.conn;
  }

  RGWCoroutine *init_sync(RGWDataSyncEnv *sync_env) override {
    ldout(sync_env->cct, 0) << conf.id << ": init" << dendl;
    return new RGWElasticInitConfigCBCR(sync_env, conf);
  }
  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << conf.id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWElasticHandleRemoteObjCR(sync_env, bucket_info, key, conf);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    /* versioned and versioned epoch params are useless in the elasticsearch backend case */
    ldout(sync_env->cct, 0) << conf.id << ": rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWElasticRemoveRemoteObjCBCR(sync_env, bucket_info, key, mtime, conf);
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << conf.id << ": create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
  RGWRESTConn *get_rest_conn() {
    return conf.conn;
  }
};

RGWElasticSyncModuleInstance::RGWElasticSyncModuleInstance(CephContext *cct, const string& endpoint)
{
  data_handler = std::unique_ptr<RGWElasticDataSyncModule>(new RGWElasticDataSyncModule(cct, endpoint));
}

RGWDataSyncModule *RGWElasticSyncModuleInstance::get_data_handler()
{
  return data_handler.get();
}

RGWRESTConn *RGWElasticSyncModuleInstance::get_rest_conn()
{
  return data_handler->get_rest_conn();
}

string RGWElasticSyncModuleInstance::get_index_path(const RGWRealm& realm) {
  return es_get_index_path(realm);
}

RGWRESTMgr *RGWElasticSyncModuleInstance::get_rest_filter(int dialect, RGWRESTMgr *orig) {
  if (dialect != RGW_REST_S3) {
    return orig;
  }
  return new RGWRESTMgr_MDSearch_S3(this);
}

int RGWElasticSyncModule::create_instance(CephContext *cct, map<string, string>& config, RGWSyncModuleInstanceRef *instance) {
  string endpoint;
  auto i = config.find("endpoint");
  if (i != config.end()) {
    endpoint = i->second;
  }
  instance->reset(new RGWElasticSyncModuleInstance(cct, endpoint));
  return 0;
}

