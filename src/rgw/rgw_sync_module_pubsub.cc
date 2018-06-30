#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_pubsub.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_tools.h"
#include "rgw_op.h"
#include "rgw_pubsub.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw


/*

config:

{
   "tenant": <tenant>,             # default: <empty>
   "uid": <uid>,                   # default: "pubsub"
   "data_bucket_prefix": <prefix>  # default: "pubsub-"
   "data_oid_prefix": <prefix>     #

    # non-dynamic config
    "notifications": [
        {
            "path": <notification-path>,    # this can be either an explicit path: <bucket>, or <bucket>/<object>,
                                            # or a prefix if it ends with a wildcard
            "topic": <topic-name>
         },
        ...
    ],
    "subscriptions": [
        {
            "name": <subscription-name>,
            "topic": <topic>,
            "push_endpoint": <endpoint>,
            "data_bucket": <bucket>,       # override name of bucket where subscription data will be store
            "data_oid_prefix": <prefix>    # set prefix for subscription data object ids
        },
        ...
    ]
}

*/

struct PSSubConfig { /* subscription config */
  string name;
  string topic;
  string push_endpoint;

  string data_bucket_name;
  string data_prefix;

  void dump(Formatter *f) const {
    encode_json("name", name, f);
    encode_json("topic", topic, f);
    encode_json("push_endpoint", push_endpoint, f);
    encode_json("data_bucket_name", data_bucket_name, f);
    encode_json("data_oid_prefix", data_oid_prefix, f);
  }

  void init(CephContext *cct, const JSONFormattable& config,
            const string& data_bucket_prefix) {
    name = config["name"];
    topic = config["topic"];
    push_endpoint = config["push_endpoint"];
    string default_bucket_name = data_prefix + name;
    data_bucket_name = config["data_bucket"](default_bucket_name.c_str());
    data_prefix = config["data_prefix"];
  }
};

using  PSSubConfigRef = std::shared_ptr<PSSubConfig>;

struct PSTopicConfig {
  string name;
  set<string> subs;
};

struct PSNotificationConfig {
  string path; /* a path or a path prefix that would trigger the event (prefix: if ends with a wildcard) */
  string topic;

  uint64_t id{0};
  bool is_prefix{false};

  void init(CephContext *cct, const JSONFormattable& config) {
    path = config["path"];
    if (!path.empty() && path[path.size() - 1] == '*') {
      path = path.substr(0, path.size() - 1);
      is_prefix = true;
    }
    topic = config["topic"];
  }
};

template<class T>
static string json_str(const char *name, const T& obj, bool pretty = false)
{
  stringstream ss;
  JSONFormatter f(pretty);

  encode_json(name, obj, &f);
  f.flush(ss);

  return ss.str();
}


struct PSConfig {
  string id{"pubsub"};
  rgw_user user;
  string data_bucket_prefix;

  uint64_t sync_instance{0};
  uint32_t num_pub_shards{0};
  uint32_t num_topic_shards{0};
  uint64_t max_id{0};


  /* FIXME: no hard coded buckets, we'll have configurable topics */
  map<string, PSSubConfigRef> subs;
  map<string, PSTopicConfig> topics;
  multimap<string, PSNotificationConfig> notifications;

  void init(CephContext *cct, const JSONFormattable& config) {
    string uid = config["uid"]("pubsub");
    user = rgw_user(config["tenant"], uid);
    data_bucket_prefix = config["data_bucket_prefix"]("pubsub");

    num_pub_shards = config["num_pub_shards"](PS_NUM_PUB_SHARDS_DEFAULT);
    if (num_pub_shards < PS_NUM_PUB_SHARDS_MIN) {
      num_pub_shards = PS_NUM_PUB_SHARDS_MIN;
    }

    num_topic_shards = config["num_topic_shards"](PS_NUM_TOPIC_SHARDS_DEFAULT);
    if (num_topic_shards < PS_NUM_TOPIC_SHARDS_MIN) {
      num_topic_shards = PS_NUM_TOPIC_SHARDS_MIN;
    }
    /* FIXME: this will be dynamically configured */
    for (auto& c : config["notifications"].array()) {
      PSNotificationConfig nc;
      nc.id = ++max_id;
      nc.init(cct, c);
      notifications.insert(std::make_pair(nc.path, nc));

      PSTopicConfig topic_config = { .name = nc.topic };
      topics[nc.topic] = topic_config;
    }
    for (auto& c : config["subscriptions"].array()) {
      auto sc = std::make_shared<PSSubConfig>();
      sc->init(cct, c, data_bucket_prefix);
      subs[sc->name] = sc;
      topics[sc->topic].subs.insert(sc->name);
    }

    ldout(cct, 5) << "pubsub: module config (parsed representation):\n" << json_str("config", *this, true) << dendl;
  }

  void init_instance(RGWRealm& realm, uint64_t instance_id) {
    sync_instance = instance_id;
  }

  void get_topics(CephContext *cct, const RGWBucketInfo& bucket_info, const rgw_obj_key& key, vector<PSTopicConfig *> *result) {
    string path = bucket_info.bucket.name + "/" + key.name;

    result->clear();

    auto iter = notifications.upper_bound(path);
    if (iter == notifications.begin()) {
      return;
    }

    do {
      --iter;
      if (iter->first.size() > path.size()) {
        break;
      }
      if (path.compare(0, iter->first.size(), iter->first) != 0) {
        break;
      }

      PSNotificationConfig& target = iter->second;

      if (!target.is_prefix &&
          path.size() != iter->first.size()) {
        continue;
      }

      auto topic = topics.find(target.topic);
      if (topic == topics.end()) {
        continue;
      }

      ldout(cct, 10) << ": found topic for path=" << bucket_info.bucket << "/" << key << ": id=" << target.id << " target_path=" << target.path << ", topic=" << target.topic << dendl;
      result->push_back(&topic->second);
    } while (iter != notifications.begin());
  }

  bool find_sub(const string& name, PSSubConfigRef *ref) {
    auto iter = subs.find(name);
    if (iter != subs.end()) {
      *ref = iter->second;
      return true;
    }
    return false;
  }
};

using PSConfigRef = std::shared_ptr<PSConfig>;
using EventRef = std::shared_ptr<rgw_pubsub_event>;

static void make_event_ref(EventRef *event) {
  *event = std::make_shared<rgw_pubsub_event>();
}

class PSManager;
using PSManagerRef = std::shared_ptr<PSManager>;

struct PSEnv {
  PSConfigRef conf;
  shared_ptr<RGWUserInfo> data_user_info;
  PSManagerRef manager;

  PSEnv() : conf(make_shared<PSConfig>()),
            data_user_info(make_shared<RGWUserInfo>()) {}

  void init(CephContext *cct, const JSONFormattable& config) {
    conf->init(cct, config);
  }

  void init_instance(RGWRealm& realm, uint64_t instance_id, PSManagerRef& mgr);
};

using PSEnvRef = std::shared_ptr<PSEnv>;

class PSEvent {
  EventRef event;

public:
  PSEvent(EventRef& _event) : event(_event) {}

  string generate_message_id() {
    char buf[64];
    utime_t ts(event->timestamp);

    string etag;
    RGWMD5Etag hash;
    hash.update(event->bucket.bucket_id);
    hash.update(event->key.name);
    hash.update(event->key.instance);
    hash.finish(&etag);

    assert(etag.size() > 8);

    etag = etag.substr(0, 8);
    snprintf(buf, sizeof(buf), "%010ld.%06ld.%s", (long)ts.sec(), (long)ts.usec(), etag.c_str());

    return buf;
  }

  void format(bufferlist *bl) {
    stringstream ss;
    JSONFormatter f;

    encode_json("event", *event, &f);
    f.flush(ss);

    bl->append(ss.str());
  }
        
};

class PSSubscription : public RefCountedObject {
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  PSSubConfigRef sub_conf;
  shared_ptr<rgw_get_bucket_info_result> get_bucket_info_result;
  RGWBucketInfo *bucket_info{nullptr};
  RGWDataAccessRef data_access;
  RGWDataAccess::BucketRef bucket;

public:

  class InitCR : public RGWCoroutine {
    RGWDataSyncEnv *sync_env;
    PSSubscriptionRef sub;
    rgw_get_bucket_info_params get_bucket_info;
    rgw_bucket_create_local_params create_bucket;
    PSConfigRef& conf;
    PSSubConfigRef& sub_conf;
    int i;
  public:
    InitCR(RGWDataSyncEnv *_sync_env,
           PSSubscriptionRef& _sub) : RGWCoroutine(_sync_env->cct),
                                    sync_env(_sync_env),
                                    sub(_sub), conf(sub->env->conf),
                                    sub_conf(sub->sub_conf) {
    }

    int operate() override {
      reenter(this) {
        get_bucket_info.tenant = conf->user.tenant;
        get_bucket_info.bucket_name = sub_conf->data_bucket_name;
        sub->get_bucket_info_result = make_shared<rgw_get_bucket_info_result>();

        for (i = 0; i < 2; ++i) {
          yield call(new RGWGetBucketInfoCR(sync_env->async_rados,
                                            sync_env->store,
                                            get_bucket_info,
                                            sub->get_bucket_info_result));
          if (retcode < 0 && retcode != -ENOENT) {
            ldout(sync_env->cct, 0) << "ERROR: failed to geting bucket info: " << "tenant="
              << get_bucket_info.tenant << " name=" << get_bucket_info.bucket_name << ": ret=" << retcode << dendl;
          }
          if (retcode == 0) {
            auto& result = sub->get_bucket_info_result;
            sub->bucket_info = &result->bucket_info;

            int ret = sub->data_access->get_bucket(result->bucket_info, result->attrs, &sub->bucket);
            if (ret < 0) {
              ldout(sync_env->cct, 0) << "ERROR: data_access.get_bucket() bucket=" << result->bucket_info.bucket << " failed, ret=" << ret << dendl;
              return set_cr_error(ret);
            }
            return set_cr_done();
          }

          create_bucket.user_info = sub->env->data_user_info;
          create_bucket.bucket_name = sub_conf->data_bucket_name;
          ldout(sync_env->cct, 20) << "pubsub: bucket create: using user info: " << json_str("obj", *sub->env->data_user_info, true) << dendl;
          yield call(new RGWBucketCreateLocalCR(sync_env->async_rados,
                                                sync_env->store,
                                                create_bucket));
          if (retcode < 0) {
            ldout(sync_env->cct, 0) << "ERROR: failed to create bucket: " << "tenant="
              << get_bucket_info.tenant << " name=" << get_bucket_info.bucket_name << ": ret=" << retcode << dendl;
            return set_cr_error(retcode);
          }
ldout(sync_env->cct, 20) << "pubsub: bucket create: after user info: " << json_str("obj", *sub->env->data_user_info, true) << dendl;

          /* second iteration: we got -ENOENT and created a bucket */
        }

        /* failed twice on -ENOENT, unexpected */
        ldout(sync_env->cct, 0) << "ERROR: failed to create bucket " << "tenant=" << get_bucket_info.tenant
          << " name=" << get_bucket_info.bucket_name << dendl;
        return set_cr_error(-EIO);
      }
      return 0;
    }
  };

  class StoreEventCR : public RGWCoroutine {
    RGWDataSyncEnv *sync_env;
    PSSubscriptionRef sub;
    PSEvent pse;
    PSConfigRef& conf;
    PSSubConfigRef& sub_conf;
    rgw_object_simple_put_params put_obj;
    int i;
  public:
    StoreEventCR(RGWDataSyncEnv *_sync_env,
                 PSSubscriptionRef& _sub,
                 EventRef& _event) : RGWCoroutine(_sync_env->cct),
                                     sync_env(_sync_env),
                                     sub(_sub),
                                     pse(_event),
                                     conf(sub->env->conf),
                                     sub_conf(sub->sub_conf) {
    }

    int operate() override {
      reenter(this) {

        put_obj.bucket = sub->bucket;
        put_obj.key = rgw_obj_key(pse.generate_message_id());

        pse.format(&put_obj.data);
        
        yield call(new RGWObjectSimplePutCR(sync_env->async_rados,
                                            sync_env->store,
                                            put_obj));
      }
      return 0;
    }
  };

public:
  PSSubscription(RGWDataSyncEnv *_sync_env,
                 PSEnvRef _env,
                 PSSubConfigRef& _sub_conf) : sync_env(_sync_env),
                                      env(_env),
                                      sub_conf(_sub_conf),
                                      data_access(std::make_shared<RGWDataAccess>(sync_env->store)) {}

  RGWCoroutine *init_cr() {
    return new InitCR(sync_env, this);
  }

  RGWCoroutine *store_event_cr(EventRef& event) {
    return new StoreEventCR(sync_env, this, event);
  }

  friend class InitCR;
};

using PSSubscriptionRef = std::shared_ptr<PSSubscription>;


class PSManager : public RefCountedObject
{
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;

  map<string, PSSubscriptionRef> subs;

  class GetSubCR : public RGWCoroutine {
    RGWDataSyncEnv *sync_env;
    PSManagerRef mgr;
    const string& sub_name;
    PSSubscriptionRef *ref;

    PSConfigRef& conf;

    PSSubConfigRef sub_conf;
  public:
    GetSubCR(RGWDataSyncEnv *_sync_env,
                      PSManagerRef& _mgr,
                      const string& _sub_name,
                      PSSubscriptionRef *_ref) : RGWCoroutine(_sync_env->cct),
                                                 sync_env(_sync_env),
                                                 mgr(_mgr),
                                                 sub_name(_sub_name),
                                                 ref(_ref),
                                                 conf(mgr->env->conf) {
    }

    int operate() override {
      reenter(this) {
        if (!conf->find_sub(sub_name, &sub_conf)) {
          ldout(sync_env->cct, 0) << "ERROR: could not find subscription config: name=" << sub_name << dendl;
          return set_cr_error(-ENOENT);
        }

        *ref = PSSubscription::get_shared(sync_env, mgr->env, sub_conf);

        yield call((*ref)->init_cr());
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to init subscription" << dendl;
          ref->reset();
          return set_cr_error(retcode);
        }

        mgr->subs[sub_name] = *ref;
        return set_cr_done();
      }
      return 0;
    }
  };

  bool find_sub_instance(const string& sub_name, PSSubscriptionRef *sub) {
    auto iter = subs.find(sub_name);
    if (iter != subs.end()) {
      *sub = iter->second;
      return true;
    }
    return false;
  }

  PSManager(RGWDataSyncEnv *_sync_env,
            PSEnvRef _env) : sync_env(_sync_env),
                             env(_env) {}

public:
  static PSManagerRef& get_shared(RGWDataSyncEnv *_sync_env,
                                 PSEnvRef _env) {
    auto mgr = new PSManager(_sync_env, _env);
    mgr->self = std::shared_ptr<PSManager>(mgr);
    return mgr->self;
  }

  RGWCoroutine *get_subscription_cr(const string& sub_name, PSSubscriptionRef *ref) {
    if (find_sub_instance(sub_name, ref)) {
      /* found it! nothing to execute */
      return nullptr;
    }
    return new GetSubCR(sync_env, self, sub_name, ref);
  }

  friend class GetSubCR;
};

void PSEnv::init_instance(RGWRealm& realm, uint64_t instance_id, PSManagerRef& mgr) {
  manager = mgr;
  conf->init_instance(realm, instance_id);
}

class RGWPSInitEnvCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  PSConfigRef& conf;

  rgw_user_create_params create_user;
  rgw_get_user_info_params get_user_info;
public:
  RGWPSInitEnvCBCR(RGWDataSyncEnv *_sync_env,
                       PSEnvRef _env) : RGWCoroutine(_sync_env->cct),
                                                    sync_env(_sync_env),
                                                    env(_env), conf(env->conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": init pubsub config zone=" << sync_env->source_zone << dendl;

      /* nothing to do here right now */
      create_user.user = conf->user;
      create_user.max_buckets = 0; /* unlimited */
      create_user.display_name = "pubsub";
      create_user.generate_key = false;
      yield call(new RGWUserCreateCR(sync_env->async_rados, sync_env->store, create_user));
      if (retcode < 0) {
        ldout(sync_env->store->ctx(), 0) << "ERROR: failed to create rgw user: ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }

      get_user_info.user = conf->user;
      yield call(new RGWGetUserInfoCR(sync_env->async_rados, sync_env->store, get_user_info, env->data_user_info));
      if (retcode < 0) {
        ldout(sync_env->store->ctx(), 0) << "ERROR: failed to create rgw user: ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }
    return 0;
  }
};

class RGWPSHandleRemoteObjCBCR : public RGWStatRemoteObjCBCR {
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  uint64_t versioned_epoch;
  vector<PSTopicConfig *> topics;
  vector<PSTopicConfig *>::iterator titer;
  set<string>::iterator siter;
  PSSubscriptionRef sub;
  EventRef event;
public:
  RGWPSHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                          PSEnvRef _env, uint64_t _versioned_epoch) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key),
                                                                      sync_env(_sync_env),
                                                                      env(_env),
                                                                      versioned_epoch(_versioned_epoch) {
#warning this will need to change obviously
    env->conf->get_topics(sync_env->cct, _bucket_info, _key, &topics);
  }
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 10) << ": stat of remote obj: z=" << sync_env->source_zone
                               << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                               << " attrs=" << attrs << dendl;
      make_event_ref(&event);
      event->bucket = bucket_info.bucket;
      event->key = key;
      event->event = OBJECT_CREATE;
      event->timestamp = real_clock::now();

      ldout(sync_env->cct, 20) << "pubsub: " << topics.size() << " topics found for path" << dendl;
#warning more event init

      for (titer = topics.begin(); titer != topics.end(); ++titer) {
        ldout(sync_env->cct, 10) << ": notification for " << bucket_info.bucket << "/" << key << ": topic=" << (*titer)->name << ", has " << (*titer)->subs.size() << " subscriptions" << dendl;

        for (siter = (*titer)->subs.begin(); siter != (*titer)->subs.end(); ++siter) {
          ldout(sync_env->cct, 10) << ": subscription: " << *siter << dendl;

          yield call(env->manager->get_subscription_cr(*siter, &sub));
          if (retcode < 0) {
            ldout(sync_env->cct, 10) << "ERROR: failed to find subscription config for subscription=" << *siter << " ret=" << retcode << dendl;
            continue;
          }

          yield call(sub->store_event_cr(event));
          if (retcode < 0) {
            ldout(sync_env->cct, 10) << "ERROR: failed to store event for subscription=" << *siter << " ret=" << retcode << dendl;
            continue;
          }

#warning publish notification
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }
      }
      return set_cr_done();
    }
    return 0;
  }
};

class RGWPSHandleRemoteObjCR : public RGWCallStatRemoteObjCR {
  PSEnvRef env;
  uint64_t versioned_epoch;
public:
  RGWPSHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                        PSEnvRef _env, uint64_t _versioned_epoch) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                           env(_env), versioned_epoch(_versioned_epoch) {
  }

  ~RGWPSHandleRemoteObjCR() override {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
#warning things need to change
    /* FIXME: we need to create a pre_callback coroutine that decides whether object should
     * actually be handled. Otherwise we fetch info from remote zone about every object, even
     * if we don't intend to handle it.
     */
    return new RGWPSHandleRemoteObjCBCR(sync_env, bucket_info, key, env, versioned_epoch);
  }
};

class RGWPSRemoveRemoteObjCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  PSEnvRef env;
public:
  RGWPSRemoveRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                          PSEnvRef _env) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                        bucket_info(_bucket_info), key(_key),
                                                        mtime(_mtime), env(_env) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 10) << ": remove remote obj: z=" << sync_env->source_zone
                               << " b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield {
#if 0
        string path = conf->get_obj_path(bucket_info, key);

        call(new RGWDeleteRESTResourceCR(sync_env->cct, conf->conn.get(),
                                         sync_env->http_manager,
                                         path, nullptr /* params */));
#endif
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};

class RGWPSDataSyncModule : public RGWDataSyncModule {
  PSEnvRef env;
  PSConfigRef& conf;
public:
  RGWPSDataSyncModule(CephContext *cct, const JSONFormattable& config) : env(std::make_shared<PSEnv>()), conf(env->conf) {
    env->init(cct, config);
  }
  ~RGWPSDataSyncModule() override {}

  void init(RGWDataSyncEnv *sync_env, uint64_t instance_id) override {
    PSManagerRef mgr = PSManager::get_shared(sync_env, env);
    env->init_instance(sync_env->store->get_realm(), instance_id, mgr);
  }

  RGWCoroutine *init_sync(RGWDataSyncEnv *sync_env) override {
    ldout(sync_env->cct, 5) << conf->id << ": init" << dendl;
    return new RGWPSInitEnvCBCR(sync_env, env);
  }
  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
#warning this should be done correctly
#if 0
    if (!conf->should_handle_operation(bucket_info)) {
      ldout(sync_env->cct, 10) << conf->id << ": skipping operation (bucket not approved)" << dendl;
      return nullptr;
    }
#endif
    return new RGWPSHandleRemoteObjCR(sync_env, bucket_info, key, env, versioned_epoch);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    /* versioned and versioned epoch params are useless in the elasticsearch backend case */
    ldout(sync_env->cct, 10) << conf->id << ": rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
#warning this should be done correctly
#if 0
    if (!conf->should_handle_operation(bucket_info)) {
      ldout(sync_env->cct, 10) << conf->id << ": skipping operation (bucket not approved)" << dendl;
      return nullptr;
    }
#endif
    return new RGWPSRemoveRemoteObjCBCR(sync_env, bucket_info, key, mtime, env);
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
#warning requests should be filtered correctly
#if 0
    ldout(sync_env->cct, 10) << conf->id << ": skipping operation (not handled)" << dendl;
#endif
#warning delete markers need to be handled too
    return NULL;
  }
};

RGWPSSyncModuleInstance::RGWPSSyncModuleInstance(CephContext *cct, const JSONFormattable& config)
{
  data_handler = std::unique_ptr<RGWPSDataSyncModule>(new RGWPSDataSyncModule(cct, config));
}

RGWDataSyncModule *RGWPSSyncModuleInstance::get_data_handler()
{
  return data_handler.get();
}

RGWRESTMgr *RGWPSSyncModuleInstance::get_rest_filter(int dialect, RGWRESTMgr *orig) {
#warning REST filter implementation missing
#if 0
  if (dialect != RGW_REST_S3) {
    return orig;
  }
  delete orig;
  return new RGWRESTMgr_MDSearch_S3();
#endif
  return orig;
}

int RGWPSSyncModule::create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) {
  instance->reset(new RGWPSSyncModuleInstance(cct, config));
  return 0;
}

