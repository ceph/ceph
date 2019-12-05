// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "services/svc_zone.h"
#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_pubsub.h"
#include "rgw_sync_module_pubsub_rest.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_cr_tools.h"
#include "rgw_op.h"
#include "rgw_pubsub.h"
#include "rgw_pubsub_push.h"
#include "rgw_notify_event_type.h"
#include "rgw_perf_counters.h"
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
#include "rgw_amqp.h"
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
#include "rgw_kafka.h"
#endif

#include <boost/algorithm/hex.hpp>
#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw


#define PUBSUB_EVENTS_RETENTION_DEFAULT 7

/*

config:

{
   "tenant": <tenant>,             # default: <empty>
   "uid": <uid>,                   # default: "pubsub"
   "data_bucket_prefix": <prefix>  # default: "pubsub-"
   "data_oid_prefix": <prefix>     #
   "events_retention_days": <int>  # default: 7
   "start_with_full_sync" <bool>   # default: false

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
            "push_endpoint_args:" <arg list>.            # any push endpoint specific args (include all args)
            "data_bucket": <bucket>,       # override name of bucket where subscription data will be store
            "data_oid_prefix": <prefix>    # set prefix for subscription data object ids
            "s3_id": <id>                  # in case of S3 compatible notifications, the notification ID will be set here
        },
        ...
    ]
}

*/

// utility function to convert the args list from string format 
// (ampresend separated with equal sign) to prased structure
RGWHTTPArgs string_to_args(const std::string& str_args) {
  RGWHTTPArgs args;
  args.set(str_args);
  args.parse();
  return args;
}

struct PSSubConfig {
  std::string name;
  std::string topic;
  std::string push_endpoint_name;
  std::string push_endpoint_args;
  std::string data_bucket_name;
  std::string data_oid_prefix;
  std::string s3_id;
  std::string arn_topic;
  RGWPubSubEndpoint::Ptr push_endpoint;

  void from_user_conf(CephContext *cct, const rgw_pubsub_sub_config& uc) {
    name = uc.name;
    topic = uc.topic;
    push_endpoint_name = uc.dest.push_endpoint;
    data_bucket_name = uc.dest.bucket_name;
    data_oid_prefix = uc.dest.oid_prefix;
    s3_id = uc.s3_id;
    arn_topic = uc.dest.arn_topic;
    if (!push_endpoint_name.empty()) {
      push_endpoint_args = uc.dest.push_endpoint_args;
      try {
        push_endpoint = RGWPubSubEndpoint::create(push_endpoint_name, arn_topic, string_to_args(push_endpoint_args), cct);
        ldout(cct, 20) << "push endpoint created: " << push_endpoint->to_str() << dendl;
      } catch (const RGWPubSubEndpoint::configuration_error& e) {
          ldout(cct, 1) << "ERROR: failed to create push endpoint: " 
            << push_endpoint_name << " due to: " << e.what() << dendl;
      }
    }
  }

  void dump(Formatter *f) const {
    encode_json("name", name, f);
    encode_json("topic", topic, f);
    encode_json("push_endpoint", push_endpoint_name, f);
    encode_json("push_endpoint_args", push_endpoint_args, f);
    encode_json("data_bucket_name", data_bucket_name, f);
    encode_json("data_oid_prefix", data_oid_prefix, f);
    encode_json("s3_id", s3_id, f);
  }

  void init(CephContext *cct, const JSONFormattable& config,
            const string& data_bucket_prefix,
            const string& default_oid_prefix) {
    name = config["name"];
    topic = config["topic"];
    push_endpoint_name = config["push_endpoint"];
    string default_bucket_name = data_bucket_prefix + name;
    data_bucket_name = config["data_bucket"](default_bucket_name.c_str());
    data_oid_prefix = config["data_oid_prefix"](default_oid_prefix.c_str());
    s3_id = config["s3_id"];
    arn_topic = config["arn_topic"];
    if (!push_endpoint_name.empty()) {
      push_endpoint_args = config["push_endpoint_args"];
      try {
        push_endpoint = RGWPubSubEndpoint::create(push_endpoint_name, arn_topic, string_to_args(push_endpoint_args), cct);
        ldout(cct, 20) << "push endpoint created: " << push_endpoint->to_str() << dendl;
      } catch (const RGWPubSubEndpoint::configuration_error& e) {
        ldout(cct, 1) << "ERROR: failed to create push endpoint: " 
          << push_endpoint_name << " due to: " << e.what() << dendl;
      }
    }
  }
};

using  PSSubConfigRef = std::shared_ptr<PSSubConfig>;

struct PSTopicConfig {
  std::string name;
  std::set<std::string> subs;

  void dump(Formatter *f) const {
    encode_json("name", name, f);
    encode_json("subs", subs, f);
  }
};

struct PSNotificationConfig {
  uint64_t id{0};
  string path; /* a path or a path prefix that would trigger the event (prefix: if ends with a wildcard) */
  string topic;
  bool is_prefix{false};


  void dump(Formatter *f) const {
    encode_json("id", id, f);
    encode_json("path", path, f);
    encode_json("topic", topic, f);
    encode_json("is_prefix", is_prefix, f);
  }

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

using PSTopicConfigRef = std::shared_ptr<PSTopicConfig>;
using TopicsRef = std::shared_ptr<std::vector<PSTopicConfigRef>>;

struct PSConfig {
  string id{"pubsub"};
  rgw_user user;
  string data_bucket_prefix;
  string data_oid_prefix;

  int events_retention_days{0};

  uint64_t sync_instance{0};
  uint64_t max_id{0};

  /* FIXME: no hard coded buckets, we'll have configurable topics */
  std::map<std::string, PSSubConfigRef> subs;
  std::map<std::string, PSTopicConfigRef> topics;
  std::multimap<std::string, PSNotificationConfig> notifications;
  
  bool start_with_full_sync{false};

  void dump(Formatter *f) const {
    encode_json("id", id, f);
    encode_json("user", user, f);
    encode_json("data_bucket_prefix", data_bucket_prefix, f);
    encode_json("data_oid_prefix", data_oid_prefix, f);
    encode_json("events_retention_days", events_retention_days, f);
    encode_json("sync_instance", sync_instance, f);
    encode_json("max_id", max_id, f);
    {
      Formatter::ArraySection section(*f, "subs");
      for (auto& sub : subs) {
        encode_json("sub", *sub.second, f);
      }
    }
    {
      Formatter::ArraySection section(*f, "topics");
      for (auto& topic : topics) {
        encode_json("topic", *topic.second, f);
      }
    }
    {
      Formatter::ObjectSection section(*f, "notifications");
      string last;
      for (auto& notif : notifications) {
        const string& n = notif.first;
        if (n != last) {
          if (!last.empty()) {
            f->close_section();
          }
          f->open_array_section(n.c_str());
        }
        last = n;
        encode_json("notifications", notif.second, f);
      }
      if (!last.empty()) {
        f->close_section();
      }
    }
    encode_json("start_with_full_sync", start_with_full_sync, f);
  }

  void init(CephContext *cct, const JSONFormattable& config) {
    string uid = config["uid"]("pubsub");
    user = rgw_user(config["tenant"], uid);
    data_bucket_prefix = config["data_bucket_prefix"]("pubsub-");
    data_oid_prefix = config["data_oid_prefix"];
    events_retention_days = config["events_retention_days"](PUBSUB_EVENTS_RETENTION_DEFAULT);

    for (auto& c : config["notifications"].array()) {
      PSNotificationConfig nc;
      nc.id = ++max_id;
      nc.init(cct, c);
      notifications.insert(std::make_pair(nc.path, nc));

      PSTopicConfig topic_config = { .name = nc.topic };
      topics[nc.topic] = make_shared<PSTopicConfig>(topic_config);
    }
    for (auto& c : config["subscriptions"].array()) {
      auto sc = std::make_shared<PSSubConfig>();
      sc->init(cct, c, data_bucket_prefix, data_oid_prefix);
      subs[sc->name] = sc;
      auto iter = topics.find(sc->topic);
      if (iter != topics.end()) {
        iter->second->subs.insert(sc->name);
      }
    }
    start_with_full_sync = config["start_with_full_sync"](false);

    ldout(cct, 5) << "pubsub: module config (parsed representation):\n" << json_str("config", *this, true) << dendl;
  }

  void init_instance(const RGWRealm& realm, uint64_t instance_id) {
    sync_instance = instance_id;
  }

  void get_topics(CephContext *cct, const rgw_bucket& bucket, const rgw_obj_key& key, TopicsRef *result) {
    string path = bucket.name + "/" + key.name;

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

      ldout(cct, 20) << ": found topic for path=" << bucket << "/" << key << ": id=" << target.id << 
          " target_path=" << target.path << ", topic=" << target.topic << dendl;
      (*result)->push_back(topic->second);
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
template<typename EventType>
using EventRef = std::shared_ptr<EventType>;

struct objstore_event {
  string id;
  const rgw_bucket& bucket;
  const rgw_obj_key& key;
  const ceph::real_time& mtime;
  const std::vector<std::pair<std::string, std::string> > *attrs;

  objstore_event(const rgw_bucket& _bucket,
                 const rgw_obj_key& _key,
                 const ceph::real_time& _mtime,
                 const std::vector<std::pair<std::string, std::string> > *_attrs) : bucket(_bucket),
                                                  key(_key),
                                                  mtime(_mtime),
                                                  attrs(_attrs) {}

  string get_hash() {
    string etag;
    RGWMD5Etag hash;
    hash.update(bucket.bucket_id);
    hash.update(key.name);
    hash.update(key.instance);
    hash.finish(&etag);

    assert(etag.size() > 8);

    return etag.substr(0, 8);
  }

  void dump(Formatter *f) const {
    {
      Formatter::ObjectSection s(*f, "bucket");
      encode_json("name", bucket.name, f);
      encode_json("tenant", bucket.tenant, f);
      encode_json("bucket_id", bucket.bucket_id, f);
    }
    {
      Formatter::ObjectSection s(*f, "key");
      encode_json("name", key.name, f);
      encode_json("instance", key.instance, f);
    }
    utime_t mt(mtime);
    encode_json("mtime", mt, f);
    Formatter::ObjectSection s(*f, "attrs");
    if (attrs) {
      for (auto& attr : *attrs) {
        encode_json(attr.first.c_str(), attr.second.c_str(), f);
      }
    }
  }
};

static void make_event_ref(CephContext *cct, const rgw_bucket& bucket,
                       const rgw_obj_key& key,
                       const ceph::real_time& mtime,
                       const std::vector<std::pair<std::string, std::string> > *attrs,
                       rgw::notify::EventType event_type,
                       EventRef<rgw_pubsub_event> *event) {
  *event = std::make_shared<rgw_pubsub_event>();

  EventRef<rgw_pubsub_event>& e = *event;
  e->event_name = rgw::notify::to_ceph_string(event_type);
  e->source = bucket.name + "/" + key.name;
  e->timestamp = real_clock::now();

  objstore_event oevent(bucket, key, mtime, attrs);

  const utime_t ts(e->timestamp);
  set_event_id(e->id, oevent.get_hash(), ts);

  encode_json("info", oevent, &e->info);
}

static void make_s3_record_ref(CephContext *cct, const rgw_bucket& bucket,
                       const rgw_user& owner,
                       const rgw_obj_key& key,
                       const ceph::real_time& mtime,
                       const std::vector<std::pair<std::string, std::string> > *attrs,
                       rgw::notify::EventType event_type,
                       EventRef<rgw_pubsub_s3_record> *record) {
  *record = std::make_shared<rgw_pubsub_s3_record>();

  EventRef<rgw_pubsub_s3_record>& r = *record;
  r->eventTime = mtime;
  r->eventName = rgw::notify::to_string(event_type);
  // userIdentity: not supported in sync module
  // x_amz_request_id: not supported in sync module
  // x_amz_id_2: not supported in sync module
  // configurationId is filled from subscription configuration
  r->bucket_name = bucket.name;
  r->bucket_ownerIdentity = owner.to_str();
  r->bucket_arn = to_string(rgw::ARN(bucket));
  r->bucket_id = bucket.bucket_id; // rgw extension
  r->object_key = key.name;
  // object_size not supported in sync module
  objstore_event oevent(bucket, key, mtime, attrs);
  r->object_etag = oevent.get_hash();
  r->object_versionId = key.instance;
 
  // use timestamp as per key sequence id (hex encoded)
  const utime_t ts(real_clock::now());
  boost::algorithm::hex((const char*)&ts, (const char*)&ts + sizeof(utime_t), 
          std::back_inserter(r->object_sequencer));
 
  set_event_id(r->id, r->object_etag, ts);
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

  void init_instance(const RGWRealm& realm, uint64_t instance_id, PSManagerRef& mgr);
};

using PSEnvRef = std::shared_ptr<PSEnv>;

template<typename EventType>
class PSEvent {
  const EventRef<EventType> event;

public:
  PSEvent(const EventRef<EventType>& _event) : event(_event) {}

  void format(bufferlist *bl) const {
    bl->append(json_str("", *event));
  }

  void encode_event(bufferlist& bl) const {
    encode(*event, bl);
  }

  const string& id() const {
    return event->id;
  }
};

template <class T>
class RGWSingletonCR : public RGWCoroutine {
  friend class WrapperCR;

  boost::asio::coroutine wrapper_state;
  bool started{false};
  int operate_ret{0};

  struct WaiterInfo {
    RGWCoroutine *cr{nullptr};
    T *result;
  };
  using WaiterInfoRef = std::shared_ptr<WaiterInfo>;

  deque<WaiterInfoRef> waiters;

  void add_waiter(RGWCoroutine *cr, T *result) {
    auto waiter = std::make_shared<WaiterInfo>();
    waiter->cr = cr;
    waiter->result = result;
    waiters.push_back(waiter);
  };

  bool get_next_waiter(WaiterInfoRef *waiter) {
    if (waiters.empty()) {
      waiter->reset();
      return false;
    }

    *waiter = waiters.front();
    waiters.pop_front();
    return true;
  }

  int operate_wrapper() override {
    reenter(&wrapper_state) {
      while (!is_done()) {
        ldout(cct, 20) << __func__ << "(): operate_wrapper() -> operate()" << dendl;
        operate_ret = operate();
        if (operate_ret < 0) {
          ldout(cct, 20) << *this << ": operate() returned r=" << operate_ret << dendl;
        }
        if (!is_done()) {
          yield;
        }
      }

      ldout(cct, 20) << __func__ << "(): RGWSingletonCR: operate_wrapper() done, need to wake up " << waiters.size() << " waiters" << dendl;
      /* we're done, can't yield anymore */

      WaiterInfoRef waiter;
      while (get_next_waiter(&waiter)) {
        ldout(cct, 20) << __func__ << "(): RGWSingletonCR: waking up waiter" << dendl;
        waiter->cr->set_retcode(retcode);
        waiter->cr->set_sleeping(false);
        return_result(waiter->result);
        put();
      }

      return retcode;
    }
    return 0;
  }

  virtual void return_result(T *result) {}

public:
  RGWSingletonCR(CephContext *_cct)
    : RGWCoroutine(_cct) {}

  int execute(RGWCoroutine *caller, T *result = nullptr) {
    if (!started) {
      ldout(cct, 20) << __func__ << "(): singleton not started, starting" << dendl;
      started = true;
      caller->call(this);
      return 0;
    } else if (!is_done()) {
      ldout(cct, 20) << __func__ << "(): singleton not done yet, registering as waiter" << dendl;
      get();
      add_waiter(caller, result);
      caller->set_sleeping(true);
      return 0;
    }

    ldout(cct, 20) << __func__ << "(): singleton done, returning retcode=" << retcode << dendl;
    caller->set_retcode(retcode);
    return_result(result);
    return retcode;
  }
};


class PSSubscription;
using PSSubscriptionRef = std::shared_ptr<PSSubscription>;

class PSSubscription {
  class InitCR;
  friend class InitCR;
  friend class RGWPSHandleObjEventCR;

  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  PSSubConfigRef sub_conf;
  std::shared_ptr<rgw_get_bucket_info_result> get_bucket_info_result;
  RGWBucketInfo *bucket_info{nullptr};
  RGWDataAccessRef data_access;
  RGWDataAccess::BucketRef bucket;

  InitCR *init_cr{nullptr};

  class InitBucketLifecycleCR : public RGWCoroutine {
    RGWDataSyncEnv *sync_env;
    PSConfigRef& conf;
    LCRule rule;

    int retention_days;

    rgw_bucket_lifecycle_config_params lc_config;

  public:
    InitBucketLifecycleCR(RGWDataSyncEnv *_sync_env,
           PSConfigRef& _conf,
           RGWBucketInfo& _bucket_info,
           std::map<string, bufferlist>& _bucket_attrs) : RGWCoroutine(_sync_env->cct),
                                                     sync_env(_sync_env),
                                                     conf(_conf) {
      lc_config.bucket_info = _bucket_info;
      lc_config.bucket_attrs = _bucket_attrs;
      retention_days = conf->events_retention_days;
    }

    int operate() override {
      reenter(this) {

        rule.init_simple_days_rule("Pubsub Expiration", "" /* all objects in bucket */, retention_days);

        {
          /* maybe we already have it configured? */
          RGWLifecycleConfiguration old_config;
          auto aiter = lc_config.bucket_attrs.find(RGW_ATTR_LC);
          if (aiter != lc_config.bucket_attrs.end()) {
            bufferlist::const_iterator iter{&aiter->second};
            try {
              old_config.decode(iter);
            } catch (const buffer::error& e) {
              ldpp_dout(sync_env->dpp, 0) << __func__ <<  "(): decode life cycle config failed" << dendl;
            }
          }

          auto old_rules = old_config.get_rule_map();
          for (auto ori : old_rules) {
            auto& old_rule = ori.second;

            if (old_rule.get_prefix().empty() && 
                old_rule.get_expiration().get_days() == retention_days &&
                old_rule.is_enabled()) {
              ldpp_dout(sync_env->dpp, 20) << "no need to set lifecycle rule on bucket, existing rule matches config" << dendl;
              return set_cr_done();
            }
          }
        }

        lc_config.config.add_rule(rule);
        yield call(new RGWBucketLifecycleConfigCR(sync_env->async_rados,
                                                  sync_env->store,
                                                  lc_config,
                                                  sync_env->dpp));
        if (retcode < 0) {
          ldpp_dout(sync_env->dpp, 1) << "ERROR: failed to set lifecycle on bucket: ret=" << retcode << dendl;
          return set_cr_error(retcode);
        }

        return set_cr_done();
      }
      return 0;
    }
  };

  class InitCR : public RGWSingletonCR<bool> {
    RGWDataSyncEnv *sync_env;
    PSSubscriptionRef sub;
    rgw_get_bucket_info_params get_bucket_info;
    rgw_bucket_create_local_params create_bucket;
    PSConfigRef& conf;
    PSSubConfigRef& sub_conf;
    int i;

  public:
    InitCR(RGWDataSyncEnv *_sync_env,
           PSSubscriptionRef& _sub) : RGWSingletonCR<bool>(_sync_env->cct),
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
            ldpp_dout(sync_env->dpp, 1) << "ERROR: failed to geting bucket info: " << "tenant="
              << get_bucket_info.tenant << " name=" << get_bucket_info.bucket_name << ": ret=" << retcode << dendl;
          }
          if (retcode == 0) {
            {
              auto& result = sub->get_bucket_info_result;
              sub->bucket_info = &result->bucket_info;

              int ret = sub->data_access->get_bucket(result->bucket_info, result->attrs, &sub->bucket);
              if (ret < 0) {
                ldpp_dout(sync_env->dpp, 1) << "ERROR: data_access.get_bucket() bucket=" << result->bucket_info.bucket << " failed, ret=" << ret << dendl;
                return set_cr_error(ret);
              }
            }

            yield call(new InitBucketLifecycleCR(sync_env, conf,
                                                 sub->get_bucket_info_result->bucket_info,
                                                 sub->get_bucket_info_result->attrs));
            if (retcode < 0) {
              ldpp_dout(sync_env->dpp, 1) << "ERROR: failed to init lifecycle on bucket (bucket=" << sub_conf->data_bucket_name << ") ret=" << retcode << dendl;
              return set_cr_error(retcode);
            }

            return set_cr_done();
          }

          create_bucket.user_info = sub->env->data_user_info;
          create_bucket.bucket_name = sub_conf->data_bucket_name;
          ldpp_dout(sync_env->dpp, 20) << "pubsub: bucket create: using user info: " << json_str("obj", *sub->env->data_user_info, true) << dendl;
          yield call(new RGWBucketCreateLocalCR(sync_env->async_rados,
                                                sync_env->store,
                                                create_bucket,
                                                sync_env->dpp));
          if (retcode < 0) {
            ldpp_dout(sync_env->dpp, 1) << "ERROR: failed to create bucket: " << "tenant="
              << get_bucket_info.tenant << " name=" << get_bucket_info.bucket_name << ": ret=" << retcode << dendl;
            return set_cr_error(retcode);
          }

          /* second iteration: we got -ENOENT and created a bucket */
        }

        /* failed twice on -ENOENT, unexpected */
        ldpp_dout(sync_env->dpp, 1) << "ERROR: failed to create bucket " << "tenant=" << get_bucket_info.tenant
          << " name=" << get_bucket_info.bucket_name << dendl;
        return set_cr_error(-EIO);
      }
      return 0;
    }
  };

  template<typename EventType>
  class StoreEventCR : public RGWCoroutine {
    RGWDataSyncEnv* const sync_env;
    const PSSubscriptionRef sub;
    const PSEvent<EventType> pse;
    const string oid_prefix;

  public:
    StoreEventCR(RGWDataSyncEnv* const _sync_env,
                 const PSSubscriptionRef& _sub,
                 const EventRef<EventType>& _event) : RGWCoroutine(_sync_env->cct),
                                     sync_env(_sync_env),
                                     sub(_sub),
                                     pse(_event),
                                     oid_prefix(sub->sub_conf->data_oid_prefix) {
    }

    int operate() override {
      rgw_object_simple_put_params put_obj;
      reenter(this) {

        put_obj.bucket = sub->bucket;
        put_obj.key = rgw_obj_key(oid_prefix + pse.id());

        pse.format(&put_obj.data);
       
        {
          bufferlist bl;
          pse.encode_event(bl);
          bufferlist bl64;
          bl.encode_base64(bl64);
          put_obj.user_data = bl64.to_str();
        }
        
        yield call(new RGWObjectSimplePutCR(sync_env->async_rados,
                                            sync_env->store,
                                            put_obj,
                                            sync_env->dpp));
        if (retcode < 0) {
          ldpp_dout(sync_env->dpp, 10) << "failed to store event: " << put_obj.bucket << "/" << put_obj.key << " ret=" << retcode << dendl;
          return set_cr_error(retcode);
        } else {
          ldpp_dout(sync_env->dpp, 20) << "event stored: " << put_obj.bucket << "/" << put_obj.key << dendl;
        }

        return set_cr_done();
      }
      return 0;
    }
  };

  template<typename EventType>
  class PushEventCR : public RGWCoroutine {
    RGWDataSyncEnv* const sync_env;
    const EventRef<EventType> event;
    const PSSubConfigRef& sub_conf;

  public:
    PushEventCR(RGWDataSyncEnv* const _sync_env,
                 const PSSubscriptionRef& _sub,
                 const EventRef<EventType>& _event) : RGWCoroutine(_sync_env->cct),
                                     sync_env(_sync_env),
                                     event(_event),
                                     sub_conf(_sub->sub_conf) {
    }

    int operate() override {
      reenter(this) {
        ceph_assert(sub_conf->push_endpoint);
        yield call(sub_conf->push_endpoint->send_to_completion_async(*event.get(), sync_env));
      
        if (retcode < 0) {
          ldout(sync_env->cct, 10) << "failed to push event: " << event->id <<
            " to endpoint: " << sub_conf->push_endpoint_name << " ret=" << retcode << dendl;
          return set_cr_error(retcode);
        }
        
        ldout(sync_env->cct, 20) << "event: " << event->id <<
          " pushed to endpoint: " << sub_conf->push_endpoint_name << dendl;
        return set_cr_done();
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

  PSSubscription(RGWDataSyncEnv *_sync_env,
                 PSEnvRef _env,
                 rgw_pubsub_sub_config& user_sub_conf) : sync_env(_sync_env),
                                      env(_env),
                                      sub_conf(std::make_shared<PSSubConfig>()),
                                      data_access(std::make_shared<RGWDataAccess>(sync_env->store)) {
    sub_conf->from_user_conf(sync_env->cct, user_sub_conf);
  }
  virtual ~PSSubscription() {
    if (init_cr) {
      init_cr->put();
    }
  }

  template <class C>
  static PSSubscriptionRef get_shared(RGWDataSyncEnv *_sync_env,
                                PSEnvRef _env,
                                C& _sub_conf) {
    auto sub = std::make_shared<PSSubscription>(_sync_env, _env, _sub_conf);
    sub->init_cr = new InitCR(_sync_env, sub);
    sub->init_cr->get();
    return sub;
  }

  int call_init_cr(RGWCoroutine *caller) {
    return init_cr->execute(caller);
  }

  template<typename EventType>
  static RGWCoroutine *store_event_cr(RGWDataSyncEnv* const sync_env, const PSSubscriptionRef& sub, const EventRef<EventType>& event) {
    return new StoreEventCR<EventType>(sync_env, sub, event);
  }

  template<typename EventType>
  static RGWCoroutine *push_event_cr(RGWDataSyncEnv* const sync_env, const PSSubscriptionRef& sub, const EventRef<EventType>& event) {
    return new PushEventCR<EventType>(sync_env, sub, event);
  }
  friend class InitCR;
};

class PSManager
{
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;

  std::map<string, PSSubscriptionRef> subs;

  class GetSubCR : public RGWSingletonCR<PSSubscriptionRef> {
    RGWDataSyncEnv *sync_env;
    PSManagerRef mgr;
    rgw_user owner;
    string sub_name;
    string sub_id;
    PSSubscriptionRef *ref;

    PSConfigRef conf;

    PSSubConfigRef sub_conf;
    rgw_pubsub_sub_config user_sub_conf;

  public:
    GetSubCR(RGWDataSyncEnv *_sync_env,
                      PSManagerRef& _mgr,
                      const rgw_user& _owner,
                      const string& _sub_name,
                      PSSubscriptionRef *_ref) : RGWSingletonCR<PSSubscriptionRef>(_sync_env->cct),
                                                 sync_env(_sync_env),
                                                 mgr(_mgr),
                                                 owner(_owner),
                                                 sub_name(_sub_name),
                                                 ref(_ref),
                                                 conf(mgr->env->conf) {
    }
    ~GetSubCR() { }

    int operate() override {
      reenter(this) {
        if (owner.empty()) {
          if (!conf->find_sub(sub_name, &sub_conf)) {
            ldout(sync_env->cct, 10) << "failed to find subscription config: name=" << sub_name << dendl;
            mgr->remove_get_sub(owner, sub_name);
            return set_cr_error(-ENOENT);
          }

          *ref = PSSubscription::get_shared(sync_env, mgr->env, sub_conf);
        } else {
          using ReadInfoCR = RGWSimpleRadosReadCR<rgw_pubsub_sub_config>;
          yield {
            RGWUserPubSub ups(sync_env->store, owner);
            rgw_raw_obj obj;
            ups.get_sub_meta_obj(sub_name, &obj);
            bool empty_on_enoent = false;
            call(new ReadInfoCR(sync_env->async_rados, sync_env->store->svc()->sysobj,
                                obj,
                                &user_sub_conf, empty_on_enoent));
          }
          if (retcode < 0) {
            mgr->remove_get_sub(owner, sub_name);
            return set_cr_error(retcode);
          }

          *ref = PSSubscription::get_shared(sync_env, mgr->env, user_sub_conf);
        }

        yield (*ref)->call_init_cr(this);
        if (retcode < 0) {
          ldout(sync_env->cct, 10) << "failed to init subscription" << dendl;
          mgr->remove_get_sub(owner, sub_name);
          return set_cr_error(retcode);
        }

        if (owner.empty()) {
          mgr->subs[sub_name] = *ref;
        }
        mgr->remove_get_sub(owner, sub_name);

        return set_cr_done();
      }
      return 0;
    }

    void return_result(PSSubscriptionRef *result) override {
      ldout(cct, 20) << __func__ << "(): returning result: retcode=" << retcode << " resultp=" << (void *)result << dendl;
      if (retcode >= 0) {
        *result = *ref;
      }
    }
  };

  string sub_id(const rgw_user& owner, const string& sub_name) {
    string owner_prefix;
    if (!owner.empty()) {
      owner_prefix = owner.to_str() + "/";
    }

    return owner_prefix + sub_name;
  }

  std::map<std::string, GetSubCR *> get_subs;

  GetSubCR *& get_get_subs(const rgw_user& owner, const string& name) {
    return get_subs[sub_id(owner, name)];
  }

  void remove_get_sub(const rgw_user& owner, const string& name) {
    get_subs.erase(sub_id(owner, name));
  }

  bool find_sub_instance(const rgw_user& owner, const string& sub_name, PSSubscriptionRef *sub) {
    auto iter = subs.find(sub_id(owner, sub_name));
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
  static PSManagerRef get_shared(RGWDataSyncEnv *_sync_env,
                                 PSEnvRef _env) {
    return std::shared_ptr<PSManager>(new PSManager(_sync_env, _env));
  }

  static int call_get_subscription_cr(RGWDataSyncEnv *sync_env, PSManagerRef& mgr, 
      RGWCoroutine *caller, const rgw_user& owner, const string& sub_name, PSSubscriptionRef *ref) {
    if (mgr->find_sub_instance(owner, sub_name, ref)) {
      /* found it! nothing to execute */
      ldout(sync_env->cct, 20) << __func__ << "(): found sub instance" << dendl;
    }
    auto& gs = mgr->get_get_subs(owner, sub_name);
    if (!gs) {
      ldout(sync_env->cct, 20) << __func__ << "(): first get subs" << dendl;
      gs = new GetSubCR(sync_env, mgr, owner, sub_name, ref);
    }
    ldout(sync_env->cct, 20) << __func__ << "(): executing get subs" << dendl;
    return gs->execute(caller, ref);
  }

  friend class GetSubCR;
};

void PSEnv::init_instance(const RGWRealm& realm, uint64_t instance_id, PSManagerRef& mgr) {
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
                       PSEnvRef& _env) : RGWCoroutine(_sync_env->cct),
                                                    sync_env(_sync_env),
                                                    env(_env), conf(env->conf) {}
  int operate() override {
    reenter(this) {
      ldpp_dout(sync_env->dpp, 1) << ": init pubsub config zone=" << sync_env->source_zone << dendl;

      /* nothing to do here right now */
      create_user.user = conf->user;
      create_user.max_buckets = 0; /* unlimited */
      create_user.display_name = "pubsub";
      create_user.generate_key = false;
      yield call(new RGWUserCreateCR(sync_env->async_rados, sync_env->store, create_user, sync_env->dpp));
      if (retcode < 0) {
        ldpp_dout(sync_env->dpp, 1) << "ERROR: failed to create rgw user: ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }

      get_user_info.user = conf->user;
      yield call(new RGWGetUserInfoCR(sync_env->async_rados, sync_env->store, get_user_info, env->data_user_info));
      if (retcode < 0) {
        ldpp_dout(sync_env->dpp, 1) << "ERROR: failed to create rgw user: ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }

      ldpp_dout(sync_env->dpp, 20) << "pubsub: get user info cr returned: " << json_str("obj", *env->data_user_info, true) << dendl;


      return set_cr_done();
    }
    return 0;
  }
};

bool match(const rgw_pubsub_topic_filter& filter, const std::string& key_name, rgw::notify::EventType event_type) {
  if (!match(filter.events, event_type)) {
    return false;
  }
  if (!match(filter.s3_filter.key_filter, key_name)) {
    return false;
  }
  return true;
}

class RGWPSFindBucketTopicsCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  rgw_user owner;
  rgw_bucket bucket;
  rgw_obj_key key;
  rgw::notify::EventType event_type;

  RGWUserPubSub ups;

  rgw_raw_obj bucket_obj;
  rgw_raw_obj user_obj;
  rgw_pubsub_bucket_topics bucket_topics;
  rgw_pubsub_user_topics user_topics;
  TopicsRef *topics;
public:
  RGWPSFindBucketTopicsCR(RGWDataSyncEnv *_sync_env,
                      PSEnvRef& _env,
                      const rgw_user& _owner,
                      const rgw_bucket& _bucket,
                      const rgw_obj_key& _key,
                      rgw::notify::EventType _event_type,
                      TopicsRef *_topics) : RGWCoroutine(_sync_env->cct),
                                                          sync_env(_sync_env),
                                                          env(_env),
                                                          owner(_owner),
                                                          bucket(_bucket),
                                                          key(_key),
                                                          event_type(_event_type),
                                                          ups(_sync_env->store, owner),
                                                          topics(_topics) {
    *topics = std::make_shared<vector<PSTopicConfigRef> >();
  }
  int operate() override {
    reenter(this) {
      ups.get_bucket_meta_obj(bucket, &bucket_obj);
      ups.get_user_meta_obj(&user_obj);

      using ReadInfoCR = RGWSimpleRadosReadCR<rgw_pubsub_bucket_topics>;
      yield {
        bool empty_on_enoent = true;
        call(new ReadInfoCR(sync_env->async_rados, sync_env->store->svc()->sysobj,
                            bucket_obj,
                            &bucket_topics, empty_on_enoent));
      }
      if (retcode < 0 && retcode != -ENOENT) {
        return set_cr_error(retcode);
      }

      ldout(sync_env->cct, 20) << "RGWPSFindBucketTopicsCR(): found " << bucket_topics.topics.size() << " topics for bucket " << bucket << dendl;

      if (!bucket_topics.topics.empty()) {
	using ReadUserTopicsInfoCR = RGWSimpleRadosReadCR<rgw_pubsub_user_topics>;
	yield {
	  bool empty_on_enoent = true;
	  call(new ReadUserTopicsInfoCR(sync_env->async_rados, sync_env->store->svc()->sysobj,
					user_obj,
					&user_topics, empty_on_enoent));
	}
	if (retcode < 0 && retcode != -ENOENT) {
	  return set_cr_error(retcode);
	}
      }

      for (auto& titer : bucket_topics.topics) {
        auto& topic_filter = titer.second;
        auto& info = topic_filter.topic;
        if (!match(topic_filter, key.name, event_type)) {
          continue;
        }
        std::shared_ptr<PSTopicConfig> tc = std::make_shared<PSTopicConfig>();
        tc->name = info.name;
        tc->subs = user_topics.topics[info.name].subs;
        (*topics)->push_back(tc);
      }

      env->conf->get_topics(sync_env->cct, bucket, key, topics);
      return set_cr_done();
    }
    return 0;
  }
};

class RGWPSHandleObjEventCR : public RGWCoroutine {
  RGWDataSyncEnv* const sync_env;
  const PSEnvRef env;
  const rgw_user& owner;
  const EventRef<rgw_pubsub_event> event;
  const EventRef<rgw_pubsub_s3_record> record;
  const TopicsRef topics;
  const std::array<rgw_user, 2> owners;
  bool has_subscriptions;
  bool event_handled;
  bool sub_conf_found;
  PSSubscriptionRef sub;
  std::array<rgw_user, 2>::const_iterator oiter;
  std::vector<PSTopicConfigRef>::const_iterator titer;
  std::set<string>::const_iterator siter;
  int last_sub_conf_error;

public:
  RGWPSHandleObjEventCR(RGWDataSyncEnv* const _sync_env,
                      const PSEnvRef _env,
                      const rgw_user& _owner,
                      const EventRef<rgw_pubsub_event>& _event,
                      const EventRef<rgw_pubsub_s3_record>& _record,
                      const TopicsRef& _topics) : RGWCoroutine(_sync_env->cct),
                                          sync_env(_sync_env),
                                          env(_env),
                                          owner(_owner),
                                          event(_event),
                                          record(_record),
                                          topics(_topics),
                                          owners({owner, rgw_user{}}),
                                          has_subscriptions(false),
                                          event_handled(false) {}

  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 20) << ": handle event: obj: z=" << sync_env->source_zone
                               << " event=" << json_str("event", *event, false)
                               << " owner=" << owner << dendl;

      ldout(sync_env->cct, 20) << "pubsub: " << topics->size() << " topics found for path" << dendl;
     
      // outside caller should check that
      ceph_assert(!topics->empty());

      if (perfcounter) perfcounter->inc(l_rgw_pubsub_event_triggered);

      // loop over all topics related to the bucket/object
      for (titer = topics->begin(); titer != topics->end(); ++titer) {
        ldout(sync_env->cct, 20) << ": notification for " << event->source << ": topic=" << 
          (*titer)->name << ", has " << (*titer)->subs.size() << " subscriptions" << dendl;
        // loop over all subscriptions of the topic
        for (siter = (*titer)->subs.begin(); siter != (*titer)->subs.end(); ++siter) {
          ldout(sync_env->cct, 20) << ": subscription: " << *siter << dendl;
          has_subscriptions = true;
          sub_conf_found = false;
          // try to read subscription configuration from global/user cond
          // configuration is considered missing only if does not exist in either
          for (oiter = owners.begin(); oiter != owners.end(); ++oiter) {
            yield PSManager::call_get_subscription_cr(sync_env, env->manager, this, *oiter, *siter, &sub);
            if (retcode < 0) {
              if (sub_conf_found) {
                // not a real issue, sub conf already found
                retcode = 0;
              }
              last_sub_conf_error = retcode;
              continue;
            }
            sub_conf_found = true;
            if (sub->sub_conf->s3_id.empty()) {
              // subscription was not made by S3 compatible API
              ldout(sync_env->cct, 20) << "storing event for subscription=" << *siter << " owner=" << *oiter << " ret=" << retcode << dendl;
              yield call(PSSubscription::store_event_cr(sync_env, sub, event));
              if (retcode < 0) {
                if (perfcounter) perfcounter->inc(l_rgw_pubsub_store_fail);
                ldout(sync_env->cct, 1) << "ERROR: failed to store event for subscription=" << *siter << " ret=" << retcode << dendl;
              } else {
                if (perfcounter) perfcounter->inc(l_rgw_pubsub_store_ok);
                event_handled = true;
              }
              if (sub->sub_conf->push_endpoint) {
                ldout(sync_env->cct, 20) << "push event for subscription=" << *siter << " owner=" << *oiter << " ret=" << retcode << dendl;
                yield call(PSSubscription::push_event_cr(sync_env, sub, event));
                if (retcode < 0) {
                  if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_failed);
                  ldout(sync_env->cct, 1) << "ERROR: failed to push event for subscription=" << *siter << " ret=" << retcode << dendl;
                } else {
                  if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_ok);
                  event_handled = true;
                }
              } 
            } else {
              // subscription was made by S3 compatible API
              ldout(sync_env->cct, 20) << "storing record for subscription=" << *siter << " owner=" << *oiter << " ret=" << retcode << dendl;
              record->configurationId = sub->sub_conf->s3_id;
              yield call(PSSubscription::store_event_cr(sync_env, sub, record));
              if (retcode < 0) {
                if (perfcounter) perfcounter->inc(l_rgw_pubsub_store_fail);
                ldout(sync_env->cct, 1) << "ERROR: failed to store record for subscription=" << *siter << " ret=" << retcode << dendl;
              } else {
                if (perfcounter) perfcounter->inc(l_rgw_pubsub_store_ok);
                event_handled = true;
              }
              if (sub->sub_conf->push_endpoint) {
                  ldout(sync_env->cct, 20) << "push record for subscription=" << *siter << " owner=" << *oiter << " ret=" << retcode << dendl;
                yield call(PSSubscription::push_event_cr(sync_env, sub, record));
                if (retcode < 0) {
                  if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_failed);
                  ldout(sync_env->cct, 1) << "ERROR: failed to push record for subscription=" << *siter << " ret=" << retcode << dendl;
                } else {
                  if (perfcounter) perfcounter->inc(l_rgw_pubsub_push_ok);
                  event_handled = true;
                }
              }
            }
          }
          if (!sub_conf_found) {
            // could not find conf for subscription at user or global levels
            if (perfcounter) perfcounter->inc(l_rgw_pubsub_missing_conf);
            ldout(sync_env->cct, 1) << "ERROR: failed to find subscription config for subscription=" << *siter 
              << " ret=" << last_sub_conf_error << dendl;
              if (retcode == -ENOENT) {
                // missing subscription info should be reflected back as invalid argument
                // and not as missing object
                retcode =  -EINVAL;
              }
          }
        }
      }
      if (has_subscriptions && !event_handled) {
        // event is considered "lost" of it has subscriptions on any of its topics
        // but it was not stored in, or pushed to, any of them
        if (perfcounter) perfcounter->inc(l_rgw_pubsub_event_lost);
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

// coroutine invoked on remote object creation
class RGWPSHandleRemoteObjCBCR : public RGWStatRemoteObjCBCR {
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  std::optional<uint64_t> versioned_epoch;
  EventRef<rgw_pubsub_event> event;
  EventRef<rgw_pubsub_s3_record> record;
  TopicsRef topics;
public:
  RGWPSHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                          PSEnvRef _env, std::optional<uint64_t> _versioned_epoch,
                          TopicsRef& _topics) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key),
                                                                      sync_env(_sync_env),
                                                                      env(_env),
                                                                      versioned_epoch(_versioned_epoch),
                                                                      topics(_topics) {
  }
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 20) << ": stat of remote obj: z=" << sync_env->source_zone
                               << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                               << " attrs=" << attrs << dendl;
      {
        std::vector<std::pair<std::string, std::string> > attrs;
        for (auto& attr : attrs) {
          string k = attr.first;
          if (boost::algorithm::starts_with(k, RGW_ATTR_PREFIX)) {
            k = k.substr(sizeof(RGW_ATTR_PREFIX) - 1);
          }
          attrs.push_back(std::make_pair(k, attr.second));
        } 
        // at this point we don't know whether we need the ceph event or S3 record
        // this is why both are created here, once we have information about the 
        // subscription, we will store/push only the relevant ones
        make_event_ref(sync_env->cct,
                       bucket_info.bucket, key,
                       mtime, &attrs,
                       rgw::notify::ObjectCreated, &event);
        make_s3_record_ref(sync_env->cct,
                       bucket_info.bucket, bucket_info.owner, key,
                       mtime, &attrs,
                       rgw::notify::ObjectCreated, &record);
      }

      yield call(new RGWPSHandleObjEventCR(sync_env, env, bucket_info.owner, event, record, topics));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

class RGWPSHandleRemoteObjCR : public RGWCallStatRemoteObjCR {
  PSEnvRef env;
  std::optional<uint64_t> versioned_epoch;
  TopicsRef topics;
public:
  RGWPSHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                        PSEnvRef _env, std::optional<uint64_t> _versioned_epoch,
                        TopicsRef& _topics) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                           env(_env), versioned_epoch(_versioned_epoch),
                                                           topics(_topics) {
  }

  ~RGWPSHandleRemoteObjCR() override {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWPSHandleRemoteObjCBCR(sync_env, bucket_info, key, env, versioned_epoch, topics);
  }
};

class RGWPSHandleObjCreateCR : public RGWCoroutine {
  
  RGWDataSyncEnv *sync_env;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  PSEnvRef env;
  std::optional<uint64_t> versioned_epoch;
  TopicsRef topics;
public:
  RGWPSHandleObjCreateCR(RGWDataSyncEnv *_sync_env,
                       RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                       PSEnvRef _env, std::optional<uint64_t> _versioned_epoch) : RGWCoroutine(_sync_env->cct),
                                                                   sync_env(_sync_env),
                                                                   bucket_info(_bucket_info),
                                                                   key(_key),
                                                                   env(_env),
                                                                   versioned_epoch(_versioned_epoch) {
  }

  ~RGWPSHandleObjCreateCR() override {}

  int operate() override {
    reenter(this) {
      yield call(new RGWPSFindBucketTopicsCR(sync_env, env, bucket_info.owner,
                                             bucket_info.bucket, key,
                                             rgw::notify::ObjectCreated,
                                             &topics));
      if (retcode < 0) {
        ldout(sync_env->cct, 1) << "ERROR: RGWPSFindBucketTopicsCR returned ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }
      if (topics->empty()) {
        ldout(sync_env->cct, 20) << "no topics found for " << bucket_info.bucket << "/" << key << dendl;
        return set_cr_done();
      }
      yield call(new RGWPSHandleRemoteObjCR(sync_env, bucket_info, key, env, versioned_epoch, topics));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

// coroutine invoked on remote object deletion
class RGWPSGenericObjEventCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  rgw_user owner;
  rgw_bucket bucket;
  rgw_obj_key key;
  ceph::real_time mtime;
  rgw::notify::EventType event_type;
  EventRef<rgw_pubsub_event> event;
  EventRef<rgw_pubsub_s3_record> record;
  TopicsRef topics;
public:
  RGWPSGenericObjEventCBCR(RGWDataSyncEnv *_sync_env,
                           PSEnvRef _env,
                           RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                           rgw::notify::EventType _event_type) : RGWCoroutine(_sync_env->cct),
                                                             sync_env(_sync_env),
                                                             env(_env),
                                                             owner(_bucket_info.owner),
                                                             bucket(_bucket_info.bucket),
                                                             key(_key),
                                                             mtime(_mtime), event_type(_event_type) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 20) << ": remove remote obj: z=" << sync_env->source_zone
                               << " b=" << bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield call(new RGWPSFindBucketTopicsCR(sync_env, env, owner, bucket, key, event_type, &topics));
      if (retcode < 0) {
        ldout(sync_env->cct, 1) << "ERROR: RGWPSFindBucketTopicsCR returned ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }
      if (topics->empty()) {
        ldout(sync_env->cct, 20) << "no topics found for " << bucket << "/" << key << dendl;
        return set_cr_done();
      }
      // at this point we don't know whether we need the ceph event or S3 record
      // this is why both are created here, once we have information about the 
      // subscription, we will store/push only the relevant ones
      make_event_ref(sync_env->cct,
                     bucket, key,
                     mtime, nullptr,
                     event_type, &event);
      make_s3_record_ref(sync_env->cct,
                     bucket, owner, key,
                     mtime, nullptr,
                     event_type, &record);
      yield call(new RGWPSHandleObjEventCR(sync_env, env, owner, event, record, topics));
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
    env->init_instance(sync_env->store->svc()->zone->get_realm(), instance_id, mgr);
  }

  RGWCoroutine *start_sync(RGWDataSyncEnv *sync_env) override {
    ldout(sync_env->cct, 5) << conf->id << ": start" << dendl;
    return new RGWPSInitEnvCBCR(sync_env, env);
  }

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, 
      rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": sync_object: b=" << bucket_info.bucket << 
          " k=" << key << " versioned_epoch=" << versioned_epoch.value_or(0) << dendl;
    return new RGWPSHandleObjCreateCR(sync_env, bucket_info, key, env, versioned_epoch);
  }

  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, 
      rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": rm_object: b=" << bucket_info.bucket << 
          " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWPSGenericObjEventCBCR(sync_env, env, bucket_info, key, mtime, rgw::notify::ObjectRemovedDelete);
  }

  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, 
      rgw_obj_key& key, real_time& mtime, rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": create_delete_marker: b=" << bucket_info.bucket << 
          " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWPSGenericObjEventCBCR(sync_env, env, bucket_info, key, mtime, rgw::notify::ObjectRemovedDeleteMarkerCreated);
  }

  PSConfigRef& get_conf() { return conf; }
};

RGWPSSyncModuleInstance::RGWPSSyncModuleInstance(CephContext *cct, const JSONFormattable& config)
{
  data_handler = std::unique_ptr<RGWPSDataSyncModule>(new RGWPSDataSyncModule(cct, config));
  string jconf = json_str("conf", *data_handler->get_conf());
  JSONParser p;
  if (!p.parse(jconf.c_str(), jconf.size())) {
    ldout(cct, 1) << "ERROR: failed to parse sync module effective conf: " << jconf << dendl;
    effective_conf = config;
  } else {
    effective_conf.decode_json(&p);
  }
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  if (!rgw::amqp::init(cct)) {
    ldout(cct, 1) << "ERROR: failed to initialize AMQP manager in pubsub sync module" << dendl;
  }
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
  if (!rgw::kafka::init(cct)) {
    ldout(cct, 1) << "ERROR: failed to initialize Kafka manager in pubsub sync module" << dendl;
  }
#endif
}

RGWPSSyncModuleInstance::~RGWPSSyncModuleInstance() {
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
  rgw::amqp::shutdown();
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
  rgw::kafka::shutdown();
#endif
}

RGWDataSyncModule *RGWPSSyncModuleInstance::get_data_handler()
{
  return data_handler.get();
}

RGWRESTMgr *RGWPSSyncModuleInstance::get_rest_filter(int dialect, RGWRESTMgr *orig) {
  if (dialect != RGW_REST_S3) {
    return orig;
  }
  return new RGWRESTMgr_PubSub();
}

bool RGWPSSyncModuleInstance::should_full_sync() const {
   return data_handler->get_conf()->start_with_full_sync;
}

int RGWPSSyncModule::create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) {
  instance->reset(new RGWPSSyncModuleInstance(cct, config));
  return 0;
}


