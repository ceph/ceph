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
  string data_oid_prefix;

  void from_user_conf(const rgw_pubsub_user_sub_config& uc) {
    name = uc.name;
    topic = uc.topic;
    push_endpoint = uc.dest.push_endpoint;
    data_bucket_name = uc.dest.bucket_name;
    data_oid_prefix = uc.dest.oid_prefix;
  }

  void dump(Formatter *f) const {
    encode_json("name", name, f);
    encode_json("topic", topic, f);
    encode_json("push_endpoint", push_endpoint, f);
    encode_json("data_bucket_name", data_bucket_name, f);
    encode_json("data_oid_prefix", data_oid_prefix, f);
  }

  void init(CephContext *cct, const JSONFormattable& config,
            const string& data_bucket_prefix,
            const string& default_oid_prefix) {
    name = config["name"];
    topic = config["topic"];
    push_endpoint = config["push_endpoint"];
    string default_bucket_name = data_bucket_prefix + name;
    data_bucket_name = config["data_bucket"](default_bucket_name.c_str());
    data_oid_prefix = config["data_oid_prefix"](default_oid_prefix.c_str());
  }
};

using  PSSubConfigRef = std::shared_ptr<PSSubConfig>;

struct PSTopicConfig {
  string name;
  set<string> subs;

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
using TopicsRef = std::shared_ptr<vector<PSTopicConfigRef>>;


struct PSConfig {
  string id{"pubsub"};
  rgw_user user;
  string data_bucket_prefix;
  string data_oid_prefix;

  uint64_t sync_instance{0};
  uint64_t max_id{0};

  /* FIXME: no hard coded buckets, we'll have configurable topics */
  map<string, PSSubConfigRef> subs;
  map<string, PSTopicConfigRef> topics;
  multimap<string, PSNotificationConfig> notifications;

  void dump(Formatter *f) const {
    encode_json("id", id, f);
    encode_json("user", user, f);
    encode_json("data_bucket_prefix", data_bucket_prefix, f);
    encode_json("data_oid_prefix", data_bucket_prefix, f);
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
  }

  void init(CephContext *cct, const JSONFormattable& config) {
    string uid = config["uid"]("pubsub");
    user = rgw_user(config["tenant"], uid);
    data_bucket_prefix = config["data_bucket_prefix"]("pubsub-");
    data_oid_prefix = config["data_oid_prefix"];

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

    ldout(cct, 5) << "pubsub: module config (parsed representation):\n" << json_str("config", *this, true) << dendl;
  }

  void init_instance(RGWRealm& realm, uint64_t instance_id) {
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

      ldout(cct, 10) << ": found topic for path=" << bucket << "/" << key << ": id=" << target.id << " target_path=" << target.path << ", topic=" << target.topic << dendl;
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

    event->id = buf;

    return buf;
  }

  void format(bufferlist *bl) {
    bl->append(json_str("event", *event));
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
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  PSSubConfigRef sub_conf;
  shared_ptr<rgw_get_bucket_info_result> get_bucket_info_result;
  RGWBucketInfo *bucket_info{nullptr};
  RGWDataAccessRef data_access;
  RGWDataAccess::BucketRef bucket;

  class InitCR;
  InitCR *init_cr{nullptr};

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
    string oid_prefix;
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
      oid_prefix = sub->sub_conf->data_oid_prefix;
    }

    int operate() override {
      reenter(this) {

        put_obj.bucket = sub->bucket;
        put_obj.key = rgw_obj_key(oid_prefix + pse.generate_message_id());

        pse.format(&put_obj.data);
        
        yield call(new RGWObjectSimplePutCR(sync_env->async_rados,
                                            sync_env->store,
                                            put_obj));
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to store event: " << put_obj.bucket << "/" << put_obj.key << " ret=" << retcode << dendl;
          return set_cr_error(retcode);
        }

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
                 rgw_pubsub_user_sub_config& user_sub_conf) : sync_env(_sync_env),
                                      env(_env),
                                      data_access(std::make_shared<RGWDataAccess>(sync_env->store)) {
    sub_conf->from_user_conf(user_sub_conf);
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

  static RGWCoroutine *store_event_cr(RGWDataSyncEnv *sync_env, PSSubscriptionRef& sub, EventRef& event) {
    return new StoreEventCR(sync_env, sub, event);
  }

  friend class InitCR;
};


class PSManager
{
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;

  map<string, PSSubscriptionRef> subs;

  class GetSubCR : public RGWSingletonCR<PSSubscriptionRef> {
    RGWDataSyncEnv *sync_env;
    PSManagerRef mgr;
    rgw_user owner;
    string sub_name;
    string sub_id;
    PSSubscriptionRef *ref;

    PSConfigRef conf;

    PSSubConfigRef sub_conf;
    rgw_pubsub_user_sub_config user_sub_conf;
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
    ~GetSubCR() {
    }

    int operate() override {
      reenter(this) {
        if (owner.empty()) {
          if (!conf->find_sub(sub_name, &sub_conf)) {
            ldout(sync_env->cct, 0) << "ERROR: could not find subscription config: name=" << sub_name << dendl;
            mgr->remove_get_sub(owner, sub_name);
            return set_cr_error(-ENOENT);
          }

          *ref = PSSubscription::get_shared(sync_env, mgr->env, sub_conf);
        } else {
          using ReadInfoCR = RGWSimpleRadosReadCR<rgw_pubsub_user_sub_config>;
          yield {
            RGWUserPubSub ups(sync_env->store, owner);
            rgw_raw_obj obj;
            ups.get_sub_meta_obj(sub_name, &obj);
            bool empty_on_enoent = false;
            call(new ReadInfoCR(sync_env->async_rados, sync_env->store,
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
          ldout(sync_env->cct, 0) << "ERROR: failed to init subscription" << dendl;
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

  map<string, GetSubCR *> get_subs;

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

  static int call_get_subscription_cr(RGWDataSyncEnv *sync_env, PSManagerRef& mgr, RGWCoroutine *caller, const rgw_user& owner, const string& sub_name, PSSubscriptionRef *ref) {
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
                       PSEnvRef& _env) : RGWCoroutine(_sync_env->cct),
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

      ldout(sync_env->cct, 20) << "pubsub: get user info cr returned: " << json_str("obj", *env->data_user_info, true) << dendl;


      return set_cr_done();
    }
    return 0;
  }
};

class RGWPSFindBucketTopicsCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  rgw_user owner;
  rgw_bucket bucket;
  rgw_obj_key key;

  RGWUserPubSub ups;

  rgw_raw_obj obj;
  rgw_pubsub_user_topics bucket_topics;
  TopicsRef *topics;
public:
  RGWPSFindBucketTopicsCR(RGWDataSyncEnv *_sync_env,
                      PSEnvRef& _env,
                      const rgw_user& _owner,
                      const rgw_bucket& _bucket,
                      const rgw_obj_key& _key,
                      TopicsRef *_topics) : RGWCoroutine(_sync_env->cct),
                                                          sync_env(_sync_env),
                                                          env(_env),
                                                          owner(_owner),
                                                          bucket(_bucket),
                                                          key(_key),
                                                          ups(_sync_env->store, owner),
                                                          topics(_topics) {
    *topics = std::make_shared<vector<PSTopicConfigRef> >();
  }
  int operate() override {
    reenter(this) {
      ups.get_bucket_meta_obj(bucket, &obj);


      using ReadInfoCR = RGWSimpleRadosReadCR<rgw_pubsub_user_topics>;
      yield {
        bool empty_on_enoent = true;
        call(new ReadInfoCR(sync_env->async_rados, sync_env->store,
                            obj,
                            &bucket_topics, empty_on_enoent));
      }
      if (retcode < 0 && retcode != -ENOENT) {
        return set_cr_error(retcode);
      }

      ldout(sync_env->cct, 20) << "RGWPSFindBucketTopicsCR(): found " << bucket_topics.topics.size() << " topics for bucket " << bucket << dendl;

      for (auto& titer : bucket_topics.topics) {
        auto& info = titer.second;
        shared_ptr<PSTopicConfig> tc = std::make_shared<PSTopicConfig>();
        tc->name = info.topic.name;
        tc->subs = info.subs;
        (*topics)->push_back(tc);
      }

      env->conf->get_topics(sync_env->cct, bucket, key, topics);
      return set_cr_done();
    }
    return 0;
  }
};

class RGWPSHandleObjEvent : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  std::array<rgw_user, 2> owners;
  rgw_user& owner;
  rgw_user& no_owner;
  std::array<rgw_user, 2>::iterator oiter;
  EventRef event;

  vector<PSTopicConfigRef>::iterator titer;
  set<string>::iterator siter;
  PSSubscriptionRef sub;
  TopicsRef topics;
public:
  RGWPSHandleObjEvent(RGWDataSyncEnv *_sync_env,
                      PSEnvRef _env,
                      const rgw_user& _owner,
                      EventRef& _event,
                      TopicsRef& _topics) : RGWCoroutine(_sync_env->cct),
                                          sync_env(_sync_env),
                                          env(_env),
                                          owner(owners[0]),
                                          no_owner(owners[1]),
                                          event(_event),
                                          topics(_topics) {
    owner = _owner;
  }
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 10) << ": handle event: obj: z=" << sync_env->source_zone
                               << " event=" << json_str("event", *event, false) << dendl;

      ldout(sync_env->cct, 20) << "pubsub: " << topics->size() << " topics found for path" << dendl;

      for (titer = topics->begin(); titer != topics->end(); ++titer) {
        ldout(sync_env->cct, 10) << ": notification for " << event->bucket << "/" << event->key << ": topic=" << (*titer)->name << ", has " << (*titer)->subs.size() << " subscriptions" << dendl;

        for (siter = (*titer)->subs.begin(); siter != (*titer)->subs.end(); ++siter) {
          ldout(sync_env->cct, 10) << ": subscription: " << *siter << dendl;

          for (oiter = owners.begin(); oiter != owners.end(); ++oiter) {
            /*
             * once for the global subscriptions, once for the user specific subscriptions
             */
            yield PSManager::call_get_subscription_cr(sync_env, env->manager, this, *oiter, *siter, &sub);
            if (retcode < 0) {
              ldout(sync_env->cct, 10) << "ERROR: failed to find subscription config for subscription=" << *siter << " ret=" << retcode << dendl;
              continue;
            }

            yield call(PSSubscription::store_event_cr(sync_env, sub, event));
            if (retcode < 0) {
              ldout(sync_env->cct, 10) << "ERROR: failed to store event for subscription=" << *siter << " ret=" << retcode << dendl;
              continue;
            }
          }

#warning push notification
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


class RGWPSHandleRemoteObjCBCR : public RGWStatRemoteObjCBCR {
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  uint64_t versioned_epoch;
  EventRef event;
  TopicsRef topics;
public:
  RGWPSHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                          PSEnvRef _env, uint64_t _versioned_epoch,
                          TopicsRef& _topics) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key),
                                                                      sync_env(_sync_env),
                                                                      env(_env),
                                                                      versioned_epoch(_versioned_epoch),
                                                                      topics(_topics) {
  }
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 10) << ": stat of remote obj: z=" << sync_env->source_zone
                               << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                               << " attrs=" << attrs << dendl;
      make_event_ref(&event);
      event->bucket = bucket_info.bucket;
      event->key = key;
      event->mtime = mtime;
      event->event = OBJECT_CREATE;
      event->timestamp = real_clock::now();
      {
        for (auto& attr : attrs) {
          string k = attr.first;
          if (boost::algorithm::starts_with(k, RGW_ATTR_PREFIX)) {
            k = k.substr(sizeof(RGW_ATTR_PREFIX) - 1);
          }
          string v = attr.second.to_str();
          auto p = std::make_pair(k, v);
          event->attrs.push_back(p);
        }
      }

      yield call(new RGWPSHandleObjEvent(sync_env, env, bucket_info.owner, event, topics));
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
  uint64_t versioned_epoch;
  TopicsRef topics;
public:
  RGWPSHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                        PSEnvRef _env, uint64_t _versioned_epoch,
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
  uint64_t versioned_epoch;
  TopicsRef topics;
public:
  RGWPSHandleObjCreateCR(RGWDataSyncEnv *_sync_env,
                       RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                       PSEnvRef _env, uint64_t _versioned_epoch) : RGWCoroutine(_sync_env->cct),
                                                                   sync_env(_sync_env),
                                                                   bucket_info(_bucket_info),
                                                                   key(_key),
                                                                   env(_env),
                                                                   versioned_epoch(_versioned_epoch) {
  }

  ~RGWPSHandleObjCreateCR() override {}

  int operate() override {
    reenter(this) {
      yield call(new RGWPSFindBucketTopicsCR(sync_env, env, bucket_info.owner, bucket_info.bucket, key, &topics));
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: RGWPSFindBucketTopicsCR returned ret=" << retcode << dendl;
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

class RGWPSGenericObjEventCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  PSEnvRef env;
  rgw_user owner;
  rgw_bucket bucket;
  rgw_obj_key key;
  ceph::real_time mtime;
  RGWPubSubEventType event_type;
  EventRef event;
  TopicsRef topics;
public:
  RGWPSGenericObjEventCBCR(RGWDataSyncEnv *_sync_env,
                           PSEnvRef _env,
                           RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                           RGWPubSubEventType _event_type) : RGWCoroutine(_sync_env->cct),
                                                             sync_env(_sync_env),
                                                             env(_env),
                                                             owner(_bucket_info.owner),
                                                             bucket(_bucket_info.bucket),
                                                             key(_key),
                                                             mtime(_mtime), event_type(_event_type) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 10) << ": remove remote obj: z=" << sync_env->source_zone
                               << " b=" << bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield call(new RGWPSFindBucketTopicsCR(sync_env, env, owner, bucket, key, &topics));
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: RGWPSFindBucketTopicsCR returned ret=" << retcode << dendl;
        return set_cr_error(retcode);
      }
      if (topics->empty()) {
        ldout(sync_env->cct, 20) << "no topics found for " << bucket << "/" << key << dendl;
        return set_cr_done();
      }
      make_event_ref(&event);
      event->event = event_type;
      event->bucket = bucket;
      event->key = key;
      event->mtime = mtime;
      event->timestamp = real_clock::now();

      yield call(new RGWPSHandleObjEvent(sync_env, env, owner, event, topics));
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

  RGWCoroutine *start_sync(RGWDataSyncEnv *sync_env) override {
    ldout(sync_env->cct, 5) << conf->id << ": start" << dendl;
    return new RGWPSInitEnvCBCR(sync_env, env);
  }
  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWPSHandleObjCreateCR(sync_env, bucket_info, key, env, versioned_epoch);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWPSGenericObjEventCBCR(sync_env, env, bucket_info, key, mtime, OBJECT_DELETE);
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 10) << conf->id << ": create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWPSGenericObjEventCBCR(sync_env, env, bucket_info, key, mtime, DELETE_MARKER_CREATE);
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

