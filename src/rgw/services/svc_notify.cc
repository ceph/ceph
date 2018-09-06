#include "include/random.h"
#include "common/errno.h"

#include "svc_notify.h"
#include "svc_finisher.h"
#include "svc_zone.h"
#include "svc_rados.h"

#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

static string notify_oid_prefix = "notify";

int RGWS_Notify::create_instance(const string& conf, RGWServiceInstanceRef *instance)
{
  instance->reset(new RGWSI_Notify(this, cct));
  return 0;
}

class RGWWatcher : public librados::WatchCtx2 {
  CephContext *cct;
  RGWSI_Notify *svc;
  int index;
  RGWSI_RADOS::Obj obj;
  uint64_t watch_handle;
  int register_ret{0};
  librados::AioCompletion *register_completion{nullptr};

  class C_ReinitWatch : public Context {
    RGWWatcher *watcher;
    public:
      explicit C_ReinitWatch(RGWWatcher *_watcher) : watcher(_watcher) {}
      void finish(int r) override {
        watcher->reinit();
      }
  };
public:
  RGWWatcher(CephContext *_cct, RGWSI_Notify *s, int i, RGWSI_RADOS::Obj& o) : cct(_cct), svc(s), index(i), obj(o), watch_handle(0) {}
  void handle_notify(uint64_t notify_id,
		     uint64_t cookie,
		     uint64_t notifier_id,
		     bufferlist& bl) override {
    ldout(cct, 10) << "RGWWatcher::handle_notify() "
                   << " notify_id " << notify_id
                   << " cookie " << cookie
                   << " notifier " << notifier_id
                   << " bl.length()=" << bl.length() << dendl;

    if (unlikely(svc->inject_notify_timeout_probability == 1) ||
	(svc->inject_notify_timeout_probability > 0 &&
         (svc->inject_notify_timeout_probability >
	  ceph::util::generate_random_number(0.0, 1.0)))) {
      ldout(cct, 0)
	<< "RGWWatcher::handle_notify() dropping notification! "
	<< "If this isn't what you want, set "
	<< "rgw_inject_notify_timeout_probability to zero!" << dendl;
      return;
    }

    svc->watch_cb(notify_id, cookie, notifier_id, bl);

    bufferlist reply_bl; // empty reply payload
    obj.notify_ack(notify_id, cookie, reply_bl);
  }
  void handle_error(uint64_t cookie, int err) override {
    lderr(cct) << "RGWWatcher::handle_error cookie " << cookie
			<< " err " << cpp_strerror(err) << dendl;
    svc->remove_watcher(index);
    svc->schedule_context(new C_ReinitWatch(this));
  }

  void reinit() {
    int ret = unregister_watch();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: unregister_watch() returned ret=" << ret << dendl;
      return;
    }
    ret = register_watch();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: register_watch() returned ret=" << ret << dendl;
      return;
    }
  }

  int unregister_watch() {
    int r = svc->unwatch(obj, watch_handle);
    if (r < 0) {
      return r;
    }
    svc->remove_watcher(index);
    return 0;
  }

  int register_watch_async() {
    if (register_completion) {
      register_completion->release();
      register_completion = nullptr;
    }
    register_completion = librados::Rados::aio_create_completion(nullptr, nullptr, nullptr);
    register_ret = obj.aio_watch(register_completion, &watch_handle, this);
    if (register_ret < 0) {
      register_completion->release();
      return register_ret;
    }
    return 0;
  }

  int register_watch_finish() {
    if (register_ret < 0) {
      return register_ret;
    }
    if (!register_completion) {
      return -EINVAL;
    }
    register_completion->wait_for_safe();
    int r = register_completion->get_return_value();
    register_completion->release();
    register_completion = nullptr;
    if (r < 0) {
      return r;
    }
    svc->add_watcher(index);
    return 0;
  }

  int register_watch() {
    int r = obj.watch(&watch_handle, this);
    if (r < 0) {
      return r;
    }
    svc->add_watcher(index);
    return 0;
  }
};


class RGWSI_Notify_ShutdownCB : public RGWSI_Finisher::ShutdownCB
{
  RGWSI_Notify *svc;
public:
  RGWSI_Notify_ShutdownCB(RGWSI_Notify *_svc) : svc(_svc) {}
  void call() override {
    svc->shutdown();
  }
};

std::map<string, RGWServiceInstance::dependency> RGWSI_Notify::get_deps()
{
  map<string, RGWServiceInstance::dependency> deps;
  deps["zone_dep"] = { .name = "zone",
                       .conf = "{}" };
  deps["rados_dep"] = { .name = "rados",
                        .conf = "{}" };
  deps["finisher_dep"] = { .name = "finisher",
                        .conf = "{}" };
  return deps;
}

int RGWSI_Notify::load(const string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs)
{
  zone_svc = static_pointer_cast<RGWSI_Zone>(dep_refs["zone_dep"]);
  assert(zone_svc);
  rados_svc = static_pointer_cast<RGWSI_RADOS>(dep_refs["rados_dep"]);
  assert(rados_svc);
  finisher_svc = static_pointer_cast<RGWSI_Finisher>(dep_refs["finisher_dep"]);
  assert(finisher_svc);
  return 0;
}

string RGWSI_Notify::get_control_oid(int i)
{
  char buf[notify_oid_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", notify_oid_prefix.c_str(), i);

  return string(buf);
}

RGWSI_RADOS::Obj RGWSI_Notify::pick_control_obj(const string& key)
{
  uint32_t r = ceph_str_hash_linux(key.c_str(), key.size());

  int i = r % num_watchers;
  return notify_objs[i];
}

int RGWSI_Notify::init_watch()
{
  num_watchers = cct->_conf->rgw_num_control_oids;

  bool compat_oid = (num_watchers == 0);

  if (num_watchers <= 0)
    num_watchers = 1;

  watchers = new RGWWatcher *[num_watchers];

  int error = 0;

  notify_objs.resize(num_watchers);

  for (int i=0; i < num_watchers; i++) {
    string notify_oid;

    if (!compat_oid) {
      notify_oid = get_control_oid(i);
    } else {
      notify_oid = notify_oid_prefix;
    }

    notify_objs[i] = rados_svc->handle(0).obj({control_pool, notify_oid});
    auto& notify_obj = notify_objs[i];

    librados::ObjectWriteOperation op;
    op.create(false);
    int r = notify_obj.operate(&op);
    if (r < 0 && r != -EEXIST) {
      return r;
    }

    RGWWatcher *watcher = new RGWWatcher(cct, this, i, notify_obj);
    watchers[i] = watcher;

    r = watcher->register_watch_async();
    if (r < 0) {
      ldout(cct, 0) << "WARNING: register_watch_aio() returned " << r << dendl;
      error = r;
      continue;
    }
  }

  for (int i = 0; i < num_watchers; ++i) {
    int r = watchers[i]->register_watch_finish();
    if (r < 0) {
      ldout(cct, 0) << "WARNING: async watch returned " << r << dendl;
      error = r;
    }
  }

  if (error < 0) {
    return error;
  }

  return 0;
}

void RGWSI_Notify::finalize_watch()
{
  if (finalized) {
    return;
  }

  for (int i = 0; i < num_watchers; i++) {
    RGWWatcher *watcher = watchers[i];
    watcher->unregister_watch();
    delete watcher;
  }

  delete[] watchers;
}

int RGWSI_Notify::init()
{
  control_pool = zone_svc->get_zone_params().control_pool;

  int ret = init_watch();
  if (ret < 0) {
    lderr(cct) << "ERROR: failed to initialize watch: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  shutdown_cb = new RGWSI_Notify_ShutdownCB(this);
  finisher_svc->register_caller(shutdown_cb, &finisher_handle);

  return 0;
}

void RGWSI_Notify::shutdown()
{
  finisher_svc->unregister_caller(finisher_handle);
  finalize_watch();
}

int RGWSI_Notify::unwatch(RGWSI_RADOS::Obj& obj, uint64_t watch_handle)
{
  int r = obj.unwatch(watch_handle);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: rados->unwatch2() returned r=" << r << dendl;
    return r;
  }
  r = rados_svc->handle(0).watch_flush();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: rados->watch_flush() returned r=" << r << dendl;
    return r;
  }
  return 0;
}

void RGWSI_Notify::add_watcher(int i)
{
  ldout(cct, 20) << "add_watcher() i=" << i << dendl;
  RWLock::WLocker l(watchers_lock);
  watchers_set.insert(i);
  if (watchers_set.size() ==  (size_t)num_watchers) {
    ldout(cct, 2) << "all " << num_watchers << " watchers are set, enabling cache" << dendl;
    set_enabled(true);
  }
}

void RGWSI_Notify::remove_watcher(int i)
{
  ldout(cct, 20) << "remove_watcher() i=" << i << dendl;
  RWLock::WLocker l(watchers_lock);
  size_t orig_size = watchers_set.size();
  watchers_set.erase(i);
  if (orig_size == (size_t)num_watchers &&
      watchers_set.size() < orig_size) { /* actually removed */
    ldout(cct, 2) << "removed watcher, disabling cache" << dendl;
    set_enabled(false);
  }
}

int RGWSI_Notify::watch_cb(uint64_t notify_id,
                           uint64_t cookie,
                           uint64_t notifier_id,
                           bufferlist& bl)
{
  RWLock::RLocker l(watchers_lock);
  if (cb) {
    return cb->watch_cb(notify_id, cookie, notifier_id, bl);
  }
  return 0;
}

void RGWSI_Notify::set_enabled(bool status)
{
  RWLock::RLocker l(watchers_lock);
  if (cb) {
    cb->set_enabled(status);
  }
}

int RGWSI_Notify::distribute(const string& key, bufferlist& bl)
{
  RGWSI_RADOS::Obj notify_obj = pick_control_obj(key);

  ldout(cct, 10) << "distributing notification oid=" << notify_obj.get_ref().oid << " bl.length()=" << bl.length() << dendl;
  return robust_notify(notify_obj, bl);
}

int RGWSI_Notify::robust_notify(RGWSI_RADOS::Obj& notify_obj, bufferlist& bl)
{
  // The reply of every machine that acks goes in here.
  boost::container::flat_set<std::pair<uint64_t, uint64_t>> acks;
  bufferlist rbl;

  // First, try to send, without being fancy about it.
  auto r = notify_obj.notify(bl, 0, &rbl);

  // If that doesn't work, get serious.
  if (r < 0) {
    ldout(cct, 1) << "robust_notify: If at first you don't succeed: "
		  << cpp_strerror(-r) << dendl;


    auto p = rbl.cbegin();
    // Gather up the replies to the first attempt.
    try {
      uint32_t num_acks;
      decode(num_acks, p);
      // Doing this ourselves since we don't care about the payload;
      for (auto i = 0u; i < num_acks; ++i) {
	std::pair<uint64_t, uint64_t> id;
	decode(id, p);
	acks.insert(id);
	ldout(cct, 20) << "robust_notify: acked by " << id << dendl;
	uint32_t blen;
	decode(blen, p);
	p.advance(blen);
      }
    } catch (const buffer::error& e) {
      ldout(cct, 0) << "robust_notify: notify response parse failed: "
		    << e.what() << dendl;
      acks.clear(); // Throw away junk on failed parse.
    }


    // Every machine that fails to reply and hasn't acked a previous
    // attempt goes in here.
    boost::container::flat_set<std::pair<uint64_t, uint64_t>> timeouts;

    auto tries = 1u;
    while (r < 0 && tries < max_notify_retries) {
      ++tries;
      rbl.clear();
      // Reset the timeouts, we're only concerned with new ones.
      timeouts.clear();
      r = notify_obj.notify(bl, 0, &rbl);
      if (r < 0) {
	ldout(cct, 1) << "robust_notify: retry " << tries << " failed: "
		      << cpp_strerror(-r) << dendl;
	p = rbl.begin();
	try {
	  uint32_t num_acks;
	  decode(num_acks, p);
	  // Not only do we not care about the payload, but we don't
	  // want to empty the container; we just want to augment it
	  // with any new members.
	  for (auto i = 0u; i < num_acks; ++i) {
	    std::pair<uint64_t, uint64_t> id;
	    decode(id, p);
	    auto ir = acks.insert(id);
	    if (ir.second) {
	      ldout(cct, 20) << "robust_notify: acked by " << id << dendl;
	    }
	    uint32_t blen;
	    decode(blen, p);
	    p.advance(blen);
	  }

	  uint32_t num_timeouts;
	  decode(num_timeouts, p);
	  for (auto i = 0u; i < num_timeouts; ++i) {
	    std::pair<uint64_t, uint64_t> id;
	    decode(id, p);
	    // Only track timeouts from hosts that haven't acked previously.
	    if (acks.find(id) != acks.cend()) {
	      ldout(cct, 20) << "robust_notify: " << id << " timed out."
			     << dendl;
	      timeouts.insert(id);
	    }
	  }
	} catch (const buffer::error& e) {
	  ldout(cct, 0) << "robust_notify: notify response parse failed: "
			<< e.what() << dendl;
	  continue;
	}
	// If we got a good parse and timeouts is empty, that means
	// everyone who timed out in one call received the update in a
	// previous one.
	if (timeouts.empty()) {
	  r = 0;
	}
      }
    }
  }
  return r;
}

void RGWSI_Notify::register_watch_cb(CB *_cb)
{
  RWLock::WLocker l(watchers_lock);
  cb = _cb;
}

void RGWSI_Notify::schedule_context(Context *c)
{
  finisher_svc->schedule_context(c);
}
