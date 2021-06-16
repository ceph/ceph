// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "include/random.h"
#include "include/Context.h"
#include "common/errno.h"

#include "svc_notify.h"
#include "svc_finisher.h"
#include "svc_zone.h"
#include "svc_rados.h"

#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

static string notify_oid_prefix = "notify";

class RGWWatcher : public DoutPrefixProvider , public librados::WatchCtx2 {
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

  CephContext *get_cct() const { return cct; }
  unsigned get_subsys() const { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const { return out << "rgw watcher librados: "; }

public:
  RGWWatcher(CephContext *_cct, RGWSI_Notify *s, int i, RGWSI_RADOS::Obj& o) : cct(_cct), svc(s), index(i), obj(o), watch_handle(0) {}
  void handle_notify(uint64_t notify_id,
		     uint64_t cookie,
		     uint64_t notifier_id,
		     bufferlist& bl) override {
    ldpp_dout(this, 10) << "RGWWatcher::handle_notify() "
                   << " notify_id " << notify_id
                   << " cookie " << cookie
                   << " notifier " << notifier_id
                   << " bl.length()=" << bl.length() << dendl;

    if (unlikely(svc->inject_notify_timeout_probability == 1) ||
	(svc->inject_notify_timeout_probability > 0 &&
         (svc->inject_notify_timeout_probability >
	  ceph::util::generate_random_number(0.0, 1.0)))) {
      ldpp_dout(this, 0)
	<< "RGWWatcher::handle_notify() dropping notification! "
	<< "If this isn't what you want, set "
	<< "rgw_inject_notify_timeout_probability to zero!" << dendl;
      return;
    }

    svc->watch_cb(this, notify_id, cookie, notifier_id, bl);

    bufferlist reply_bl; // empty reply payload
    obj.notify_ack(notify_id, cookie, reply_bl);
  }
  void handle_error(uint64_t cookie, int err) override {
    ldpp_dout(this, -1) << "RGWWatcher::handle_error cookie " << cookie
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
    register_completion = librados::Rados::aio_create_completion(nullptr, nullptr);
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
    register_completion->wait_for_complete();
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

string RGWSI_Notify::get_control_oid(int i)
{
  char buf[notify_oid_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", notify_oid_prefix.c_str(), i);

  return string(buf);
}

// do not call pick_obj_control before init_watch
RGWSI_RADOS::Obj RGWSI_Notify::pick_control_obj(const string& key)
{
  uint32_t r = ceph_str_hash_linux(key.c_str(), key.size());

  int i = r % num_watchers;
  return notify_objs[i];
}

int RGWSI_Notify::init_watch(const DoutPrefixProvider *dpp, optional_yield y)
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

    notify_objs[i] = rados_svc->handle().obj({control_pool, notify_oid});
    auto& notify_obj = notify_objs[i];

    int r = notify_obj.open(dpp);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: notify_obj.open() returned r=" << r << dendl;
      return r;
    }

    librados::ObjectWriteOperation op;
    op.create(false);
    r = notify_obj.operate(dpp, &op, y);
    if (r < 0 && r != -EEXIST) {
      ldpp_dout(dpp, 0) << "ERROR: notify_obj.operate() returned r=" << r << dendl;
      return r;
    }

    RGWWatcher *watcher = new RGWWatcher(cct, this, i, notify_obj);
    watchers[i] = watcher;

    r = watcher->register_watch_async();
    if (r < 0) {
      ldpp_dout(dpp, 0) << "WARNING: register_watch_aio() returned " << r << dendl;
      error = r;
      continue;
    }
  }

  for (int i = 0; i < num_watchers; ++i) {
    int r = watchers[i]->register_watch_finish();
    if (r < 0) {
      ldpp_dout(dpp, 0) << "WARNING: async watch returned " << r << dendl;
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
  for (int i = 0; i < num_watchers; i++) {
    RGWWatcher *watcher = watchers[i];
    watcher->unregister_watch();
    delete watcher;
  }

  delete[] watchers;
}

int RGWSI_Notify::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{
  int r = zone_svc->start(y, dpp);
  if (r < 0) {
    return r;
  }

  assert(zone_svc->is_started()); /* otherwise there's an ordering problem */

  r = rados_svc->start(y, dpp);
  if (r < 0) {
    return r;
  }
  r = finisher_svc->start(y, dpp);
  if (r < 0) {
    return r;
  }

  control_pool = zone_svc->get_zone_params().control_pool;

  int ret = init_watch(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to initialize watch: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  shutdown_cb = new RGWSI_Notify_ShutdownCB(this);
  int handle;
  finisher_svc->register_caller(shutdown_cb, &handle);
  finisher_handle = handle;

  return 0;
}

void RGWSI_Notify::shutdown()
{
  if (finalized) {
    return;
  }

  if (finisher_handle) {
    finisher_svc->unregister_caller(*finisher_handle);
  }
  finalize_watch();

  delete shutdown_cb;

  finalized = true;
}

RGWSI_Notify::~RGWSI_Notify()
{
  shutdown();
}

int RGWSI_Notify::unwatch(RGWSI_RADOS::Obj& obj, uint64_t watch_handle)
{
  int r = obj.unwatch(watch_handle);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: rados->unwatch2() returned r=" << r << dendl;
    return r;
  }
  r = rados_svc->handle().watch_flush();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: rados->watch_flush() returned r=" << r << dendl;
    return r;
  }
  return 0;
}

void RGWSI_Notify::add_watcher(int i)
{
  ldout(cct, 20) << "add_watcher() i=" << i << dendl;
  std::unique_lock l{watchers_lock};
  watchers_set.insert(i);
  if (watchers_set.size() ==  (size_t)num_watchers) {
    ldout(cct, 2) << "all " << num_watchers << " watchers are set, enabling cache" << dendl;
    _set_enabled(true);
  }
}

void RGWSI_Notify::remove_watcher(int i)
{
  ldout(cct, 20) << "remove_watcher() i=" << i << dendl;
  std::unique_lock l{watchers_lock};
  size_t orig_size = watchers_set.size();
  watchers_set.erase(i);
  if (orig_size == (size_t)num_watchers &&
      watchers_set.size() < orig_size) { /* actually removed */
    ldout(cct, 2) << "removed watcher, disabling cache" << dendl;
    _set_enabled(false);
  }
}

int RGWSI_Notify::watch_cb(const DoutPrefixProvider *dpp,
                           uint64_t notify_id,
                           uint64_t cookie,
                           uint64_t notifier_id,
                           bufferlist& bl)
{
  std::shared_lock l{watchers_lock};
  if (cb) {
    return cb->watch_cb(dpp, notify_id, cookie, notifier_id, bl);
  }
  return 0;
}

void RGWSI_Notify::set_enabled(bool status)
{
  std::unique_lock l{watchers_lock};
  _set_enabled(status);
}

void RGWSI_Notify::_set_enabled(bool status)
{
  enabled = status;
  if (cb) {
    cb->set_enabled(status);
  }
}

int RGWSI_Notify::distribute(const DoutPrefixProvider *dpp, const string& key, bufferlist& bl,
                             optional_yield y)
{
  /* The RGW uses the control pool to store the watch notify objects.
    The precedence in RGWSI_Notify::do_start is to call to zone_svc->start and later to init_watch().
    The first time, RGW starts in the cluster, the RGW will try to create zone and zonegroup system object.
    In that case RGW will try to distribute the cache before it ran init_watch,
    which will lead to division by 0 in pick_obj_control (num_watchers is 0).
  */
  if (num_watchers > 0) {
    RGWSI_RADOS::Obj notify_obj = pick_control_obj(key);

    ldpp_dout(dpp, 10) << "distributing notification oid=" << notify_obj.get_ref().obj
        << " bl.length()=" << bl.length() << dendl;
    return robust_notify(dpp, notify_obj, bl, y);
  }
  return 0;
}

int RGWSI_Notify::robust_notify(const DoutPrefixProvider *dpp, 
                                RGWSI_RADOS::Obj& notify_obj, bufferlist& bl,
                                optional_yield y)
{
  // The reply of every machine that acks goes in here.
  boost::container::flat_set<std::pair<uint64_t, uint64_t>> acks;
  bufferlist rbl;

  // First, try to send, without being fancy about it.
  auto r = notify_obj.notify(dpp, bl, 0, &rbl, y);

  // If that doesn't work, get serious.
  if (r < 0) {
    ldpp_dout(dpp, 1) << "robust_notify: If at first you don't succeed: "
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
	ldpp_dout(dpp, 20) << "robust_notify: acked by " << id << dendl;
	uint32_t blen;
	decode(blen, p);
	p += blen;
      }
    } catch (const buffer::error& e) {
      ldpp_dout(dpp, 0) << "robust_notify: notify response parse failed: "
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
      r = notify_obj.notify(dpp, bl, 0, &rbl, y);
      if (r < 0) {
	ldpp_dout(dpp, 1) << "robust_notify: retry " << tries << " failed: "
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
	      ldpp_dout(dpp, 20) << "robust_notify: acked by " << id << dendl;
	    }
	    uint32_t blen;
	    decode(blen, p);
	    p += blen;
	  }

	  uint32_t num_timeouts;
	  decode(num_timeouts, p);
	  for (auto i = 0u; i < num_timeouts; ++i) {
	    std::pair<uint64_t, uint64_t> id;
	    decode(id, p);
	    // Only track timeouts from hosts that haven't acked previously.
	    if (acks.find(id) != acks.cend()) {
	      ldpp_dout(dpp, 20) << "robust_notify: " << id << " timed out."
			     << dendl;
	      timeouts.insert(id);
	    }
	  }
	} catch (const buffer::error& e) {
	  ldpp_dout(dpp, 0) << "robust_notify: notify response parse failed: "
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
  std::unique_lock l{watchers_lock};
  cb = _cb;
  _set_enabled(enabled);
}

void RGWSI_Notify::schedule_context(Context *c)
{
  finisher_svc->schedule_context(c);
}
