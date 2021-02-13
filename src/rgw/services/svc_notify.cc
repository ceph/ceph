// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "include/random.h"
#include "include/Context.h"
#include "common/errno.h"
#include "common/async/waiter.h"

#include "svc_notify.h"
#include "svc_finisher.h"
#include "svc_zone.h"
#include "svc_rados.h"

#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

static std::string notify_oid_prefix = "notify";

class RGWWatcher {
  CephContext *cct;
  RGWSI_Notify *svc;
  int index;
  neo_obj_ref obj;
  uint64_t watch_handle;
  bs::error_code register_ret;
  std::optional<ceph::async::waiter<bs::error_code,
				    uint64_t>> register_completion;

  auto watch_handler() {
    return [this](bs::error_code ec,
                  uint64_t notify_id,
                  uint64_t cookie,
                  uint64_t notifier_id,
                  cb::list&& bl) {
             if (ec)
               handle_error(cookie, ec);
             else
               handle_notify(notify_id, cookie, notifier_id,
                             std::move(bl));
           };
  }


public:
  RGWWatcher(CephContext *_cct, RGWSI_Notify *s, int i, neo_obj_ref& o)
    : cct(_cct), svc(s), index(i), obj(o), watch_handle(0) {}
  void handle_notify(uint64_t notify_id,
		     uint64_t cookie,
		     uint64_t notifier_id,
		     cb::list&& bl) {
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

    obj.notify_ack(notify_id, cookie, {}, null_yield);
  }
  void handle_error(uint64_t cookie, bs::error_code err) {
    lderr(cct) << "RGWWatcher::handle_error cookie " << cookie
               << " err " << err << dendl;
    svc->remove_watcher(index);
    svc->finisher_svc->schedule(std::bind(&RGWWatcher::reinit, this));
  }

  void reinit() {
    auto ret = unregister_watch();
    if (ret) {
      ldout(cct, 0) << "ERROR: unregister_watch() returned ret="
		    << ret.message() << dendl;
      return;
    }
    ret = register_watch();
    if (ret) {
      ldout(cct, 0) << "ERROR: register_watch() returned ret="
		    << ret.message() << dendl;
      return;
    }
  }

  bs::error_code unregister_watch() {
    auto r = svc->unwatch(obj, watch_handle);
    if (r) {
      return r;
    }
    svc->remove_watcher(index);
    return {};
  }

  void register_watch_async() {
    if (register_completion) {
      register_completion.reset();
    }
    register_completion.emplace();
    // This actually gets a waiter rather than a use_blocked so we can
    // start the operation in one function and wait on it in another.
    obj.watch(watch_handler(), register_completion->ref());
  }

  bs::error_code register_watch_finish() {
    if (!register_completion) {
      return bs::error_code(EINVAL,
				       bs::system_category());
    }
    std::tie(register_ret, watch_handle) = register_completion->wait();
    register_completion.reset();
    if (!register_ret)
      svc->add_watcher(index);
    return register_ret;
  }

  bs::error_code register_watch() {
    bs::error_code ec;
    auto r = obj.watch(watch_handler(), null_yield[ec]);
    if (!ec) {
      watch_handle = r;
      svc->add_watcher(index);
      return {};
    } else {
      return ec;
    }
  }
};


string RGWSI_Notify::get_control_oid(int i)
{
  char buf[notify_oid_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", notify_oid_prefix.c_str(), i);

  return string(buf);
}

// do not call pick_obj_control before init_watch
neo_obj_ref& RGWSI_Notify::pick_control_obj(const string& key)
{
  uint32_t r = ceph_str_hash_linux(key.c_str(), key.size());

  int i = r % num_watchers;
  return notify_objs[i];
}

bs::error_code RGWSI_Notify::init_watch(optional_yield y)
{
  num_watchers = cct->_conf->rgw_num_control_oids;

  bool compat_oid = (num_watchers == 0);

  if (num_watchers <= 0)
    num_watchers = 1;

  watchers = new RGWWatcher *[num_watchers];

  bs::error_code error;

  notify_objs.clear();
  notify_objs.reserve(num_watchers);

  for (int i=0; i < num_watchers; i++) {
    string notify_oid;

    if (!compat_oid) {
      notify_oid = get_control_oid(i);
    } else {
      notify_oid = notify_oid_prefix;
    }

    auto res = rados->acquire_obj({control_pool, notify_oid}, null_yield);
    if (!res) {
      ldout(cct, 0) << "ERROR: notify_obj.open() returned r="
		    << res.error().message() << dendl;
      return res.error();
    }

    notify_objs.emplace_back(std::move(*res));
    auto& notify_obj = notify_objs[i];
    neorados::WriteOp op;
    op.create(false);
    bs::error_code ec;
    notify_obj.operate(std::move(op), y[ec]);
    if (ec && ec != bs::errc::file_exists) {
      ldout(cct, 0) << "ERROR: notify_obj.operate() returned ec="
		    << ec.message() << dendl;
      return ec;
    }

    RGWWatcher *watcher = new RGWWatcher(cct, this, i, notify_obj);
    watchers[i] = watcher;

    watcher->register_watch_async();
  }

  for (int i = 0; i < num_watchers; ++i) {
    auto r = watchers[i]->register_watch_finish();
    if (r) {
      ldout(cct, 0) << "WARNING: async watch returned " << r.message() << dendl;
      error = r;
    }
  }

  return error;
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
  auto r = zone_svc->start(y, dpp);
  if (r < 0) {
    return r;
  }

  assert(zone_svc->is_started()); /* otherwise there's an ordering problem */

  r = finisher_svc->start(y, dpp);
  if (r < 0) {
    return r;
  }

  control_pool = zone_svc->get_zone_params().control_pool;

  auto ec = init_watch(y);
  if (ec) {
    lderr(cct) << "ERROR: failed to initialize watch: " << ec << dendl;
    return ceph::from_error_code(ec);
  }

  finisher_handle = finisher_svc->register_caller([this] { shutdown(); });

  return {};
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

  finalized = true;
}

RGWSI_Notify::~RGWSI_Notify()
{
  shutdown();
}

bs::error_code RGWSI_Notify::unwatch(neo_obj_ref& obj,
				     uint64_t watch_handle)
{
  bs::error_code ec;
  obj.unwatch(watch_handle, null_yield[ec]);
  if (ec)
    ldout(cct, 0) << "ERROR: rados.unwatch() returned ec=" << ec.message() << dendl;
  else
    rados->neorados().flush_watch(null_yield);
  return ec;
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

int RGWSI_Notify::watch_cb(uint64_t notify_id,
                           uint64_t cookie,
                           uint64_t notifier_id,
                           cb::list& bl)
{
  std::shared_lock l{watchers_lock};
  if (cb) {
    return cb->watch_cb(notify_id, cookie, notifier_id, bl);
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

bs::error_code RGWSI_Notify::distribute(const string& key,
                                                   cb::list& bl,
                                                   optional_yield y)
{
  /* The RGW uses the control pool to store the watch notify objects.
    The precedence in RGWSI_Notify::do_start is to call to zone_svc->start and later to init_watch().
    The first time, RGW starts in the cluster, the RGW will try to create zone and zonegroup system object.
    In that case RGW will try to distribute the cache before it ran init_watch,
    which will lead to division by 0 in pick_obj_control (num_watchers is 0).
  */
  if (num_watchers > 0) {
    auto& notify_obj = pick_control_obj(key);

    ldout(cct, 10) << "distributing notification oid=" << notify_obj.oid
		   << " bl.length()=" << bl.length() << dendl;
    return robust_notify(notify_obj, bl, y);
  }
  return {};
}

bs::error_code
RGWSI_Notify::robust_notify(neo_obj_ref& notify_obj, cb::list& bl,
                            optional_yield y)
{
  // The reply of every machine that acks goes in here.
  boost::container::flat_set<std::pair<uint64_t, uint64_t>> acks;
  // First, try to send, without being fancy about it.
  bs::error_code ec;
  cb::list rbl = notify_obj.notify(cb::list(bl), nullopt, y[ec]);

  // If that doesn't work, get serious.
  if (ec) {
    ldout(cct, 1) << "robust_notify: If at first you don't succeed: "
                  << ec.message() << dendl;

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
	p += blen;
      }
    } catch (const ceph::buffer::error& e) {
      ldout(cct, 0) << "robust_notify: notify response parse failed: "
		    << e.what() << dendl;
      acks.clear(); // Throw away junk on failed parse.
    }


    // Every machine that fails to reply and hasn't acked a previous
    // attempt goes in here.
    boost::container::flat_set<std::pair<uint64_t, uint64_t>> timeouts;

    auto tries = 1u;
    while (!ec && tries < max_notify_retries) {
      ++tries;
      rbl.clear();
      // Reset the timeouts, we're only concerned with new ones.
      timeouts.clear();
      rbl = notify_obj.notify(cb::list(bl), nullopt, y[ec]);
      if (ec) {
	ldout(cct, 1) << "robust_notify: retry " << tries << " failed: "
		      << ec.message() << dendl;
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
	    p += blen;
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
	} catch (const ceph::buffer::error& e) {
	  ldout(cct, 0) << "robust_notify: notify response parse failed: "
			<< e.what() << dendl;
	  continue;
	}
	// If we got a good parse and timeouts is empty, that means
	// everyone who timed out in one call received the update in a
	// previous one.
	if (timeouts.empty()) {
	  ec.clear();
	}
      }
    }
  }
  return ec;
}

void RGWSI_Notify::register_watch_cb(CB *_cb)
{
  std::unique_lock l{watchers_lock};
  cb = _cb;
  _set_enabled(enabled);
}
