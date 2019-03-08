#include "svc_mdlog.h"
#include "svc_zone.h"

#include "rgw/rgw_tools.h"
#include "rgw/rgw_mdlog.h"
#include "rgw/rgw_coroutine.h"

int RGWSI_MDLog::read_history(RGWRados *store, RGWMetadataLogHistory *state,
                              RGWObjVersionTracker *objv_tracker)
{
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto& pool = zone_svc->get_zone_params().log_pool;
  const auto& oid = RGWMetadataLogHistory::oid;
  bufferlist bl;
  int ret = rgw_get_system_obj(obj_ctx, pool, oid, bl, objv_tracker, nullptr);
  if (ret < 0) {
    return ret;
  }
  if (bl.length() == 0) {
    /* bad history object, remove it */
    rgw_raw_obj obj(pool, oid);
    auto sysobj = obj_ctx.get_obj(obj);
    ret = sysobj.wop().remove();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: meta history is empty, but cannot remove it (" << cpp_strerror(-ret) << ")" << dendl;
      return ret;
    }
    return -ENOENT;
  }
  try {
    auto p = bl.cbegin();
    state->decode(p);
  } catch (buffer::error& e) {
    ldout(cct, 1) << "failed to decode the mdlog history: "
        << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int RGWSI_MDLog::write_history(const RGWMetadataLogHistory& state,
                               RGWObjVersionTracker *objv_tracker,
                               bool exclusive)
{
  bufferlist bl;
  state.encode(bl);

  auto& pool = zone_svc->get_zone_params().log_pool;
  const auto& oid = RGWMetadataLogHistory::oid;
  return rgw_put_system_obj(sysobj_svc, pool, oid, bl,
                            exclusive, objv_tracker, real_time{});
}

namespace {

using Cursor = RGWPeriodHistory::Cursor;

/// read the mdlog history and use it to initialize the given cursor
class ReadHistoryCR : public RGWCoroutine {
  RGWSI_Zone *zone_svc;
  RGWSI_SysObj *sysobj_svc;
  Cursor *cursor;
  RGWObjVersionTracker *objv_tracker;
  RGWMetadataLogHistory state;
 public:
  ReadHistoryCR(RGWSI_Zone *zone_svc,
                RGWSI_SysObj *sysobj_svc,
                Cursor *cursor,
                RGWObjVersionTracker *objv_tracker)
    : RGWCoroutine(zone_svc->ctx()), zone_svc(zone_svc),
    sysobj_svc(sysobj_svc),
    cursor(cursor),
      objv_tracker(objv_tracker)
  {}

  int operate() {
    reenter(this) {
      yield {
        rgw_raw_obj obj{zone_svc->get_zone_params().log_pool,
                        RGWMetadataLogHistory::oid};
        constexpr bool empty_on_enoent = false;

        using ReadCR = RGWSimpleRadosReadCR<RGWMetadataLogHistory>;
        call(new ReadCR(store->get_async_rados(), sysobj_svc, obj,
                        &state, empty_on_enoent, objv_tracker));
      }
      if (retcode < 0) {
        ldout(cct, 1) << "failed to read mdlog history: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
      *cursor = store->period_history->lookup(state.oldest_realm_epoch);
      if (!*cursor) {
        return set_cr_error(cursor->get_error());
      }

      ldout(cct, 10) << "read mdlog history with oldest period id="
          << state.oldest_period_id << " realm_epoch="
          << state.oldest_realm_epoch << dendl;
      return set_cr_done();
    }
    return 0;
  }
};

/// write the given cursor to the mdlog history
class WriteHistoryCR : public RGWCoroutine {
  RGWSI_Zone *zone_svc;
  RGWSI_SysObj *sysobj_svc;
  Cursor cursor;
  RGWObjVersionTracker *objv;
  RGWMetadataLogHistory state;
 public:
  WriteHistoryCR(RGWSI_Zone *zone_svc, RGWSI_SysObj *sysobj_svc,
                 const Cursor& cursor,
                 RGWObjVersionTracker *objv)
    : RGWCoroutine(zone_svc->ctx()), zone_svc(zone_svc), cursor(cursor), objv(objv)
  {}

  int operate() {
    reenter(this) {
      state.oldest_period_id = cursor.get_period().get_id();
      state.oldest_realm_epoch = cursor.get_epoch();

      yield {
        rgw_raw_obj obj{zone_svc->get_zone_params().log_pool,
                        RGWMetadataLogHistory::oid};

        using WriteCR = RGWSimpleRadosWriteCR<RGWMetadataLogHistory>;
        call(new WriteCR(store->get_async_rados(), sysobj_svc, obj, state, objv));
      }
      if (retcode < 0) {
        ldout(cct, 1) << "failed to write mdlog history: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }

      ldout(cct, 10) << "wrote mdlog history with oldest period id="
          << state.oldest_period_id << " realm_epoch="
          << state.oldest_realm_epoch << dendl;
      return set_cr_done();
    }
    return 0;
  }
};

/// update the mdlog history to reflect trimmed logs
class TrimHistoryCR : public RGWCoroutine {
  RGWRados *store;
  const Cursor cursor; //< cursor to trimmed period
  RGWObjVersionTracker *objv; //< to prevent racing updates
  Cursor next; //< target cursor for oldest log period
  Cursor existing; //< existing cursor read from disk

 public:
  TrimHistoryCR(RGWRados *store, Cursor cursor, RGWObjVersionTracker *objv)
    : RGWCoroutine(store->ctx()),
      store(store), cursor(cursor), objv(objv), next(cursor)
  {
    next.next(); // advance past cursor
  }

  int operate() {
    reenter(this) {
      // read an existing history, and write the new history if it's newer
      yield call(new ReadHistoryCR(store, &existing, objv));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      // reject older trims with ECANCELED
      if (cursor.get_epoch() < existing.get_epoch()) {
        ldout(cct, 4) << "found oldest log epoch=" << existing.get_epoch()
            << ", rejecting trim at epoch=" << cursor.get_epoch() << dendl;
        return set_cr_error(-ECANCELED);
      }
      // overwrite with updated history
      yield call(new WriteHistoryCR(store, next, objv));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

// traverse all the way back to the beginning of the period history, and
// return a cursor to the first period in a fully attached history
Cursor find_oldest_period(RGWRados *store)
{
  auto cct = store->ctx();
  auto cursor = store->period_history->get_current();

  while (cursor) {
    // advance to the period's predecessor
    if (!cursor.has_prev()) {
      auto& predecessor = cursor.get_period().get_predecessor();
      if (predecessor.empty()) {
        // this is the first period, so our logs must start here
        ldout(cct, 10) << "find_oldest_period returning first "
            "period " << cursor.get_period().get_id() << dendl;
        return cursor;
      }
      // pull the predecessor and add it to our history
      RGWPeriod period;
      int r = store->period_puller->pull(predecessor, period);
      if (r < 0) {
        return Cursor{r};
      }
      auto prev = store->period_history->insert(std::move(period));
      if (!prev) {
        return prev;
      }
      ldout(cct, 20) << "find_oldest_period advancing to "
          "predecessor period " << predecessor << dendl;
      ceph_assert(cursor.has_prev());
    }
    cursor.prev();
  }
  ldout(cct, 10) << "find_oldest_period returning empty cursor" << dendl;
  return cursor;
}

} // anonymous namespace

Cursor RGWMetadataManager::init_oldest_log_period()
{
  // read the mdlog history
  RGWMetadataLogHistory state;
  RGWObjVersionTracker objv;
  int ret = read_history(store, &state, &objv);

  if (ret == -ENOENT) {
    // initialize the mdlog history and write it
    ldout(cct, 10) << "initializing mdlog history" << dendl;
    auto cursor = find_oldest_period(store);
    if (!cursor) {
      return cursor;
    }

    // write the initial history
    state.oldest_realm_epoch = cursor.get_epoch();
    state.oldest_period_id = cursor.get_period().get_id();

    constexpr bool exclusive = true; // don't overwrite
    int ret = write_history(store, state, &objv, exclusive);
    if (ret < 0 && ret != -EEXIST) {
      ldout(cct, 1) << "failed to write mdlog history: "
          << cpp_strerror(ret) << dendl;
      return Cursor{ret};
    }
    return cursor;
  } else if (ret < 0) {
    ldout(cct, 1) << "failed to read mdlog history: "
        << cpp_strerror(ret) << dendl;
    return Cursor{ret};
  }

  // if it's already in the history, return it
  auto cursor = store->period_history->lookup(state.oldest_realm_epoch);
  if (cursor) {
    return cursor;
  }
  // pull the oldest period by id
  RGWPeriod period;
  ret = store->period_puller->pull(state.oldest_period_id, period);
  if (ret < 0) {
    ldout(cct, 1) << "failed to read period id=" << state.oldest_period_id
        << " for mdlog history: " << cpp_strerror(ret) << dendl;
    return Cursor{ret};
  }
  // verify its realm_epoch
  if (period.get_realm_epoch() != state.oldest_realm_epoch) {
    ldout(cct, 1) << "inconsistent mdlog history: read period id="
        << period.get_id() << " with realm_epoch=" << period.get_realm_epoch()
        << ", expected realm_epoch=" << state.oldest_realm_epoch << dendl;
    return Cursor{-EINVAL};
  }
  // attach the period to our history
  return store->period_history->attach(std::move(period));
}

Cursor RGWMetadataManager::read_oldest_log_period() const
{
  RGWMetadataLogHistory state;
  int ret = read_history(store, &state, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "failed to read mdlog history: "
        << cpp_strerror(ret) << dendl;
    return Cursor{ret};
  }

  ldout(store->ctx(), 10) << "read mdlog history with oldest period id="
      << state.oldest_period_id << " realm_epoch="
      << state.oldest_realm_epoch << dendl;

  return store->period_history->lookup(state.oldest_realm_epoch);
}

RGWCoroutine* RGWMetadataManager::read_oldest_log_period_cr(Cursor *period,
        RGWObjVersionTracker *objv) const
{
  return new ReadHistoryCR(store, period, objv);
}

RGWCoroutine* RGWMetadataManager::trim_log_period_cr(Cursor period,
        RGWObjVersionTracker *objv) const
{
  return new TrimHistoryCR(store, period, objv);
}

RGWMetadataLog* RGWMetadataManager::get_log(const std::string& period)
{
  // construct the period's log in place if it doesn't exist
  auto insert = md_logs.emplace(std::piecewise_construct,
                                std::forward_as_tuple(period),
                                std::forward_as_tuple(cct, store, period));
  return &insert.first->second;
}

int init(RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc,
         const std::string& current_period)
{
  zone_svc = _zone_svc;
  sysobj_svc = _sysobj_svc;
  current_log = get_log(current_period);

  period_puller.reset(new RGWPeriodPuller(this));
  period_history.reset(new RGWPeriodHistory(cct, period_puller.get(),
                                            zone_svc->get_current_period()));

  return 0;
}
}

int RGWSI_MDLog::add_entry(RGWSI_MetaBacked::Module *module, const string& section, const string& key, bufferlist& bl)
{
  ceph_assert(current_log); // must have called init()
  return current_log->add_entry(module, section, key, logbl);
}
