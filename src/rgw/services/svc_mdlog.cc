// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_mdlog.h"
#include "svc_rados.h"
#include "svc_zone.h"
#include "svc_sys_obj.h"

#include "rgw/rgw_tools.h"
#include "rgw/rgw_mdlog.h"
#include "rgw/rgw_coroutine.h"
#include "rgw/rgw_cr_rados.h"
#include "rgw/rgw_zone.h"

#include "common/errno.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

using Svc = RGWSI_MDLog::Svc;
using Cursor = RGWPeriodHistory::Cursor;

RGWSI_MDLog::RGWSI_MDLog(CephContext *cct, bool _run_sync) : RGWServiceInstance(cct), run_sync(_run_sync) {
}

RGWSI_MDLog::~RGWSI_MDLog() {
}

int RGWSI_MDLog::init(RGWSI_RADOS *_rados_svc, RGWSI_Zone *_zone_svc, RGWSI_SysObj *_sysobj_svc, RGWSI_Cls *_cls_svc)
{
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
  svc.mdlog = this;
  svc.rados = _rados_svc;
  svc.cls = _cls_svc;

  return 0;
}

int RGWSI_MDLog::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{
  auto& current_period = svc.zone->get_current_period();

  current_log = get_log(current_period.get_id());

  period_puller.reset(new RGWPeriodPuller(svc.zone, svc.sysobj));
  period_history.reset(new RGWPeriodHistory(cct, period_puller.get(),
                                            current_period));

  if (run_sync &&
      svc.zone->need_to_sync()) {
    // initialize the log period history
    svc.mdlog->init_oldest_log_period(y, dpp);
  }
  return 0;
}

int RGWSI_MDLog::read_history(RGWMetadataLogHistory *state,
                              RGWObjVersionTracker *objv_tracker,
			      optional_yield y,
                              const DoutPrefixProvider *dpp) const
{
  auto obj_ctx = svc.sysobj->init_obj_ctx();
  auto& pool = svc.zone->get_zone_params().log_pool;
  const auto& oid = RGWMetadataLogHistory::oid;
  bufferlist bl;
  int ret = rgw_get_system_obj(obj_ctx, pool, oid, bl, objv_tracker, nullptr, y, dpp);
  if (ret < 0) {
    return ret;
  }
  if (bl.length() == 0) {
    /* bad history object, remove it */
    rgw_raw_obj obj(pool, oid);
    auto sysobj = obj_ctx.get_obj(obj);
    ret = sysobj.wop().remove(y);
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
                               optional_yield y, bool exclusive)
{
  bufferlist bl;
  state.encode(bl);

  auto& pool = svc.zone->get_zone_params().log_pool;
  const auto& oid = RGWMetadataLogHistory::oid;
  auto obj_ctx = svc.sysobj->init_obj_ctx();
  return rgw_put_system_obj(obj_ctx, pool, oid, bl,
                            exclusive, objv_tracker, real_time{}, y);
}

namespace mdlog {

using Cursor = RGWPeriodHistory::Cursor;

/// read the mdlog history and use it to initialize the given cursor
class ReadHistoryCR : public RGWCoroutine {
  Svc svc;
  Cursor *cursor;
  RGWObjVersionTracker *objv_tracker;
  RGWMetadataLogHistory state;
  RGWAsyncRadosProcessor *async_processor;

 public:
  ReadHistoryCR(const Svc& svc,
                Cursor *cursor,
                RGWObjVersionTracker *objv_tracker)
    : RGWCoroutine(svc.zone->ctx()), svc(svc),
      cursor(cursor),
      objv_tracker(objv_tracker),
      async_processor(svc.rados->get_async_processor())
  {}

  int operate() {
    reenter(this) {
      yield {
        rgw_raw_obj obj{svc.zone->get_zone_params().log_pool,
                        RGWMetadataLogHistory::oid};
        constexpr bool empty_on_enoent = false;

        using ReadCR = RGWSimpleRadosReadCR<RGWMetadataLogHistory>;
        call(new ReadCR(async_processor, svc.sysobj, obj,
                        &state, empty_on_enoent, objv_tracker));
      }
      if (retcode < 0) {
        ldout(cct, 1) << "failed to read mdlog history: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
      *cursor = svc.mdlog->period_history->lookup(state.oldest_realm_epoch);
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
  Svc svc;
  Cursor cursor;
  RGWObjVersionTracker *objv;
  RGWMetadataLogHistory state;
  RGWAsyncRadosProcessor *async_processor;

 public:
  WriteHistoryCR(Svc& svc,
                 const Cursor& cursor,
                 RGWObjVersionTracker *objv)
    : RGWCoroutine(svc.zone->ctx()), svc(svc),
      cursor(cursor), objv(objv),
      async_processor(svc.rados->get_async_processor())
  {}

  int operate() {
    reenter(this) {
      state.oldest_period_id = cursor.get_period().get_id();
      state.oldest_realm_epoch = cursor.get_epoch();

      yield {
        rgw_raw_obj obj{svc.zone->get_zone_params().log_pool,
                        RGWMetadataLogHistory::oid};

        using WriteCR = RGWSimpleRadosWriteCR<RGWMetadataLogHistory>;
        call(new WriteCR(async_processor, svc.sysobj, obj, state, objv));
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
  Svc svc;
  const Cursor cursor; //< cursor to trimmed period
  RGWObjVersionTracker *objv; //< to prevent racing updates
  Cursor next; //< target cursor for oldest log period
  Cursor existing; //< existing cursor read from disk

 public:
  TrimHistoryCR(const Svc& svc, Cursor cursor, RGWObjVersionTracker *objv)
    : RGWCoroutine(svc.zone->ctx()), svc(svc),
      cursor(cursor), objv(objv), next(cursor) {
    next.next(); // advance past cursor
  }

  int operate() {
    reenter(this) {
      // read an existing history, and write the new history if it's newer
      yield call(new ReadHistoryCR(svc, &existing, objv));
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
      yield call(new WriteHistoryCR(svc, next, objv));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

} // mdlog namespace

// traverse all the way back to the beginning of the period history, and
// return a cursor to the first period in a fully attached history
Cursor RGWSI_MDLog::find_oldest_period(optional_yield y)
{
  auto cursor = period_history->get_current();

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
      int r = period_puller->pull(predecessor, period, y);
      if (r < 0) {
        return cursor;
      }
      auto prev = period_history->insert(std::move(period));
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

Cursor RGWSI_MDLog::init_oldest_log_period(optional_yield y, const DoutPrefixProvider *dpp)
{
  // read the mdlog history
  RGWMetadataLogHistory state;
  RGWObjVersionTracker objv;
  int ret = read_history(&state, &objv, y, dpp);

  if (ret == -ENOENT) {
    // initialize the mdlog history and write it
    ldpp_dout(dpp, 10) << "initializing mdlog history" << dendl;
    auto cursor = find_oldest_period(y);
    if (!cursor) {
      return cursor;
    }
    // write the initial history
    state.oldest_realm_epoch = cursor.get_epoch();
    state.oldest_period_id = cursor.get_period().get_id();

    constexpr bool exclusive = true; // don't overwrite
    int ret = write_history(state, &objv, y, exclusive);
    if (ret < 0 && ret != -EEXIST) {
      ldpp_dout(dpp, 1) << "failed to write mdlog history: "
          << cpp_strerror(ret) << dendl;
      return Cursor{ret};
    }
    return cursor;
  } else if (ret < 0) {
    ldpp_dout(dpp, 1) << "failed to read mdlog history: "
        << cpp_strerror(ret) << dendl;
    return Cursor{ret};
  }

  // if it's already in the history, return it
  auto cursor = period_history->lookup(state.oldest_realm_epoch);
  if (cursor) {
    return cursor;
  } else {
    cursor = find_oldest_period(y);
    state.oldest_realm_epoch = cursor.get_epoch();
    state.oldest_period_id = cursor.get_period().get_id();
    ldout(cct, 10) << "rewriting mdlog history" << dendl;
    ret = write_history(state, &objv, y);
    if (ret < 0 && ret != -ECANCELED) {
    ldout(cct, 1) << "failed to write mdlog history: "
          << cpp_strerror(ret) << dendl;
    return Cursor{ret};
    }
    return cursor;
  }

  // pull the oldest period by id
  RGWPeriod period;
  ret = period_puller->pull(state.oldest_period_id, period, y);
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
  return period_history->attach(std::move(period), y);
}

Cursor RGWSI_MDLog::read_oldest_log_period(optional_yield y, const DoutPrefixProvider *dpp) const
{
  RGWMetadataLogHistory state;
  int ret = read_history(&state, nullptr, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "failed to read mdlog history: "
        << cpp_strerror(ret) << dendl;
    return Cursor{ret};
  }

  ldpp_dout(dpp, 10) << "read mdlog history with oldest period id="
      << state.oldest_period_id << " realm_epoch="
      << state.oldest_realm_epoch << dendl;

  return period_history->lookup(state.oldest_realm_epoch);
}

RGWCoroutine* RGWSI_MDLog::read_oldest_log_period_cr(Cursor *period,
        RGWObjVersionTracker *objv) const
{
  return new mdlog::ReadHistoryCR(svc, period, objv);
}

RGWCoroutine* RGWSI_MDLog::trim_log_period_cr(Cursor period,
        RGWObjVersionTracker *objv) const
{
  return new mdlog::TrimHistoryCR(svc, period, objv);
}

RGWMetadataLog* RGWSI_MDLog::get_log(const std::string& period)
{
  // construct the period's log in place if it doesn't exist
  auto insert = md_logs.emplace(std::piecewise_construct,
                                std::forward_as_tuple(period),
                                std::forward_as_tuple(cct, svc.zone, svc.cls, period));
  return &insert.first->second;
}

int RGWSI_MDLog::add_entry(const string& hash_key, const string& section, const string& key, bufferlist& bl)
{
  ceph_assert(current_log); // must have called init()
  return current_log->add_entry(hash_key, section, key, bl);
}

int RGWSI_MDLog::get_shard_id(const string& hash_key, int *shard_id)
{
  ceph_assert(current_log); // must have called init()
  return current_log->get_shard_id(hash_key, shard_id);
}

int RGWSI_MDLog::pull_period(const std::string& period_id, RGWPeriod& period,
			     optional_yield y)
{
  return period_puller->pull(period_id, period, y);
}

