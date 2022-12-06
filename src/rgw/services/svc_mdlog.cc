// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_mdlog.h"
#include "svc_zone.h"
#include "svc_sys_obj.h"

#include "rgw_tools.h"
#include "rgw_mdlog.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"

#include "driver/rados/rgw_zone.h" // FIXME: subclass dependency

#include "common/errno.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

using namespace std;

using Svc = RGWSI_MDLog::Svc;
using Cursor = RGWPeriodHistory::Cursor;

RGWSI_MDLog::RGWSI_MDLog(CephContext *cct, bool _run_sync) : RGWServiceInstance(cct), run_sync(_run_sync) {
}

RGWSI_MDLog::~RGWSI_MDLog() {
}

int RGWSI_MDLog::init(librados::Rados* rados_, RGWSI_Zone *_zone_svc,
		      RGWSI_SysObj *_sysobj_svc, RGWSI_Cls *_cls_svc,
		      RGWAsyncRadosProcessor* async_processor_)
{
  svc.zone = _zone_svc;
  svc.sysobj = _sysobj_svc;
  svc.mdlog = this;
  rados = rados_;
  svc.cls = _cls_svc;
  async_processor = async_processor_;

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
  auto& pool = svc.zone->get_zone_params().log_pool;
  const auto& oid = RGWMetadataLogHistory::oid;
  bufferlist bl;
  int ret = rgw_get_system_obj(svc.sysobj, pool, oid, bl, objv_tracker, nullptr, y, dpp);
  if (ret < 0) {
    return ret;
  }
  if (bl.length() == 0) {
    /* bad history object, remove it */
    rgw_raw_obj obj(pool, oid);
    auto sysobj = svc.sysobj->get_obj(obj);
    ret = sysobj.wop().remove(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: meta history is empty, but cannot remove it (" << cpp_strerror(-ret) << ")" << dendl;
      return ret;
    }
    return -ENOENT;
  }
  try {
    auto p = bl.cbegin();
    state->decode(p);
  } catch (buffer::error& e) {
    ldpp_dout(dpp, 1) << "failed to decode the mdlog history: "
        << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int RGWSI_MDLog::write_history(const DoutPrefixProvider *dpp, 
                               const RGWMetadataLogHistory& state,
                               RGWObjVersionTracker *objv_tracker,
                               optional_yield y, bool exclusive)
{
  bufferlist bl;
  state.encode(bl);

  auto& pool = svc.zone->get_zone_params().log_pool;
  const auto& oid = RGWMetadataLogHistory::oid;
  return rgw_put_system_obj(dpp, svc.sysobj, pool, oid, bl,
                            exclusive, objv_tracker, real_time{}, y);
}

namespace mdlog {

using Cursor = RGWPeriodHistory::Cursor;

namespace {
template <class T>
class SysObjReadCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  RGWAsyncRadosProcessor *async_rados;
  RGWSI_SysObj *svc;

  rgw_raw_obj obj;
  T *result;
  /// on ENOENT, call handle_data() with an empty object instead of failing
  const bool empty_on_enoent;
  RGWObjVersionTracker *objv_tracker;
  RGWAsyncGetSystemObj *req{nullptr};

public:
  SysObjReadCR(const DoutPrefixProvider *_dpp, 
	       RGWAsyncRadosProcessor *_async_rados, RGWSI_SysObj *_svc,
	       const rgw_raw_obj& _obj,
	       T *_result, bool empty_on_enoent = true,
	       RGWObjVersionTracker *objv_tracker = nullptr)
    : RGWSimpleCoroutine(_svc->ctx()), dpp(_dpp), async_rados(_async_rados), svc(_svc),
      obj(_obj), result(_result),
      empty_on_enoent(empty_on_enoent), objv_tracker(objv_tracker) {}

  ~SysObjReadCR() override {
    try {
      request_cleanup();
    } catch (const boost::container::length_error_t& e) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	": reference counted object mismatched, \"" << e.what() <<
	"\"" << dendl;
    }
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) {
    req = new RGWAsyncGetSystemObj(dpp, this, stack->create_completion_notifier(), svc,
				   objv_tracker, obj, false, false);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    int ret = req->get_ret_status();
    retcode = ret;
    if (ret == -ENOENT && empty_on_enoent) {
      *result = T();
    } else {
      if (ret < 0) {
	return ret;
      }
      if (objv_tracker) { // copy the updated version
	*objv_tracker = req->objv_tracker;
      }
      try {
	auto iter = req->bl.cbegin();
	if (iter.end()) {
	  // allow successful reads with empty buffers. ReadSyncStatus
	  // coroutines depend on this to be able to read without
	  // locking, because the cls lock from InitSyncStatus will
	  // create an empty object if it didn't exist
	  *result = T();
	} else {
	  decode(*result, iter);
	}
      } catch (buffer::error& err) {
	return -EIO;
      }
    }
    return handle_data(*result);
  }

  virtual int handle_data(T& data) {
    return 0;
  }
};

template <class T>
class SysObjWriteCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  RGWAsyncRadosProcessor *async_rados;
  RGWSI_SysObj *svc;
  bufferlist bl;
  rgw_raw_obj obj;
  RGWObjVersionTracker *objv_tracker;
  bool exclusive;
  RGWAsyncPutSystemObj *req{nullptr};

public:
  SysObjWriteCR(const DoutPrefixProvider *_dpp, 
		RGWAsyncRadosProcessor *_async_rados, RGWSI_SysObj *_svc,
		const rgw_raw_obj& _obj, const T& _data,
		RGWObjVersionTracker *objv_tracker = nullptr,
		bool exclusive = false)
    : RGWSimpleCoroutine(_svc->ctx()), dpp(_dpp), async_rados(_async_rados),
      svc(_svc), obj(_obj), objv_tracker(objv_tracker), exclusive(exclusive) {
    encode(_data, bl);
  }

  ~SysObjWriteCR() override {
    try {
      request_cleanup();
    } catch (const boost::container::length_error_t& e) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ <<
	": reference counted object mismatched, \"" << e.what() <<
	"\"" << dendl;
    }
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncPutSystemObj(dpp, this, stack->create_completion_notifier(),
			           svc, objv_tracker, obj, exclusive, std::move(bl));
    async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    if (objv_tracker) { // copy the updated version
      *objv_tracker = req->objv_tracker;
    }
    return req->get_ret_status();
  }
};
}

/// read the mdlog history and use it to initialize the given cursor
class ReadHistoryCR : public RGWCoroutine {
  const DoutPrefixProvider *dpp;
  Svc svc;
  Cursor *cursor;
  RGWObjVersionTracker *objv_tracker;
  RGWMetadataLogHistory state;
  RGWAsyncRadosProcessor *async_processor;

 public:
  ReadHistoryCR(const DoutPrefixProvider *dpp, 
                const Svc& svc,
                Cursor *cursor,
                RGWObjVersionTracker *objv_tracker,
		RGWAsyncRadosProcessor* async_processor)
    : RGWCoroutine(svc.zone->ctx()), dpp(dpp), svc(svc),
      cursor(cursor),
      objv_tracker(objv_tracker),
      async_processor(async_processor)
  {}

  int operate(const DoutPrefixProvider *dpp) {
    reenter(this) {
      yield {
        rgw_raw_obj obj{svc.zone->get_zone_params().log_pool,
                        RGWMetadataLogHistory::oid};
        constexpr bool empty_on_enoent = false;

        using ReadCR = SysObjReadCR<RGWMetadataLogHistory>;
        call(new ReadCR(dpp, async_processor, svc.sysobj, obj,
                        &state, empty_on_enoent, objv_tracker));
      }
      if (retcode < 0) {
        ldpp_dout(dpp, 1) << "failed to read mdlog history: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
      *cursor = svc.mdlog->period_history->lookup(state.oldest_realm_epoch);
      if (!*cursor) {
        return set_cr_error(cursor->get_error());
      }

      ldpp_dout(dpp, 10) << "read mdlog history with oldest period id="
          << state.oldest_period_id << " realm_epoch="
          << state.oldest_realm_epoch << dendl;
      return set_cr_done();
    }
    return 0;
  }
};

/// write the given cursor to the mdlog history
class WriteHistoryCR : public RGWCoroutine {
  const DoutPrefixProvider *dpp;
  Svc svc;
  Cursor cursor;
  RGWObjVersionTracker *objv;
  RGWMetadataLogHistory state;
  RGWAsyncRadosProcessor *async_processor;

 public:
  WriteHistoryCR(const DoutPrefixProvider *dpp, 
                 Svc& svc,
                 const Cursor& cursor,
                 RGWObjVersionTracker *objv,
		 RGWAsyncRadosProcessor* async_processor)
    : RGWCoroutine(svc.zone->ctx()), dpp(dpp), svc(svc),
      cursor(cursor), objv(objv),
      async_processor(async_processor)
  {}

  int operate(const DoutPrefixProvider *dpp) {
    reenter(this) {
      state.oldest_period_id = cursor.get_period().get_id();
      state.oldest_realm_epoch = cursor.get_epoch();

      yield {
        rgw_raw_obj obj{svc.zone->get_zone_params().log_pool,
                        RGWMetadataLogHistory::oid};

        using WriteCR = SysObjWriteCR<RGWMetadataLogHistory>;
        call(new WriteCR(dpp, async_processor, svc.sysobj, obj, state, objv));
      }
      if (retcode < 0) {
        ldpp_dout(dpp, 1) << "failed to write mdlog history: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }

      ldpp_dout(dpp, 10) << "wrote mdlog history with oldest period id="
          << state.oldest_period_id << " realm_epoch="
          << state.oldest_realm_epoch << dendl;
      return set_cr_done();
    }
    return 0;
  }
};

/// update the mdlog history to reflect trimmed logs
class TrimHistoryCR : public RGWCoroutine {
  const DoutPrefixProvider *dpp;
  Svc svc;
  const Cursor cursor; //< cursor to trimmed period
  RGWObjVersionTracker *objv; //< to prevent racing updates
  Cursor next; //< target cursor for oldest log period
  Cursor existing; //< existing cursor read from disk
  RGWAsyncRadosProcessor* async_processor;

 public:
  TrimHistoryCR(const DoutPrefixProvider *dpp, const Svc& svc, Cursor cursor,
		RGWObjVersionTracker *objv,
		RGWAsyncRadosProcessor* async_processor)
    : RGWCoroutine(svc.zone->ctx()), dpp(dpp), svc(svc),
      cursor(cursor), objv(objv), next(cursor),
      async_processor(async_processor) {
    next.next(); // advance past cursor
  }

  int operate(const DoutPrefixProvider *dpp) {
    reenter(this) {
      // read an existing history, and write the new history if it's newer
      yield call(new ReadHistoryCR(dpp, svc, &existing, objv, async_processor));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      // reject older trims with ECANCELED
      if (cursor.get_epoch() < existing.get_epoch()) {
        ldpp_dout(dpp, 4) << "found oldest log epoch=" << existing.get_epoch()
            << ", rejecting trim at epoch=" << cursor.get_epoch() << dendl;
        return set_cr_error(-ECANCELED);
      }
      // overwrite with updated history
      yield call(new WriteHistoryCR(dpp, svc, next, objv, async_processor));
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
Cursor RGWSI_MDLog::find_oldest_period(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto cursor = period_history->get_current();

  while (cursor) {
    // advance to the period's predecessor
    if (!cursor.has_prev()) {
      auto& predecessor = cursor.get_period().get_predecessor();
      if (predecessor.empty()) {
        // this is the first period, so our logs must start here
        ldpp_dout(dpp, 10) << "find_oldest_period returning first "
            "period " << cursor.get_period().get_id() << dendl;
        return cursor;
      }
      // pull the predecessor and add it to our history
      RGWPeriod period;
      int r = period_puller->pull(dpp, predecessor, period, y);
      if (r < 0) {
        return cursor;
      }
      auto prev = period_history->insert(std::move(period));
      if (!prev) {
        return prev;
      }
      ldpp_dout(dpp, 20) << "find_oldest_period advancing to "
          "predecessor period " << predecessor << dendl;
      ceph_assert(cursor.has_prev());
    }
    cursor.prev();
  }
  ldpp_dout(dpp, 10) << "find_oldest_period returning empty cursor" << dendl;
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
    auto cursor = find_oldest_period(dpp, y);
    if (!cursor) {
      return cursor;
    }
    // write the initial history
    state.oldest_realm_epoch = cursor.get_epoch();
    state.oldest_period_id = cursor.get_period().get_id();

    constexpr bool exclusive = true; // don't overwrite
    int ret = write_history(dpp, state, &objv, y, exclusive);
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
    cursor = find_oldest_period(dpp, y);
    state.oldest_realm_epoch = cursor.get_epoch();
    state.oldest_period_id = cursor.get_period().get_id();
    ldpp_dout(dpp, 10) << "rewriting mdlog history" << dendl;
    ret = write_history(dpp, state, &objv, y);
    if (ret < 0 && ret != -ECANCELED) {
    ldpp_dout(dpp, 1) << "failed to write mdlog history: "
          << cpp_strerror(ret) << dendl;
    return Cursor{ret};
    }
    return cursor;
  }

  // pull the oldest period by id
  RGWPeriod period;
  ret = period_puller->pull(dpp, state.oldest_period_id, period, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "failed to read period id=" << state.oldest_period_id
        << " for mdlog history: " << cpp_strerror(ret) << dendl;
    return Cursor{ret};
  }
  // verify its realm_epoch
  if (period.get_realm_epoch() != state.oldest_realm_epoch) {
    ldpp_dout(dpp, 1) << "inconsistent mdlog history: read period id="
        << period.get_id() << " with realm_epoch=" << period.get_realm_epoch()
        << ", expected realm_epoch=" << state.oldest_realm_epoch << dendl;
    return Cursor{-EINVAL};
  }
  // attach the period to our history
  return period_history->attach(dpp, std::move(period), y);
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

RGWCoroutine* RGWSI_MDLog::read_oldest_log_period_cr(const DoutPrefixProvider *dpp, 
        Cursor *period, RGWObjVersionTracker *objv) const
{
  return new mdlog::ReadHistoryCR(dpp, svc, period, objv, async_processor);
}

RGWCoroutine* RGWSI_MDLog::trim_log_period_cr(const DoutPrefixProvider *dpp, 
        Cursor period, RGWObjVersionTracker *objv) const
{
  return new mdlog::TrimHistoryCR(dpp, svc, period, objv, async_processor);
}

RGWMetadataLog* RGWSI_MDLog::get_log(const std::string& period)
{
  // construct the period's log in place if it doesn't exist
  auto insert = md_logs.emplace(std::piecewise_construct,
                                std::forward_as_tuple(period),
                                std::forward_as_tuple(cct, svc.zone, svc.cls, period));
  return &insert.first->second;
}

int RGWSI_MDLog::add_entry(const DoutPrefixProvider *dpp, const string& hash_key, const string& section, const string& key, bufferlist& bl, optional_yield y)
{
  ceph_assert(current_log); // must have called init()
  return current_log->add_entry(dpp, hash_key, section, key, bl, y);
}

int RGWSI_MDLog::get_shard_id(const string& hash_key, int *shard_id)
{
  ceph_assert(current_log); // must have called init()
  return current_log->get_shard_id(hash_key, shard_id);
}

int RGWSI_MDLog::pull_period(const DoutPrefixProvider *dpp, const std::string& period_id, RGWPeriod& period,
			     optional_yield y)
{
  return period_puller->pull(dpp, period_id, period, y);
}

