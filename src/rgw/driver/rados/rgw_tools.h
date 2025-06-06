// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <coroutine>
#include <functional>
#include <string>
#include <string_view>

#include <boost/asio/deferred.hpp>
#include <boost/asio/async_result.hpp>

#include <boost/asio/experimental/co_composed.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/errc.hpp>

#include <fmt/format.h>

#include "include/types.h"
#include "include/ceph_hash.h"

#include "include/neorados/RADOS.hpp"

#include "common/ceph_time.h"
#include "common/dout.h"

#include "osd/osd_types.h"

#include "rgw_common.h"
#include "rgw_obj_types.h"
#include "rgw_pool_types.h"

class RGWSI_SysObj;

class RGWRados;
struct RGWObjVersionTracker;
class optional_yield;

struct obj_version;

int rgw_init_ioctx(const DoutPrefixProvider *dpp,
                   librados::Rados *rados, const rgw_pool& pool,
                   librados::IoCtx& ioctx,
                   bool create = false,
                   bool mostly_omap = false,
                   bool bulk = false);

#define RGW_NO_SHARD -1

#define RGW_SHARDS_PRIME_0 7877
#define RGW_SHARDS_PRIME_1 65521

extern const std::string MP_META_SUFFIX;

inline int rgw_shards_max()
{
  return RGW_SHARDS_PRIME_1;
}

// only called by rgw_shard_id and rgw_bucket_shard_index
static inline int rgw_shards_mod(unsigned hval, int max_shards)
{
  if (max_shards <= 0) {
    return -1;
  } else if (max_shards <= RGW_SHARDS_PRIME_0) {
    return hval % RGW_SHARDS_PRIME_0 % max_shards;
  } else {
    return hval % RGW_SHARDS_PRIME_1 % max_shards;
  }
}

// used for logging and tagging
inline int rgw_shard_id(const std::string& key, int max_shards)
{
  return rgw_shards_mod(ceph_str_hash_linux(key.c_str(), key.size()),
			max_shards);
}

void rgw_shard_name(const std::string& prefix, unsigned max_shards, const std::string& key, std::string& name, int *shard_id);
void rgw_shard_name(const std::string& prefix, unsigned max_shards, const std::string& section, const std::string& key, std::string& name);
void rgw_shard_name(const std::string& prefix, unsigned shard_id, std::string& name);

int rgw_put_system_obj(const DoutPrefixProvider *dpp, RGWSI_SysObj* svc_sysobj,
                       const rgw_pool& pool, const std::string& oid,
                       bufferlist& data, bool exclusive,
                       RGWObjVersionTracker *objv_tracker,
                       real_time set_mtime, optional_yield y,
                       const std::map<std::string, bufferlist> *pattrs = nullptr);
int rgw_get_system_obj(RGWSI_SysObj* svc_sysobj, const rgw_pool& pool,
                       const std::string& key, bufferlist& bl,
                       RGWObjVersionTracker *objv_tracker, real_time *pmtime,
                       optional_yield y, const DoutPrefixProvider *dpp,
                       std::map<std::string, bufferlist> *pattrs = nullptr,
                       rgw_cache_entry_info *cache_info = nullptr,
		       boost::optional<obj_version> refresh_version = boost::none,
                       bool raw_attrs=false);
int rgw_delete_system_obj(const DoutPrefixProvider *dpp,
                          RGWSI_SysObj *sysobj_svc, const rgw_pool& pool, const std::string& oid,
                          RGWObjVersionTracker *objv_tracker, optional_yield y);
int rgw_stat_system_obj(const DoutPrefixProvider *dpp, RGWSI_SysObj* svc_sysobj,
                        const rgw_pool& pool, const std::string& key,
                        RGWObjVersionTracker *objv_tracker,
                        real_time *pmtime, uint64_t *psize, optional_yield y,
                        std::map<std::string, bufferlist> *pattrs = nullptr);

const char *rgw_find_mime_by_ext(std::string& ext);

void rgw_filter_attrset(std::map<std::string, bufferlist>& unfiltered_attrset, const std::string& check_prefix,
                        std::map<std::string, bufferlist> *attrset);

/// perform the rados operation, using the yield context when given
int rgw_rados_operate(const DoutPrefixProvider *dpp, librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectReadOperation&& op, bufferlist* pbl,
                      optional_yield y, int flags = 0, const jspan_context* trace_info = nullptr,
                      version_t* pver = nullptr);
int rgw_rados_operate(const DoutPrefixProvider *dpp, librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectWriteOperation&& op, optional_yield y,
		      int flags = 0, const jspan_context* trace_info = nullptr,
                      version_t* pver = nullptr);
int rgw_rados_notify(const DoutPrefixProvider *dpp, librados::IoCtx& ioctx, const std::string& oid,
                     bufferlist& bl, uint64_t timeout_ms, bufferlist* pbl,
                     optional_yield y);

struct rgw_rados_ref {
  librados::IoCtx ioctx;
  rgw_raw_obj obj;


  int operate(const DoutPrefixProvider* dpp, librados::ObjectReadOperation&& op,
	      bufferlist* pbl, optional_yield y, int flags = 0) {
    return rgw_rados_operate(dpp, ioctx, obj.oid, std::move(op), pbl, y, flags);
  }

  int operate(const DoutPrefixProvider* dpp, librados::ObjectWriteOperation&& op,
	      optional_yield y, int flags = 0) {
    return rgw_rados_operate(dpp, ioctx, obj.oid, std::move(op), y, flags);
  }

  int aio_operate(librados::AioCompletion* c,
		  librados::ObjectWriteOperation* op) {
    return ioctx.aio_operate(obj.oid, c, op);
  }

  int aio_operate(librados::AioCompletion* c, librados::ObjectReadOperation* op,
		  bufferlist *pbl) {
    return ioctx.aio_operate(obj.oid, c, op, pbl);
  }

  int watch(const DoutPrefixProvider* dpp, uint64_t* handle,
            librados::WatchCtx2* ctx, optional_yield y);

  int unwatch(const DoutPrefixProvider* dpp, uint64_t handle, optional_yield y);

  int notify(const DoutPrefixProvider* dpp, bufferlist& bl, uint64_t timeout_ms,
	     bufferlist* pbl, optional_yield y) {
    return rgw_rados_notify(dpp, ioctx, obj.oid, bl, timeout_ms, pbl, y);
  }

  void notify_ack(uint64_t notify_id, uint64_t cookie, bufferlist& bl) {
    ioctx.notify_ack(obj.oid, notify_id, cookie, bl);
  }
};

inline std::ostream& operator <<(std::ostream& m, const rgw_rados_ref& ref) {
  return m << ref.obj;
}

int rgw_get_rados_ref(const DoutPrefixProvider* dpp, librados::Rados* rados,
		      rgw_raw_obj obj, rgw_rados_ref* ref);



int rgw_tools_init(const DoutPrefixProvider *dpp, CephContext *cct);
void rgw_tools_cleanup();

/// Complete an AioCompletion. To return error values or otherwise
/// satisfy the caller. Useful for making complicated asynchronous
/// calls and error handling.
void rgw_complete_aio_completion(librados::AioCompletion* c, int r);

/// This returns a static, non-NULL pointer, recognized only by
/// rgw_put_system_obj(). When supplied instead of the attributes, the
/// attributes will be unmodified.
///
// (Currently providing nullptr will wipe all attributes.)

std::map<std::string, ceph::buffer::list>* no_change_attrs();

bool rgw_check_secure_mon_conn(const DoutPrefixProvider *dpp);
int rgw_clog_warn(librados::Rados* h, const std::string& msg);

int rgw_list_pool(const DoutPrefixProvider *dpp,
		  librados::IoCtx& ioctx,
		  uint32_t max,
		  const rgw::AccessListFilter& filter,
		  std::string& marker,
		  std::vector<std::string> *oids,
		  bool *is_truncated);

// Asio's co_compse generates spurious warnings when compiled with
// -O0. the 'mismatched' `operator new` calls directly into the
// matching `operator new`, returning its result.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmismatched-new-delete"
namespace rgw {
struct mostly_omap_t {};
inline constexpr mostly_omap_t mostly_omap;

struct create_t {};
inline constexpr create_t create;

template<boost::asio::completion_token_for<
	   void(boost::system::error_code)> CompletionToken>
auto set_mostly_omap(const DoutPrefixProvider* dpp,
		     neorados::RADOS& rados, std::string name,
		     CompletionToken&& token)
{
  namespace asio = ::boost::asio;
  namespace sys = ::boost::system;
  namespace R = ::neorados;
  return asio::async_initiate<CompletionToken, void(sys::error_code)>
    (asio::experimental::co_composed<void(sys::error_code)>
     ([](auto state, const DoutPrefixProvider* dpp,
	 R::RADOS& rados, std::string name) -> void {
       try {
	 state.throw_if_cancelled(true);
	 try {
	   // set pg_autoscale_bias
	   auto bias = rados.cct()->_conf.get_val<double>(
	     "rgw_rados_pool_autoscale_bias");
	   std::vector<std::string> biascommand{
	     {fmt::format(R"({{"prefix": "osd pool set", "pool": "{}", )"
			  R"("var": "pg_autoscale_bias", "val": "{}"}})",
			  name, bias)}
	   };
	   co_await rados.mon_command(std::move(biascommand), {}, nullptr,
				      nullptr, asio::deferred);
	 } catch (const std::exception& e) {
	   ldpp_dout(dpp, 2)
	     << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	     << "mon_command to set "
	     <<  "autoscale bias failed with error: "
	     << e.what() << dendl;
	   throw;
	 }
	 try {
	   // set recovery_priority
	   auto p = rados.cct()->_conf.get_val<uint64_t>(
	     "rgw_rados_pool_recovery_priority");
	   std::vector<std::string> recoverycommand{
	     {fmt::format(R"({{"prefix": "osd pool set", "pool": "{}", )"
			  R"("var": "recovery_priority": "{}"}})",
			  name, p)}
	   };
	   co_await rados.mon_command(std::move(recoverycommand), {}, nullptr,
				      nullptr, asio::deferred);
	 } catch (const std::exception& e) {
	   ldpp_dout(dpp, 2)
	     << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	     << "mon_command to set recovery priority failed with error: "
	     << e.what() << dendl;
	   throw;
	 }
	 co_return sys::error_code{};
       } catch (const sys::system_error& e) {
	 ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
			    << ": " << e.what() << dendl;
	 co_return e.code();
       }
     }, rados.get_executor()),
     token, dpp, std::ref(rados), std::move(name));
}

template<boost::asio::completion_token_for<
	   void(boost::system::error_code)> CompletionToken>
auto create_pool(const DoutPrefixProvider* dpp, neorados::RADOS& rados,
		 std::string name, CompletionToken&& token)
{
  namespace asio = ::boost::asio;
  namespace sys = ::boost::system;
  namespace R = ::neorados;
  return asio::async_initiate<CompletionToken, void(sys::error_code)>
    (asio::experimental::co_composed<void(sys::error_code)>
     ([](auto state, const DoutPrefixProvider* dpp,
	 R::RADOS& rados, std::string name) -> void {
       try {
	 state.throw_if_cancelled(true);
	 try {
	   co_await rados.create_pool(std::string(name), std::nullopt,
				      asio::deferred);
	 } catch (const sys::system_error& e) {
	   if (e.code() == sys::errc::result_out_of_range) {
	     ldpp_dout(dpp, 0)
	       << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	       << "ERROR: RADOS::create_pool failed with " << e.what()
	       << " (this can be due to a pool or placement group "
	       << "misconfiguration, e.g. pg_num < pgp_num or "
	       << "mon_max_pg_per_osd exceeded)" << dendl;
	     throw;
	   } else if (e.code() == sys::errc::file_exists) {
	     co_return sys::error_code{};
	   } else {
	     ldpp_dout(dpp, 2)
	       << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	       << "RADOS::create_pool failed with error: " << e.what() << dendl;
	     throw;
	   }
	 } catch (const std::exception& e) {
	   ldpp_dout(dpp, 2)
	     << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	     << "RADOS::create_pool failed with error: " << e.what() << dendl;
	   throw;
	 }
	 co_await rados.enable_application(std::move(name),
					   pg_pool_t::APPLICATION_NAME_RGW,
					   false, asio::deferred);
	 co_return sys::error_code{};
       } catch (const std::system_error& e) {
	 ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	   << "ERROR: " << e.what() << dendl;
	 co_return e.code();
       } catch (const std::exception& e) {
	 ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	   << "ERROR: " << e.what() << dendl;
	 co_return sys::error_code{EIO, sys::generic_category()};
       }
     }, rados.get_executor()),
     token, dpp, std::ref(rados), std::move(name));
}

template<boost::asio::completion_token_for<
	   void(boost::system::error_code)> CompletionToken>
auto create_pool(const DoutPrefixProvider* dpp, neorados::RADOS& rados,
		 std::string name, mostly_omap_t, CompletionToken&& token)
{
  namespace asio = ::boost::asio;
  namespace sys = ::boost::system;
  namespace R = ::neorados;
  return asio::async_initiate<CompletionToken, void(sys::error_code)>
    (asio::experimental::co_composed<void(sys::error_code)>
     ([](auto state, const DoutPrefixProvider* dpp,
	 R::RADOS& rados, std::string name) -> void {
       try {
	 state.throw_if_cancelled(true);
	 co_await create_pool(dpp, rados, name, asio::deferred);
	 co_await set_mostly_omap(dpp, rados, std::move(name), asio::deferred);
	 co_return sys::error_code{};
       } catch (const std::system_error& e) {
	 ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	   << "ERROR: " << e.what() << dendl;
	 co_return e.code();
       } catch (const std::exception& e) {
	 ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	   << "ERROR: " << e.what() << dendl;
	 co_return sys::error_code{EIO, sys::generic_category()};
       }
     }, rados.get_executor()),
     token, dpp, std::ref(rados), std::move(name));
}

template<boost::asio::completion_token_for<
	   void(boost::system::error_code, neorados::IOContext)> CompletionToken>
auto init_iocontext(const DoutPrefixProvider* dpp,
		    neorados::RADOS& rados, rgw_pool pool,
		    CompletionToken&& token)
{
  namespace asio = ::boost::asio;
  namespace sys = ::boost::system;
  namespace R = ::neorados;
  return asio::async_initiate<CompletionToken, void(sys::error_code, R::IOContext)>
    (asio::experimental::co_composed<void(sys::error_code, R::IOContext)>
     ([](auto state, const DoutPrefixProvider* dpp,
	 R::RADOS& rados, rgw_pool pool) -> void {
       try {
	 state.throw_if_cancelled(true);
	 R::IOContext ioc;
	 try {
	   ioc.set_pool(co_await rados.lookup_pool(pool.name, asio::deferred));
	 } catch (const std::exception& e) {
	   ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	     << "RADOS::lookup_pool failed with error: " << e.what() << dendl;
	   throw;
	 }
	 ioc.set_ns(pool.ns);
	 co_return {sys::error_code{}, std::move(ioc)};
       } catch (const std::system_error& e) {
	 ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	   << "ERROR: " << e.what() << dendl;
	 co_return {e.code(), R::IOContext{}};
       } catch (const std::exception& e) {
	 ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	   << "ERROR: " << e.what() << dendl;
	 co_return {sys::error_code{EIO, sys::generic_category()},
		    R::IOContext{}};
       }
     }, rados.get_executor()),
     token, dpp, std::ref(rados), std::move(pool));
}

template<boost::asio::completion_token_for<
	   void(boost::system::error_code, neorados::IOContext)> CompletionToken>
auto init_iocontext(const DoutPrefixProvider* dpp,
		    neorados::RADOS& rados, rgw_pool pool,
		    create_t, CompletionToken&& token)
{
  namespace asio = ::boost::asio;
  namespace sys = ::boost::system;
  namespace R = ::neorados;
  return asio::async_initiate<CompletionToken, void(sys::error_code,
						    R::IOContext)>
    (asio::experimental::co_composed<void(sys::error_code, R::IOContext)>
     ([](auto state, const DoutPrefixProvider* dpp,
	 R::RADOS& rados, rgw_pool pool) -> void {
       bool must_create = false;
       try {
	 state.throw_if_cancelled(true);
	 R::IOContext ioc;
	 try {
	   ioc.set_pool(co_await rados.lookup_pool(pool.name, asio::deferred));
	 } catch (const sys::system_error& e) {
	   if (e.code() == sys::errc::no_such_file_or_directory) {
	     must_create = true;
	   } else {
	     ldpp_dout(dpp, 2)
	       << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	       << "RADOS::lookup_pool failed with error: " << e.what() << dendl;
	     throw;
	   }
	 }
	 if (must_create) {
	   co_await create_pool(dpp, rados, pool.name, asio::deferred);
	   ioc.set_pool(co_await rados.lookup_pool(pool.name, asio::deferred));
	 }
	 ioc.set_ns(pool.ns);
	 co_return {sys::error_code{}, std::move(ioc)};
       } catch (const std::system_error& e) {
	 ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	   << "ERROR: " << e.what() << dendl;
	 co_return {e.code(), R::IOContext{}};
       } catch (const std::exception& e) {
	 ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	   << "ERROR: " << e.what() << dendl;
	 co_return {sys::error_code{EIO, sys::generic_category()},
		    R::IOContext{}};
       }
     }, rados.get_executor()),
     token, dpp, std::ref(rados), std::move(pool));
}

template<boost::asio::completion_token_for<
	   void(boost::system::error_code, neorados::IOContext)> CompletionToken>
auto init_iocontext(const DoutPrefixProvider* dpp,
		    neorados::RADOS& rados, rgw_pool pool,
		    create_t, mostly_omap_t, CompletionToken&& token)
{
  namespace asio = ::boost::asio;
  namespace sys = ::boost::system;
  namespace R = ::neorados;
  return asio::async_initiate<CompletionToken, void(sys::error_code,
						    R::IOContext)>
    (asio::experimental::co_composed<void(sys::error_code, R::IOContext)>
     ([](auto state, const DoutPrefixProvider* dpp,
	 R::RADOS& rados, rgw_pool pool) -> void {
       try {
	 state.throw_if_cancelled(true);
	 auto ioc = co_await init_iocontext(dpp, rados, pool, create,
					    asio::deferred);
	 co_await set_mostly_omap(dpp, rados, pool.name, asio::deferred);
	 co_return {sys::error_code{}, std::move(ioc)};
       } catch (const std::system_error& e) {
	 ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	   << "ERROR: " << e.what() << dendl;
	 co_return {e.code(), R::IOContext{}};
       } catch (const std::exception& e) {
	 ldpp_dout(dpp, 2)
	   << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
	   << "ERROR: " << e.what() << dendl;
	 co_return {sys::error_code{EIO, sys::generic_category()},
		    R::IOContext{}};
       }
     }, rados.get_executor()),
     token, dpp, std::ref(rados), std::move(pool));
}

template<boost::asio::completion_token_for<
	   void(boost::system::error_code, neorados::IOContext)> CompletionToken>
auto init_iocontext(const DoutPrefixProvider* dpp,
		    neorados::RADOS& rados, rgw_pool pool,
		    mostly_omap_t, create_t, CompletionToken&& token)
{
  return init_iocontext(dpp, rados, pool, create, mostly_omap,
			std::forward<CompletionToken>(token));
}
} // namespace rgw
#pragma GCC diagnostic push
