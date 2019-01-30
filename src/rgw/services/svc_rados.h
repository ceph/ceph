// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include "rgw/rgw_service.h"
#include "rgw/rgw_tools.h"
#include "include/expected.hpp"

#include "include/RADOS/RADOS.hpp"

#include "common/async/waiter.h"
#include "common/async/yield_context.h"
#include "common/RWLock.h"

class RGWAsyncRadosProcessor;

#include "rgw/rgw_service.h"


using RGWAccessListFilter =
  std::function<bool(std::string_view, std::string_view)>;

inline auto RGWAccessListFilterPrefix(std::string prefix) {
  return [prefix = std::move(prefix)](std::string_view,
                                      std::string_view key) -> bool {
           return (prefix.compare(key.substr(0, prefix.size())) == 0);
         };
}

class RGWSI_RADOS : public RGWServiceInstance,
		    public md_config_obs_t
{
  static constexpr auto dout_subsys = ceph_subsys_rgw;

  RADOS::RADOS rados;

public:

  RADOS::RADOS& get_rados() {
    return rados;
  }

  operator RADOS::RADOS&() {
    return rados;
  }

  class Obj;

  class Pool {
    friend class RGWSI_RADOS;

    CephContext* cct;
    RADOS::RADOS& rados;
    RADOS::IOContext ioc;
    std::string name; // Rather annoying.

    Pool(CephContext* cct,
         RADOS::RADOS& rados,
         int64_t id,
         std::string ns,
         std::string(name)) : cct(cct), rados(rados), ioc(id, ns), name(name) {}

  public:

    class List {
      friend Pool;
      Pool& pool;
      RGWAccessListFilter filter;
      RADOS::Cursor iter;

      List(Pool& pool, RGWAccessListFilter filter,
	   std::optional<RADOS::Cursor> marker) :
	pool(pool), filter(std::move(filter)),
	iter(marker ? *marker : RADOS::Cursor::begin()) {}

    public:

      boost::system::error_code get_next(int max,
                                         std::vector<std::string> *oids,
                                         bool *is_truncated,
                                         optional_yield);
      boost::system::error_code get_all(std::vector<std::string> *oids,
					optional_yield);

      boost::system::error_code for_each(std::function<void(std::string_view)> f,
					 optional_yield);
      std::string get_marker() const {
	return iter.to_str();
      }
      bool set_marker(std::string s) {
	if (auto c = RADOS::Cursor::from_str(s)) {
	  iter = *c;
	  return true;
	} else {
	  return false;
	}
      }
    };

    List list(RGWAccessListFilter filter = nullptr);
    std::optional<List> list(std::string marker,
			     RGWAccessListFilter filter = nullptr);

    RADOS::RADOS& get_rados() {
      return rados;
    }

    RADOS::IOContext& get_ioc() {
      return ioc;
    }

    rgw_pool get_rgw_pool() const {
      return { name, std::string(ioc.ns()) };
    }

    CephContext* get_cct() const {
      return cct;
    }

    Pool with_key(std::string ns) {
      return Pool(cct,
		  rados,
		  ioc.pool(),
		  std::move(ns),
		  name);

    }

    template<typename CompletionToken>
    auto aio_operate(std::string_view oid, RADOS::WriteOp&& op,
		     CompletionToken&& token, version_t* v = nullptr)
      {
	ldout(cct, 20) << __func__ << " " << name << "/" << oid << " " << op
		       << dendl;

	return rados.execute(oid, ioc, std::move(op),
			     std::forward<CompletionToken>(token), v);
      }
    template<typename CompletionToken>
    auto aio_operate(std::string_view oid, RADOS::ReadOp&& op,
		     ceph::buffer::list* bl, CompletionToken&& token,
		     version_t* v = nullptr)
      {
	ldout(cct, 20) << __func__ << " " << name << "/" << oid << " " << op
		       << dendl;
	return rados.execute(oid, ioc, std::move(op), bl,
			     std::forward<CompletionToken>(token), v);
    }

    Obj obj(std::string_view oid);
    Obj obj(std::string_view oid, std::string_view loc);
  };

private:

  tl::expected<std::int64_t, boost::system::error_code>
  open_pool(std::string_view pool, bool create, optional_yield y,
	    bool mostly_omap);

  void set_omap_heavy(std::string_view pool, optional_yield y);

  std::atomic<double> pg_autoscale_bias;
  std::atomic<uint64_t> pg_num_min;

public:
  RGWSI_RADOS(CephContext *cct, boost::asio::io_context& ioc);
  ~RGWSI_RADOS();
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set<std::string>& changed) override;

  void init() {}

  uint64_t instance_id();

  class Obj {
    friend class RGWSI_RADOS;

    CephContext* cct;
    RADOS::RADOS& r;
    RADOS::IOContext i;
    RADOS::Object o;
    std::string pname; // Fudge. We need this for now to support
                       // getting an rgw_raw_obj back out.

    Obj(CephContext* cct, RADOS::RADOS& rados, int64_t pool, std::string_view ns,
        std::string_view obj, std::string_view key, std::string_view pname)
      : cct(cct), r(rados), i(pool, ns), o(obj), pname(pname) {
      if (!key.empty())
        i.key(key);
    }

  public:

    Obj(const Obj& rhs)
      : cct(rhs.cct), r(rhs.r), i(rhs.i), o(rhs.o),
        pname(rhs.pname) {}
    Obj(Obj&& rhs)
      : cct(rhs.cct), r(rhs.r), i(std::move(rhs.i)), o(std::move(rhs.o)),
	pname(std::move(rhs.pname)) {}

    Obj& operator =(const Obj& rhs) {
      i = rhs.i;
      o = rhs.o;
      pname = rhs.pname;
      return *this;
    }
    Obj& operator =(Obj&& rhs) {
      i = std::move(rhs.i);
      o = std::move(rhs.o);
      pname = std::move(rhs.pname);
      return *this;
    }

    rgw_raw_obj get_raw_obj() const {
      return rgw_raw_obj(rgw_pool(pname, std::string(i.ns())),
                         std::string(o),
                         std::string(i.key().value_or(std::string_view{})));
    }

    RADOS::RADOS& rados() {
      return r;
    }
    std::string_view oid() const {
      return std::string_view(o);
    }
    const RADOS::IOContext& ioc() const {
      return i;
    }

    boost::system::error_code operate(RADOS::WriteOp&& op, optional_yield y,
                                      version_t* objver = nullptr);
    boost::system::error_code operate(RADOS::ReadOp&& op,
                                      ceph::buffer::list* bl,
                                      optional_yield y,
                                      version_t* objver = nullptr);
    template<typename CompletionToken>
    auto aio_operate(RADOS::WriteOp&& op, CompletionToken&& token) {
      ldout(cct, 20) << __func__ << " " << o << " " << op << dendl;

      return r.execute(o, i, std::move(op),
		       std::forward<CompletionToken>(token));
    }
    template<typename CompletionToken>
    auto aio_operate(RADOS::ReadOp&& op, ceph::buffer::list* bl,
                     CompletionToken&& token) {
      ldout(cct, 20) << __func__ << " " << o << " " << op << dendl;
      return r.execute(o, i, std::move(op), bl,
		       std::forward<CompletionToken>(token));
    }

    tl::expected<uint64_t, boost::system::error_code>
    watch(RADOS::RADOS::WatchCB&& f, optional_yield y);
    template<typename CompletionToken>
    auto aio_watch(RADOS::RADOS::WatchCB&& f, CompletionToken&& token) {
      ldout(cct, 20) << __func__ << " " << o << dendl;
      return r.watch(o, i, nullopt, std::move(f),
		     std::forward<CompletionToken>(token));
    }
    boost::system::error_code unwatch(uint64_t handle,
                                      optional_yield y);
    boost::system::error_code
    notify(bufferlist&& bl,
           std::optional<std::chrono::milliseconds> timeouts,
           bufferlist* pbl, optional_yield y);
    boost::system::error_code notify_ack(uint64_t notify_id, uint64_t cookie, bufferlist&& bl,
                                         optional_yield y);
  };

  boost::system::error_code create_pool(const rgw_pool& p, optional_yield y,
					bool mostly_omap = false);

  void watch_flush(optional_yield y);

  tl::expected<Obj, boost::system::error_code>
  obj(const rgw_raw_obj& o, optional_yield y);
  Obj obj(const Pool& p, std::string_view oid, std::string_view loc);

  tl::expected<Pool, boost::system::error_code>
  pool(const rgw_pool& p, optional_yield y, bool mostly_omap = false);

  friend Obj;
  friend Pool;
  friend Pool::List;
};

inline ostream& operator<<(ostream& out, const RGWSI_RADOS::Obj& obj) {
  return out << obj.get_raw_obj();
}
