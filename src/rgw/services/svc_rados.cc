// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <fmt/format.h>

#include "svc_rados.h"

#include "common/errno.h"
#include "common/async/waiter.h"
#include "common/convenience.h"
#include "osd/osd_types.h"
#include "rgw/rgw_tools.h"
#include "rgw/rgw_cr_rados.h"

namespace bs = boost::system;
namespace ca = ceph::async;
namespace cb = ceph::buffer;
namespace R = RADOS;

using std::chrono::milliseconds;

tl::expected<std::int64_t, bs::error_code>
RGWSI_RADOS::open_pool(std::string_view pool, bool create, optional_yield y,
		       bool mostly_omap) {
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    bs::error_code ec;
    int64_t id = rados.lookup_pool(std::string(pool), yield[ec]);
    if (ec == bs::errc::no_such_file_or_directory && create) {
      rados.create_pool(pool, nullopt, yield[ec]);
      if (ec && ec != bs::errc::file_exists) {
        if (ec == bs::errc::result_out_of_range) {
          ldout(cct, 0)
            << __func__
            << " ERROR: RADOS::RADOS::create_pool returned " << ec
            << " (this can be due to a pool or placement group misconfiguration, e.g."
            << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
            << dendl;
        }
        return tl::unexpected(ec);
      }
      id = rados.lookup_pool(std::string(pool), yield[ec]);
      if (ec)
        return tl::unexpected(ec);
      rados.enable_application(pool, pg_pool_t::APPLICATION_NAME_RGW, false,
                               yield[ec]);
      if (ec && ec != bs::errc::operation_not_supported) {
        return tl::unexpected(ec);
      }
      if (mostly_omap)
	set_omap_heavy(pool, y);
    }
    if (ec)
      return tl::unexpected(ec);
    return id;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  bs::error_code ec;
  int64_t id;
  {
    ca::waiter<bs::error_code, int64_t> w;
    rados.lookup_pool(std::string(pool), w);
    std::tie(ec, id) = w.wait();
  }
  if (ec == bs::errc::no_such_file_or_directory && create) {
    {
      ca::waiter<bs::error_code> w;
      rados.create_pool(pool, nullopt, w.ref());
      ec = w.wait();
    }
    if (ec && ec != bs::errc::file_exists) {
      if (ec == bs::errc::result_out_of_range) {
        ldout(cct, 0)
          << __func__
          << " ERROR: RADOS::RADOS::create_pool returned " << ec
          << " (this can be due to a pool or placement group misconfiguration, e.g."
          << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
          << dendl;
      }
      return tl::unexpected(ec);
    }
    {
      ca::waiter<bs::error_code, int64_t> w;
      rados.lookup_pool(std::string(pool), w);
      std::tie(ec, id) = w.wait();
    }
    if (ec)
      return tl::unexpected(ec);
    {
      ca::waiter<bs::error_code> w;
      rados.enable_application(pool, pg_pool_t::APPLICATION_NAME_RGW, false, w);
      ec = w.wait();
    }
    if (ec && ec != bs::errc::operation_not_supported) {
      return tl::unexpected(ec);
    }
    if (mostly_omap)
      set_omap_heavy(pool, y);
  }
  if (ec)
    return tl::unexpected(ec);
  return id;
}

// Alternatively, instead of having a bool on the open/create
// functions, we could just expose this function.

// Also we swallow our errors since the version in rgw_tools.cc does.

void RGWSI_RADOS::set_omap_heavy(std::string_view pool, optional_yield y) {
  using namespace std::literals;
  static constexpr auto pool_set =
    "{\"prefix\": \"osd pool set\", "
    "\"pool\": \"{}\" "
    "\"var\": \"{}\", "
    "\"val\": \"{}\"}"sv;

  auto autoscale =
    fmt::format(pool_set, pool, "pg_autoscale_bias"sv,
		pg_autoscale_bias.load(std::memory_order_consume));
  auto num_min =
    fmt::format(pool_set, pool, "pg_num_min"sv,
		pg_num_min.load(std::memory_order_consume));
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    bs::error_code ec;
    rados.mon_command({ autoscale },
		      {}, nullptr, nullptr, yield[ec]);
    if (ec) {
      ldout(cct, 10) << __func__ << " warning: failed to set pg_autoscale_bias on "
		     << pool << dendl;
    }
    rados.mon_command({ num_min },
		      {}, nullptr, nullptr, yield[ec]);
    if (ec) {
      ldout(cct, 10) << __func__ << " warning: failed to set pg_num_min on "
		     << pool << dendl;
    }
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  {
    ca::waiter<bs::error_code> w;
    rados.mon_command({ autoscale },
		      {}, nullptr, nullptr, w);
    auto ec = w.wait();
    if (ec) {
      ldout(cct, 10) << __func__ << " warning: failed to set pg_autoscale_bias on "
		     << pool << dendl;
    }
  }
  {
    ca::waiter<bs::error_code> w;
    rados.mon_command({ num_min },
		      {}, nullptr, nullptr, w);
    auto ec = w.wait();
    if (ec) {
      ldout(cct, 10) << __func__ << " warning: failed to set pg_num_min on "
		     << pool << dendl;
    }
  }
}

RGWSI_RADOS::RGWSI_RADOS(CephContext *cct, boost::asio::io_context& ioc)
  : RGWServiceInstance(cct, ioc),
    rados(R::RADOS::make_with_cct(cct, ioc,
				  boost::asio::use_future).get()) {
  cct->_conf.add_observer(this);
  pg_autoscale_bias.store(
    cct->_conf.get_val<double>("rgw_rados_pool_autoscale_bias"),
    std::memory_order_release);
  pg_num_min.store(
    cct->_conf.get_val<uint64_t>("rgw_rados_pool_pg_num_min"),
    std::memory_order_release);
}

RGWSI_RADOS::~RGWSI_RADOS() {
  cct->_conf.remove_observer(this);
}

const char** RGWSI_RADOS::get_tracked_conf_keys() const {
  static const char* keys[] = {
    "rgw_rados_pool_autoscale_bias",
    "rgw_rados_pool_pg_num_min",
    nullptr
  };
  return keys;
}

void RGWSI_RADOS::handle_conf_change(const ConfigProxy& conf,
				     const std::set<std::string>& changed) {
  if (changed.find("rgw_rados_pool_autoscale_bias") != changed.end()) {
    pg_autoscale_bias.store(
      cct->_conf.get_val<double>("rgw_rados_pool_autoscale_bias"),
      std::memory_order_release);
  }
  if (changed.find("rgw_rados_pool_pg_num_min") != changed.end()) {
    pg_num_min.store(
      cct->_conf.get_val<uint64_t>("rgw_rados_pool_pg_num_min"),
      std::memory_order_release);
  }
}

uint64_t RGWSI_RADOS::instance_id()
{
  return rados.instance_id();
}

bs::error_code RGWSI_RADOS::Obj::operate(R::WriteOp&& op,
					 optional_yield y,
					 version_t* objver)
{
  ldout(cct, 20) << __func__ << " " << o << " " << op << dendl;
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    bs::error_code ec;
    r.execute(o, i, std::move(op), yield[ec], objver);
    return ec;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ca::waiter<bs::error_code> w;
  r.execute(o, i, std::move(op), w, objver);
  return w.wait();
}

bs::error_code RGWSI_RADOS::Obj::operate(R::ReadOp&& op,
					 cb::list* bl,
					 optional_yield y,
					 version_t* objver)
{
  ldout(cct, 20) << __func__ << " " << o << " " << op << dendl;
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    bs::error_code ec;
    r.execute(o, i, std::move(op), bl, yield[ec], objver);
    return ec;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ca::waiter<bs::error_code> w;
  r.execute(o, i, std::move(op), bl, w, objver);
  return w.wait();
}

tl::expected<uint64_t, bs::error_code>
RGWSI_RADOS::Obj::watch(R::RADOS::WatchCB&& f, optional_yield y) {
  ldout(cct, 20) << __func__ << " " << o << dendl;
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    bs::error_code ec;
    auto handle = r.watch(o, i, nullopt, std::move(f), yield[ec]);
    if (ec)
      return tl::unexpected(ec);
    else
      return handle;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ca::waiter<bs::error_code, uint64_t> w;
  r.watch(o, i, nullopt, std::move(f), w);
  auto [ec, handle] = w.wait();
  if (ec)
    return tl::unexpected(ec);
  else
    return handle;
}

bs::error_code RGWSI_RADOS::Obj::unwatch(uint64_t handle,
                                                    optional_yield y)
{
  ldout(cct, 20) << __func__ << " " << o << dendl;
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    bs::error_code ec;
    r.unwatch(handle, i, yield[ec]);
    return ec;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ca::waiter<bs::error_code> w;
  r.unwatch(handle, i, w);
  return w.wait();
}

bs::error_code
RGWSI_RADOS::Obj::notify(cb::list&& bl,
                         std::optional<milliseconds> timeout,
                         cb::list* pbl, optional_yield y) {
  ldout(cct, 20) << __func__ << " " << o << " " << bl << dendl;
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    bs::error_code ec;
    auto rbl = r.notify(o, i, std::move(bl), timeout, yield[ec]);
    if (pbl)
      *pbl = std::move(rbl);
    return ec;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ca::waiter<bs::error_code, cb::list> w;
  r.notify(o, i, std::move(bl), timeout, w.ref());
  auto [ec, rbl] = w.wait();
  if (pbl)
    *pbl = std::move(rbl);
  return ec;
}

bs::error_code
RGWSI_RADOS::Obj::notify_ack(uint64_t notify_id, uint64_t cookie,
                             bufferlist&& bl,
                             optional_yield y) {
  ldout(cct, 20) << __func__ << " " << o << " " << bl << dendl;
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    bs::error_code ec;
    r.notify_ack(o, i, notify_id, cookie, std::move(bl), yield[ec]);
    return ec;
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  ca::waiter<bs::error_code> w;
  r.notify_ack(o, i, notify_id, cookie, std::move(bl), w);
  return w.wait();
}

bs::error_code RGWSI_RADOS::create_pool(const rgw_pool& p,
                                                   optional_yield y,
						   bool mostly_omap)
{
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    bs::error_code ec;
    rados.create_pool(p.name, nullopt, yield[ec]);
    if (ec) {
      if (ec == bs::errc::result_out_of_range) {
        ldout(cct, 0)
          << __func__
          << " ERROR: RADOS::RADOS::create_pool returned " << ec
          << " (this can be due to a pool or placement group misconfiguration, e.g."
          << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
          << dendl;
      }
      return ec;
    }
    rados.enable_application(p.name, pg_pool_t::APPLICATION_NAME_RGW, false,
                             yield[ec]);
    if (ec && ec != bs::errc::operation_not_supported) {
      return ec;
    }
    if (mostly_omap)
      set_omap_heavy(p.name, y);
    return {};
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  bs::error_code ec;
  {
    ca::waiter<bs::error_code> w;
    rados.create_pool(p.name, nullopt, w);
    ec = w.wait();
  }
  if (ec) {
    if (ec == bs::errc::result_out_of_range) {
      ldout(cct, 0)
        << __func__
        << " ERROR: RADOS::RADOS::create_pool returned " << ec
        << " (this can be due to a pool or placement group misconfiguration, e.g."
        << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
        << dendl;
    }
    return ec;
  }
  {
    ca::waiter<bs::error_code> w;
    rados.enable_application(p.name, pg_pool_t::APPLICATION_NAME_RGW, false, w);
    ec = w.wait();
  }
  if (mostly_omap)
    set_omap_heavy(p.name, y);
  if (ec && ec != bs::errc::operation_not_supported) {
    return ec;
  }

  return {};
}

RGWSI_RADOS::Pool::List RGWSI_RADOS::Pool::list(RGWAccessListFilter filter) {
  return List(*this, std::move(filter), std::nullopt);
}

RGWSI_RADOS::Obj RGWSI_RADOS::Pool::obj(std::string_view oid) {
  return RGWSI_RADOS::Obj(cct, rados, ioc.pool(), ioc.ns(), oid, {}, name);
}

RGWSI_RADOS::Obj RGWSI_RADOS::Pool::obj(std::string_view oid,
					std::string_view loc) {
  return RGWSI_RADOS::Obj(cct, rados, ioc.pool(), ioc.ns(), oid, loc, name);
}


std::optional<RGWSI_RADOS::Pool::List>
RGWSI_RADOS::Pool::list(std::string marker,
			RGWAccessListFilter filter) {
  return maybe_do(R::Cursor::from_str(marker),
		  [&](R::Cursor c) {
		    return List(*this, std::move(filter), std::move(c));
		  });
}


bs::error_code
RGWSI_RADOS::Pool::List::get_next(int max,
                                  std::vector<std::string>* oids,
                                  bool* is_truncated,
                                  optional_yield y)
{
  if (iter == R::Cursor::end())
    return ceph::to_error_code(-ENOENT);

  std::vector<R::Entry> ls;
  bs::error_code ec;

#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    pool.rados.enumerate_objects(pool.ioc, iter,
                                 RADOS::Cursor::end(),
                                 max, {}, &ls, &iter,
                                 yield[ec]);
    }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(pool.cct, 20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  if (!y) {
    ca::waiter<bs::error_code> w;
    pool.rados.enumerate_objects(pool.ioc, iter,
                                 RADOS::Cursor::end(),
                                 max, {}, &ls, &iter,
                                 w);
    ec = w.wait();
  }

  if (ec)
    return ec;

  for (auto& e : ls) {
    auto& oid = e.oid;
    ldout(pool.cct, 20) << "Pool::get_ext: got " << oid << dendl;
    if (filter && !filter(oid, oid))
      continue;

    oids->push_back(std::move(oid));
  }

  if (is_truncated)
    *is_truncated = (iter != RADOS::Cursor::end());
  return {};
}

bs::error_code RGWSI_RADOS::Pool::List::get_all(std::vector<std::string> *oids,
						optional_yield y) {
  iter = R::Cursor::begin();
  bs::error_code ec;
  bool truncated = true;
  while (truncated && !ec) {
    ec = get_next(1000, oids, &truncated, y);
  }
  return ec;
}

bs::error_code RGWSI_RADOS::Pool::List::for_each(
  std::function<void(std::string_view)> f, optional_yield y) {
  iter = R::Cursor::begin();
  bs::error_code ec;
  bool truncated = true;
  std::vector<std::string> oids;
  oids.reserve(1000);
  while (truncated && !ec) {
    ec = get_next(1000, &oids, &truncated, y);
    std::for_each(oids.begin(), oids.end(), f);
    oids.clear();
  }
  return ec;
}


tl::expected<RGWSI_RADOS::Obj, bs::error_code>
RGWSI_RADOS::obj(const rgw_raw_obj& o, optional_yield y) {
  auto r = open_pool(o.pool.name, true, y, false);
  if (r)
    return Obj(cct, rados, *r, o.pool.ns, o.oid, o.loc, o.pool.name);
  else
    return tl::unexpected(r.error());
}
RGWSI_RADOS::Obj RGWSI_RADOS::obj(const RGWSI_RADOS::Pool& p,
				  std::string_view oid,
				  std::string_view loc) {
  return Obj(cct, rados, p.ioc.pool(), p.ioc.ns(), oid, loc, p.name);
}

tl::expected<RGWSI_RADOS::Pool, bs::error_code>
RGWSI_RADOS::pool(const rgw_pool& p, optional_yield y, bool mostly_omap) {
  auto r = open_pool(p.name, true, y, mostly_omap);
  if (r)
    return Pool(cct, rados, *r, p.ns, p.name);
  else
    return tl::unexpected(r.error());
}

void RGWSI_RADOS::watch_flush(optional_yield y) {
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& yield = y.get_yield_context();
    rados.flush_watch(yield);
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldout(cct, 20) << "WARNING: blocking librados call" << dendl;
  }
  return;
#endif
  ca::waiter<> w;
  rados.flush_watch(w);
  w.wait();
}
