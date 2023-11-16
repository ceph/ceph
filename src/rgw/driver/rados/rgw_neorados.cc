#include "rgw_neorados.h"

#include <coroutine>
#include <vector>

#include <fmt/format.h>

namespace rgw::neorados {
asio::awaitable<void> set_mostly_omap(const DoutPrefixProvider* dpp,
                                      RADOS& rados, std::string_view name)
{
  // set pg_autoscale_bias
  try {
    auto bias = rados.cct()->_conf.get_val<double>(
      "rgw_rados_pool_autoscale_bias");
    std::vector<std::string> biascommand{
      {fmt::format(R"({{"prefix": "osd pool set", "pool": "{}", )"
                   R"("var": "pg_autoscale_bias", "val": "{}"}})",
                   name, bias)}
    };
    co_await rados.mon_command(std::move(biascommand), {}, nullptr, nullptr,
                               asio::use_awaitable);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 2) << "rgw::neorados::set_mostly_omap: mon_command to set "
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
    co_await rados.mon_command(std::move(recoverycommand), {}, nullptr, nullptr,
                               asio::use_awaitable);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 2) << "rgw::neorados::set_mostly_omap: mon_command to set "
                      <<  "recovery priority failed with error: "
                      << e.what() << dendl;
  }
  co_return;
}

asio::awaitable<void> create_pool(const DoutPrefixProvider* dpp,
				  RADOS& rados, const std::string& name)
{
  bool already_exists = false;
  try {
    co_await rados.create_pool(std::string(name), std::nullopt,
                               asio::use_awaitable);
  } catch (const sys::system_error& e) {
    if (e.code() == sys::errc::result_out_of_range) {
      ldpp_dout(dpp, 0)
        << "init_iocontext: ERROR: RADOS::create_pool failed with " << e.what()
        << " (this can be due to a pool or placement group misconfiguration, "
        "e.g. pg_num < pgp_num or mon_max_pg_per_osd exceeded)" << dendl;
    } else if (e.code() == sys::errc::file_exists) {
      already_exists = true;
    } else {
      ldpp_dout(dpp, 2) << "rgw::neorados::create_pool: RADOS::create_pool "
                        <<  "failed with error: " << e.what() << dendl;
      throw;
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 2) << "rgw::neorados::create_pool: RADOS::create_pool "
                      <<  "failed with error: " << e.what() << dendl;
    throw;
  }
  if (already_exists) {
    co_return;
  }
  try {
    co_await rados.enable_application(name, pg_pool_t::APPLICATION_NAME_RGW,
                                      false, asio::use_awaitable);
  } catch (const sys::system_error& e) {
    if (e.code() != sys::errc::operation_not_supported) {
      ldpp_dout(dpp, 2) << "rgw::neorados::create_pool: RADOS::set_application "
                        <<  "failed with error: " << e.what() << dendl;
      throw;
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 2) << "rgw::neorados::create_pool: RADOS::set_application "
                      <<  "failed with error: " << e.what() << dendl;
    throw;
  }
  co_return;
}
asio::awaitable<void> create_pool(const DoutPrefixProvider* dpp,
				  RADOS& rados, const std::string& name,
				  mostly_omap_t)
{
  co_await create_pool(dpp, rados, name);
  co_await set_mostly_omap(dpp, rados, name);
  co_return;
}

asio::awaitable<IOContext> init_iocontext(const DoutPrefixProvider* dpp,
					  RADOS& rados, const rgw_pool& pool)
{
  IOContext ioc;
  try {
    ioc.set_pool(co_await rados.lookup_pool(pool.name, asio::use_awaitable));
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 2) << "rgw::neorados::init_ioctx: RADOS::lookup_pool "
                      <<  "failed with error: " << e.what() << dendl;
    throw;
  }
  ioc.set_ns(pool.ns);
  co_return ioc;
}

asio::awaitable<IOContext> init_iocontext(const DoutPrefixProvider* dpp,
					  RADOS& rados, const rgw_pool& pool,
					  create_t)
{
  bool must_create = false;
  IOContext ioc;
  try {
    ioc.set_pool(co_await rados.lookup_pool(pool.name, asio::use_awaitable));
  } catch (const sys::system_error& e) {
    if (e.code() == sys::errc::no_such_file_or_directory) {
      must_create = true;
    } else {
      ldpp_dout(dpp, 2) << "rgw::neorados::init_ioctx: RADOS::lookup_pool "
                        <<  "failed with error: " << e.what() << dendl;
      throw;
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 2) << "rgw::neorados::init_ioctx: RADOS::lookup_pool "
                      <<  "failed with error: " << e.what() << dendl;
    throw;
  }
  if (must_create) {
    co_await create_pool(dpp, rados, pool.name);
    try {
      ioc.set_pool(co_await rados.lookup_pool(pool.name, asio::deferred));
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 2) << "rgw::neorados::init_ioctx: RADOS::lookup_pool "
                        <<  "failed with error: " << e.what() << dendl;
      throw;
    }
  }

  ioc.set_ns(pool.ns);
  co_return ioc;
}

asio::awaitable<IOContext> init_iocontext(const DoutPrefixProvider* dpp,
					  RADOS& rados, const rgw_pool& pool,
					  create_t, mostly_omap_t)
{
  auto ioc = co_await init_iocontext(dpp, rados, pool, create);
  co_await set_mostly_omap(dpp, rados, pool.name);
  co_return ioc;
}

asio::awaitable<IOContext> init_iocontext(const DoutPrefixProvider* dpp,
					  RADOS& rados, const rgw_pool& pool,
					  mostly_omap_t, create_t)
{
  return init_iocontext(dpp, rados, pool, create, mostly_omap);
}

asio::awaitable<void> write_bl(const DoutPrefixProvider* dpp, RADOS& r,
                               Object oid, IOContext ioc,
                               buffer::list bl,
                               VersionTracker* objv_tracker,
                               bool exclusive)
{
  WriteOp op;
  if (exclusive) {
    op.create(true);
  }
  if (objv_tracker) {
    objv_tracker->prepare_write(op);
  }
  op.write_full(std::move(bl));
  try {
    co_await r.execute(std::move(oid), std::move(ioc), std::move(op),
                       asio::use_awaitable);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 10) << "ERROR: rgw::neorados::write_bl: failed writing obj: "
                       << ioc << "/" << oid << ": " << e.what() << dendl;
    throw;
  }
  if (objv_tracker) {
    objv_tracker->apply_write();
  }
  co_return;
}

asio::awaitable<buffer::list> read_bl(const DoutPrefixProvider* dpp,
				      RADOS& r, Object oid, IOContext ioc,
				      VersionTracker* objv_tracker)
{
  ReadOp op;
  if (objv_tracker) {
    objv_tracker->prepare_read(op);
  }
  buffer::list bl;
  op.read(0, 0, &bl);
  try {
    co_await r.execute(std::move(oid), std::move(ioc), std::move(op), nullptr,
                       asio::use_awaitable);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 10) << "ERROR: rgw::neorados::read_bl: failed reading obj: "
                       << ioc << "/" << oid << ": " << e.what() << dendl;
    throw;
  }
  co_return bl;
}
}
