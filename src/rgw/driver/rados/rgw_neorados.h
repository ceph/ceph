// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/detail/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "common/dout.h"

#include "osd/osd_types.h"

#include "rgw_obj_types.h"
#include "rgw_pool_types.h"
#include "obj_version_asio.h"

namespace rgw::neorados {
using namespace ::neorados;
namespace asio = boost::asio;
namespace sys = boost::system;
namespace buffer = ceph::buffer;

struct mostly_omap_t {};
inline constexpr mostly_omap_t mostly_omap;

struct create_t {};
inline constexpr create_t create;

asio::awaitable<void> set_mostly_omap(const DoutPrefixProvider* dpp,
				      RADOS& rados, std::string_view name);

asio::awaitable<void> create_pool(const DoutPrefixProvider* dpp,
				  RADOS& rados, const std::string& name);
asio::awaitable<void> create_pool(const DoutPrefixProvider* dpp,
				  RADOS& rados, const std::string& name,
				  mostly_omap_t);

asio::awaitable<IOContext> init_iocontext(const DoutPrefixProvider* dpp,
					  RADOS& rados, const rgw_pool& pool);
asio::awaitable<IOContext> init_iocontext(const DoutPrefixProvider* dpp,
					  RADOS& rados, const rgw_pool& pool,
					  create_t);
asio::awaitable<IOContext> init_iocontext(const DoutPrefixProvider* dpp,
					  RADOS& rados, const rgw_pool& pool,
					  create_t, mostly_omap_t);
asio::awaitable<IOContext> init_iocontext(const DoutPrefixProvider* dpp,
					  RADOS& rados, const rgw_pool& pool,
					  mostly_omap_t, create_t);


asio::awaitable<void> write_bl(const DoutPrefixProvider* dpp, RADOS& r,
                               Object oid, IOContext ioc,
                               buffer::list bl,
                               VersionTracker* objv_tracker = nullptr,
                               bool exclusive = false);

asio::awaitable<void> write_obj(const DoutPrefixProvider* dpp, RADOS& r,
				Object oid, IOContext ioc,
				const ceph::encodable auto& data,
				VersionTracker* objv_tracker = nullptr,
				bool exclusive = false)
{
  using ceph::encode;
  buffer::list bl;
  encode(data, bl);
  return write_bl(dpp, r, std::move(oid), std::move(ioc), std::move(bl),
		  objv_tracker, exclusive);
}

asio::awaitable<void> write_obj(const DoutPrefixProvider* dpp, RADOS& r,
				const rgw_raw_obj& obj,
				const ceph::encodable auto& data,
				VersionTracker* objv_tracker = nullptr,
				bool exclusive = false)
{
  Object oid{obj.oid};
  IOContext ioc;
  try {
    ioc = co_await init_iocontext(dpp, r, obj.pool, create);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 10)
      << "ERROR: rgw::neorados::write_obj: failed opening pool: "
      << obj.pool << ": " << e.what() << dendl;
    throw;
  }
  ioc.set_key(obj.loc);

  co_await write_obj(dpp, r, std::move(oid), std::move(ioc), data,
		     objv_tracker, exclusive);
  co_return;
}

asio::awaitable<buffer::list> read_bl(const DoutPrefixProvider* dpp,
				      RADOS& r, Object oid, IOContext ioc,
				      VersionTracker* objv_tracker = nullptr);


template<ceph::decodable D>
asio::awaitable<D> read_obj(const DoutPrefixProvider* dpp,
			    RADOS& r, Object oid, IOContext ioc,
			    bool empty_on_enoent = false,
			    VersionTracker* objv_tracker = nullptr)
{
  buffer::list bl;
  bool return_empty = false;
  D data{};
  try {
    bl = co_await read_bl(dpp, r, std::move(oid), std::move(ioc), objv_tracker);
  } catch (const sys::system_error& e) {
    if (empty_on_enoent && (sys::errc::no_such_file_or_directory == e.code())) {
      // Annoying workaround since you can't `co_return` from a `catch`-clause
      return_empty = true;
    } else {
      // read_bl will print a useful error message
      throw;
    }
  }
  if (return_empty) {
    co_return data;
  }
  try {
    auto bi = bl.cbegin();
    using ceph::decode;
    decode(data, bi);
  } catch (buffer::error& e) {
    ldpp_dout(dpp, 10)
      << "ERROR: rgw::neorados::read_obj: failed decoding object: "
      << ioc << "/" << oid << ": " << e.what() << dendl;
    throw;
  }
  co_return std::move(data);
}

template<ceph::decodable D>
asio::awaitable<D> read_obj(const DoutPrefixProvider* dpp,
			    RADOS& r, const rgw_raw_obj& obj,
			    bool empty_on_enoent = false,
			    VersionTracker* objv_tracker = nullptr)
{
  Object oid{obj.oid};
  IOContext ioc;
  bool return_empty = false;
  try {
    ioc = co_await init_iocontext(dpp, r, obj.pool);
  } catch (const sys::system_error& e) {
    if (empty_on_enoent && (sys::errc::no_such_file_or_directory == e.code())) {
      // Annoying workaround since you can't `co_return` from a `catch`-clause
      return_empty = true;
    } else {
      ldpp_dout(dpp, 10)
	<< "ERROR: rgw::neorados::read_obj: failed opening pool: "
	<< obj.pool << ": " << e.what() << dendl;
      throw;
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 10)
      << "ERROR: rgw::neorados::read_obj: failed opening pool: "
      << obj.pool << ": " << e.what() << dendl;
    throw;
  }
  if (return_empty) {
    co_return D{};
  }
  ioc.set_key(obj.loc);
  co_return co_await read_obj<D>(dpp, r, std::move(oid), std::move(ioc),
				 empty_on_enoent, objv_tracker);
}
}
