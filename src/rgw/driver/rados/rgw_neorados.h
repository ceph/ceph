// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <coroutine>
#include <string_view>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/detail/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "common/dout.h"

#include "osd/osd_types.h"

#include "rgw_obj_types.h"
#include "rgw_pool_types.h"

namespace rgw::neorados {
using namespace ::neorados;
namespace asio = boost::asio;

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
}
