// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <atomic>

#include "include/RADOS/RADOS.hpp"

#include "include/buffer.h"

#include "cls/user/cls_user_ops.h"

#include "common/async/gather_completion.h"
#include "common/async/waiter.h"

#include "RADOS/cls/timeindex.h"
#include "RADOS/cls/user.h"

#include "rgw/rgw_cls.h"

#include "rgw/rgw_tools.h"

namespace rgw::cls {
namespace ca = ceph::async;
namespace cb = ceph::buffer;
namespace R = RADOS;
namespace RCt = RADOS::CLS::timeindex;
namespace RCu = RADOS::CLS::user;

namespace {
constexpr auto dout_subsys = ceph_subsys_rgw;

template<typename T>
auto readop_decoder(std::atomic<bs::error_code>* ecp, T* p) {
  return [p, ecp](bs::error_code ec, cb::list b) {
	   if (ec)
	     *ecp = ec;
	   else try {
	       decode(*p, b);
	     } catch (const bs::system_error& e) {
	       *ecp = e.code();
	     }
	 };
}

template<typename T, typename GC, typename OpFun>
void sharded_read_loop(RGWSI_RADOS::Pool pool,
		       bc::flat_map<int, std::string>::const_iterator& i,
		       bc::flat_map<int, std::string>::const_iterator end,
		       GC& gc,
		       bc::flat_map<int, T>& result,
		       std::atomic<bs::error_code>* ec,
		       std::vector<version_t>::iterator& v,
		       OpFun&& opfun,
		       const std::uint32_t max_aio)
{
  for (std::uint32_t aios = 0;
       ((aios < max_aio) || (max_aio == 0)) && (i != end);
       ++aios, ++i, ++v) {
    [[maybe_unused]] auto [inserted, _] =
      result.emplace(std::piecewise_construct,
		     std::forward_as_tuple(i->first),
		     std::forward_as_tuple());
    pool.aio_operate(i->second, opfun(i->first, ec, &inserted->second), nullptr,
                     gc.completion_minor(), &(*v));
  }
}

template<typename T, typename OpFun>
tl::expected<bc::flat_map<int, T>, bs::error_code>
sharded_readop(RGWSI_RADOS::Pool pool,
	       const bc::flat_map<int, std::string>& oids,
	       const std::uint32_t max_aio, OpFun&& opfun,
	       optional_yield y,
	       version_t* last_version = nullptr)
{
  static_assert(
    std::is_same_v<std::invoke_result_t<
                     OpFun, int,std::atomic<bs::error_code>*, T*>,
    R::ReadOp>);
  std::vector<version_t> versions(oids.size(), 0);
  auto i = oids.begin();
  auto v = versions.begin();
  bc::flat_map<int, T> result;
  result.reserve(oids.size());
  bs::error_code ec;
  while (i != oids.end() && !ec) {
    {
      std::atomic<bs::error_code> iec;
#ifdef HAVE_BOOST_CONTEXT
      if (y) {
	auto& yield = y.get_yield_context();
	auto gc = ca::make_gather_completion<void(bs::error_code)>(
	  pool.get_rados().get_executor(), yield[ec],
          ca::AnyTrue<bs::error_code>{},
          bs::error_code{});
        sharded_read_loop(pool, i, oids.end(), gc, result, &iec, v, opfun,
			  max_aio);
	gc.complete();
      }
      // work on asio threads should be asynchronous, so warn when they block
      if (is_asio_thread) {
        ldout(pool.get_cct(), 20) << "WARNING: blocking librados call" << dendl;
      }
#endif
      if (!y) {
	ca::waiter<bs::error_code> w;
	auto gc = ca::make_gather_completion<void(bs::error_code)>(
	  pool.get_rados().get_executor(), w.ref(), ca::AnyTrue<bs::error_code>{},
	  bs::error_code{});
	sharded_read_loop(pool, i, oids.end(), gc, result, &iec, v, opfun,
			  max_aio);
	gc.complete();
	ec = w.wait();
      }
      if (iec.load() && !ec)
	ec = iec;
    }
  }

  if (last_version)
    *last_version = *std::max_element(versions.cbegin(), versions.cend());

  if (ec)
    return tl::unexpected(ec);

  return result;
}

template<typename GC, typename OpFun>
void sharded_write_loop(RGWSI_RADOS::Pool& pool,
			bc::flat_map<int, std::string>::const_iterator& i,
			bc::flat_map<int, std::string>::const_iterator end,
			GC& gc,
			OpFun&& opfun,
			const std::uint32_t max_aio)
{
  static_assert(std::is_convertible_v<std::invoke_result_t<OpFun>, R::WriteOp>,
		"opfun must return an operation.");
  for (std::uint32_t aios = 0;
       ((aios < max_aio) || (max_aio == 0)) && (i != end);
       ++aios, ++i) {
    pool.aio_operate(i->second, std::forward<OpFun>(opfun)(),
                     gc.completion_minor());
  }
}

auto make_errer(bool (*errok)(bs::error_code)) {
  return [errok](bs::error_code s, bs::error_code o) {
	   if (o && !s) {
	     if (errok && errok(o)) {
	       return bs::error_code{};
	     } else {
	       return o;
	     }
	   }
	   return s;
	 };
}

template<typename OpFun>
bs::error_code
sharded_writeop(RGWSI_RADOS::Pool& pool,
		const bc::flat_map<int, std::string>& oids,
		const std::uint32_t max_aio, OpFun&& opfun,
		bs::error_code (*cleanup)(RGWSI_RADOS::Pool&,
					  const bc::flat_map<int, std::string>&,
					  const std::uint32_t max_aio,
					  optional_yield),
		bool (*errok)(bs::error_code),
		optional_yield y)
{
  static_assert(std::is_convertible_v<std::invoke_result_t<OpFun>, R::WriteOp>,
		"opfun must return an operation.");
  auto i = oids.begin();
  bs::error_code ec;
  while (i != oids.end() && !ec) {
    {
#ifdef HAVE_BOOST_CONTEXT
      if (y) {
	auto& yield = y.get_yield_context();
	auto gc = ca::make_gather_completion<void(bs::error_code)>(
	  pool.get_rados().get_executor(), yield[ec], make_errer(errok),
	  bs::error_code{});
	sharded_write_loop(pool, i, oids.end(), gc,
			   std::forward<OpFun>(opfun), max_aio);
	gc.complete();
      }
      // work on asio threads should be asynchronous, so warn when they block
      if (is_asio_thread) {
        ldout(pool.get_cct(), 20) << "WARNING: blocking librados call" << dendl;
      }
#endif
      if (!y) {
	ca::waiter<bs::error_code> w;
	auto gc = ca::make_gather_completion<void(bs::error_code)>(
	  pool.get_rados().get_executor(), w.ref(), make_errer(errok), bs::error_code{});
	sharded_write_loop(pool, i, oids.end(), gc,
			   std::forward<OpFun>(opfun), max_aio);
	gc.complete();
	ec = w.wait();
      }
    }
  }
  if (ec && cleanup)
    cleanup(pool, oids, max_aio, y);

  return ec;
}

R::WriteOp remove()
{
  R::WriteOp op;
  op.remove();
  return op;
}

bool nosuch_okay(bs::error_code ec) {
  return ec == bs::errc::no_such_file_or_directory;
}

bs::error_code removalize(RGWSI_RADOS::Pool& pool,
                          const bc::flat_map<int, std::string>& oids,
                          const std::uint32_t max_aio, optional_yield y) {
  return sharded_writeop(pool, oids, max_aio,
			 &remove, nullptr, &nosuch_okay, y);
}


std::optional<std::string_view> get_bi_shard(const RCr::bi_shards_mgr& mgr,
					     int shard) {
  if (auto i = mgr.find(shard); i != mgr.end())
    return i->second;
  return std::nullopt;
}
}
tl::expected<bc::flat_map<int, rgw_cls_list_ret>, bs::error_code>
get_dir_header(RGWSI_RADOS::Pool& pool,
	       const bc::flat_map<int, std::string>& oids,
	       const std::uint32_t max_aio, optional_yield y)
{
  return sharded_readop<rgw_cls_list_ret>(
    pool, oids, max_aio,
    [](int, std::atomic<bs::error_code>* ec, rgw_cls_list_ret* p) {
      auto op = RCr::bucket_list({}, {}, 0, false, readop_decoder(ec, p));
      return op;
    }, y);
}

tl::expected<bc::flat_map<int, rgw_cls_check_index_ret>, bs::error_code>
check_bucket(RGWSI_RADOS::Pool& pool,
	     const bc::flat_map<int, std::string>& oids,
	     const std::uint32_t max_aio, optional_yield y)
{
  return sharded_readop<rgw_cls_check_index_ret>(
    pool, oids, max_aio,
    [](int, std::atomic<bs::error_code>* ec, rgw_cls_check_index_ret* p) {
      return RCr::check_bucket(readop_decoder(ec, p));
    }, y);
}

bs::error_code bucket_index_init(RGWSI_RADOS::Pool& pool,
				 const bc::flat_map<int, std::string>& oids,
				 const std::uint32_t max_aio, optional_yield y)
{
  return sharded_writeop(pool, oids, max_aio, RCr::bucket_index_init,
			 removalize, nullptr, y);
}

bs::error_code bucket_index_cleanup(RGWSI_RADOS::Pool& pool,
				    const bc::flat_map<int, std::string>& oids,
				    const std::uint32_t max_aio,
                                    optional_yield y)
{
  return removalize(pool, oids, max_aio, y);
}

bs::error_code rebuild_bucket(RGWSI_RADOS::Pool& pool,
			      const bc::flat_map<int, std::string>& oids,
			      const std::uint32_t max_aio, optional_yield y)
{
  return sharded_writeop(pool, oids, max_aio, RCr::rebuild_bucket,
			 nullptr, nullptr, y);
}

tl::expected<cls_rgw_bucket_instance_entry, bs::error_code>
get_bucket_resharding(RGWSI_RADOS::Obj& obj, optional_yield y)
{
  cb::list bl;

  auto op = RCr::get_bucket_resharding(&bl);

  auto ec = obj.operate(std::move(op), nullptr, y);

  if (ec)
    return tl::unexpected(ec);

  cls_rgw_get_bucket_resharding_ret op_ret;
  auto iter = bl.cbegin();
  try {
    decode(op_ret, iter);
  } catch (const buffer::error& err) {
    return tl::unexpected(err.code());
  }

  return std::move(op_ret.new_instance);
}

bs::error_code
set_bucket_resharding(RGWSI_RADOS::Pool& pool,
		      const bc::flat_map<int, std::string>& oids,
		      const cls_rgw_bucket_instance_entry& entry,
		      const std::uint32_t max_aio, optional_yield y)
{
  return sharded_writeop(pool, oids, max_aio,
                         [&entry] {
                           return RCr::set_bucket_resharding(entry);
                         }, nullptr, nullptr, y);

}

bs::error_code
set_tag_timeout(RGWSI_RADOS::Pool& pool,
		const bc::flat_map<int, std::string>& oids,
		uint64_t timeout, const std::uint32_t max_aio,
		optional_yield y)
{
  return sharded_writeop(pool, oids, max_aio,
			 [timeout] { return RCr::set_tag_timeout(timeout); },
			 nullptr, nullptr, y);

}

tl::expected<bc::flat_map<int, rgw_cls_list_ret>,
	     bs::error_code>
list_bucket(RGWSI_RADOS::Pool& pool,
	    const bc::flat_map<int, std::string>& oids,
	    const cls_rgw_obj_key& start_obj,
	    const std::string& filter_prefix,
	    uint32_t num_entries,
	    bool list_versions,
	    const std::uint32_t max_aio, optional_yield y,
	    version_t* last_version)
{
  return sharded_readop<rgw_cls_list_ret>(
    pool, oids, max_aio,
    [&](int, std::atomic<bs::error_code>* ec, rgw_cls_list_ret* p) {
      return RCr::bucket_list(start_obj, filter_prefix, num_entries,
                              list_versions, readop_decoder(ec, p));
    }, y, last_version);
}

bs::error_code
bi_log_trim(RGWSI_RADOS::Pool& pool,
	    const RCr::bi_shards_mgr& start_marker,
	    const RCr::bi_shards_mgr& end_marker,
	    const bc::flat_map<int, std::string>& raw_oids,
	    std::uint32_t max_aio, optional_yield y)
{
  bc::flat_map<int, std::pair<std::string_view, bs::error_code>> oids;
  for (const auto& [shard, oid] : raw_oids) {
    oids.emplace(std::piecewise_construct,
		 std::forward_as_tuple(shard),
		 std::forward_as_tuple(oid, bs::error_code{}));
  }

  auto errer = [](bs::error_code s, bs::error_code o) {
    if (o && !s) {
      if (o == bs::errc::no_message_available) {
	return bs::error_code{};
      } else {
	return o;
      }
    }
    return s;
  };
  auto innerloop = [&](decltype(oids)::iterator& i, auto& gc) {
                     for (std::uint32_t aios = 0;
		     ((aios < max_aio) || (max_aio == 0)) && (i != oids.end());
		     ++aios, ++i) {
		  auto shard = i->first;
		  auto op = RCr::bi_log_trim(get_bi_shard(start_marker, shard),
                                             get_bi_shard(end_marker, shard),
                                             &i->second.second);
		  pool.aio_operate(i->second.first, std::move(op),
                                   gc.completion_minor());
		}
	      };

  bs::error_code ec;
  while (!oids.empty() && !ec) {
    auto i = oids.begin();
    while (i != oids.end() && !ec) {
      {
#ifdef HAVE_BOOST_CONTEXT
	if (y) {
	  auto& yield = y.get_yield_context();
	  auto gc = ca::make_gather_completion<void(bs::error_code)>(
            pool.get_rados().get_executor(), yield[ec], errer,
            bs::error_code{});
          innerloop(i, gc);
	  gc.complete();
	}
      // work on asio threads should be asynchronous, so warn when they block
      if (is_asio_thread) {
        ldout(pool.get_cct(), 20) << "WARNING: blocking librados call" << dendl;
      }
#endif
	if (!y) {
	  ca::waiter<bs::error_code> w;
	  auto gc = ca::make_gather_completion<void(bs::error_code)>(
	    pool.get_rados().get_executor(), w.ref(), errer, bs::error_code{});
	  innerloop(i, gc);
	  gc.complete();
	  ec = w.wait();
	}
      }
    }
    if (!ec) {
      decltype(oids) t;
      t.swap(oids);
      for (auto&& i = t.begin(); i != t.end(); ++i) {
	if (i->second.second != bs::errc::no_message_available)
	  oids.insert(std::move(*i));
      }
    }
  }
  return ec;
}

bs::error_code bi_log_resync(RGWSI_RADOS::Pool& pool,
                             const bc::flat_map<int, std::string>& oids,
                             const std::uint32_t max_aio, optional_yield y)
{
  return sharded_writeop(pool, oids, max_aio, &RCr::bi_log_resync,
			 nullptr, nullptr, y);

}

bs::error_code bi_log_stop(RGWSI_RADOS::Pool& pool,
			   const bc::flat_map<int, std::string>& oids,
			   const std::uint32_t max_aio, optional_yield y) {
  return sharded_writeop(pool, oids, max_aio, &RCr::bi_log_stop,
			 nullptr, nullptr, y);
}

tl::expected<bc::flat_map<int, cls_rgw_bi_log_list_ret>,
	     boost::system::error_code>
bi_log_list(RGWSI_RADOS::Pool& pool,
	    const RCr::bi_shards_mgr& marker, std::uint32_t max,
	    const bc::flat_map<int, std::string>& oids,
	    std::uint32_t max_aio, optional_yield y)
{
  return sharded_readop<cls_rgw_bi_log_list_ret>(
    pool, oids, max_aio,
    [&](int shard, std::atomic<bs::error_code>* ec, cls_rgw_bi_log_list_ret* p) {
      return RCr::bi_log_list(get_bi_shard(marker, shard), max,
                       readop_decoder(ec, p));
    }, y);
}

tl::expected<std::tuple<std::vector<cls_timeindex_entry>, std::string, bool>,
	     bs::error_code>
timeindex_list(RGWSI_RADOS::Obj& obj, ceph::real_time from,
               ceph::real_time to, std::string_view in_marker, int max_entries,
               optional_yield y)
{
  bs::error_code bec;
  cb::list bl;
  auto ec = obj.operate(RCt::list(from, to, in_marker, max_entries, &bl, &bec),
                        nullptr, y);
  if (ec)
    return tl::unexpected(ec);
  if (bec)
    return tl::unexpected(bec);

  try {
    cls_timeindex_list_ret ret;
    auto iter = bl.cbegin();
    decode(ret, iter);
    return std::make_tuple(std::move(ret.entries),
                           std::move(ret.marker),
                           ret.truncated);
  } catch (cb::error const& err) {
    return tl::unexpected(err.code());
  }
}

bs::error_code timeindex_trim(RGWSI_RADOS::Obj& obj,
                              ceph::real_time from_time,
                              ceph::real_time to_time,
                              std::optional<std::string_view> from_marker,
                              std::optional<std::string_view> to_marker,
                              optional_yield y)
{
  bs::error_code ec;

  do {
    ec = obj.operate(RCt::trim(from_time, to_time, from_marker, to_marker), y);
  } while (!ec);

  if (ec == bs::errc::no_message_available)
    ec.clear();

  return ec;
}

tl::expected<std::tuple<std::vector<cls_user_bucket_entry>, std::string, bool>,
	     bs::error_code> user_bucket_list(RGWSI_RADOS::Obj& obj,
                                              std::string_view in_marker,
                                              std::string_view end_marker,
                                              int max_entries,
                                              optional_yield y)
{
  cb::list bl;
  bs::error_code bec;
  auto ec = obj.operate(
    RCu::bucket_list(in_marker, end_marker, max_entries, &bl, &bec),
    nullptr, y);
  if (ec)
    return tl::unexpected(ec);
  if (bec)
    return tl::unexpected(bec);
  try {
    cls_user_list_buckets_ret ret;
    auto iter = bl.cbegin();
    decode(ret, iter);
    return std::make_tuple(std::move(ret.entries), std::move(ret.marker),
                           ret.truncated);
  } catch (cb::error const& err) {
    return tl::unexpected(err.code());
  }
}

tl::expected<cls_user_header,
	     bs::error_code> user_get_header(RGWSI_RADOS::Obj obj,
                                             optional_yield y)
{
  cb::list bl;
  bs::error_code bec;
  auto ec = obj.operate(
    RCu::get_header(&bl, &bec),
    nullptr, y);
  if (ec)
    return tl::unexpected(ec);
  if (bec)
    return tl::unexpected(bec);
  try {
    cls_user_get_header_ret ret;
    auto iter = bl.cbegin();
    decode(ret, iter);
    return std::move(ret.header);
  } catch (cb::error const& err) {
    return tl::unexpected(err.code());
  }
}
}
