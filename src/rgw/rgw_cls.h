// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_RGW_CLS_H
#define CEPH_RGW_RGW_CLS_H

#include <cstdint>
#include <string>

#include <boost/container/flat_map.hpp>
#include <boost/system/error_code.hpp>

#include "include/expected.hpp"

#include "common/async/yield_context.h"

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/timeindex/cls_timeindex_types.h"

#include "RADOS/cls/rgw.h"

#include "rgw/services/svc_rados.h"

namespace rgw::cls {
namespace bc = boost::container;
namespace bs = boost::system;
namespace RCr = RADOS::CLS::rgw;


tl::expected<bc::flat_map<int, rgw_cls_list_ret>, bs::error_code>
get_dir_header(RGWSI_RADOS::Pool& pool,
	       const bc::flat_map<int, std::string>& oids,
	       const std::uint32_t max_aio, optional_yield y);

tl::expected<bc::flat_map<int, rgw_cls_check_index_ret>, bs::error_code>
check_bucket(RGWSI_RADOS::Pool& pool,
	     const bc::flat_map<int, std::string>& oids,
	     const std::uint32_t max_aio, optional_yield y);

bs::error_code bucket_index_init(RGWSI_RADOS::Pool& pool,
				 const bc::flat_map<int, std::string>& oids,
				 const std::uint32_t max_aio, optional_yield y);

bs::error_code bucket_index_cleanup(RGWSI_RADOS::Pool& pool,
				    const bc::flat_map<int, std::string>& oids,
				    const std::uint32_t max_aio,
                                    optional_yield y);

bs::error_code rebuild_bucket(RGWSI_RADOS::Pool& pool,
			      const bc::flat_map<int, std::string>& oids,
			      const std::uint32_t max_aio, optional_yield y);

tl::expected<cls_rgw_bucket_instance_entry, bs::error_code>
get_bucket_resharding(RGWSI_RADOS::Obj& obj, optional_yield y);

bs::error_code
set_tag_timeout(RGWSI_RADOS::Pool& pool,
		const bc::flat_map<int, std::string>& oids,
		uint64_t timeout, const std::uint32_t max_aio,
		optional_yield y);

bs::error_code
set_bucket_resharding(RGWSI_RADOS::Pool& pool,
		      const bc::flat_map<int, std::string>& oids,
		      const cls_rgw_bucket_instance_entry& entry,
		      const std::uint32_t max_aio, optional_yield y);

tl::expected<bc::flat_map<int, rgw_cls_list_ret>,
	     bs::error_code>
list_bucket(RGWSI_RADOS::Pool& pool,
	    const bc::flat_map<int, std::string>& oids,
	    const cls_rgw_obj_key& start_obj,
	    const std::string& filter_prefix,
	    uint32_t num_entries,
	    bool list_versions,
	    const std::uint32_t max_aio, optional_yield y,
	    version_t* last_version = nullptr);
bs::error_code bi_log_resync(RGWSI_RADOS::Pool& pool,
                             const bc::flat_map<int, std::string>& oids,
                             const std::uint32_t max_aio, optional_yield y);

bs::error_code
bi_log_trim(RGWSI_RADOS::Pool& pool,
	    const RCr::bi_shards_mgr& start_marker,
	    const RCr::bi_shards_mgr& end_marker,
	    const bc::flat_map<int, std::string>& raw_oids,
	    std::uint32_t max_aio, optional_yield y);

tl::expected<bc::flat_map<int, cls_rgw_bi_log_list_ret>,
	     boost::system::error_code>
bi_log_list(RGWSI_RADOS::Pool& pool,
	    const RCr::bi_shards_mgr& marker, std::uint32_t max,
	    const bc::flat_map<int, std::string>& oids,
	    std::uint32_t max_aio, optional_yield y);

bs::error_code bi_log_stop(RGWSI_RADOS::Pool& pool,
			   const bc::flat_map<int, std::string>& oids,
			   const std::uint32_t max_aio, optional_yield y);

tl::expected<std::tuple<std::vector<cls_timeindex_entry>, std::string, bool>,
	     bs::error_code>
timeindex_list(RGWSI_RADOS::Obj& obj, ceph::real_time from,
               ceph::real_time to, std::string_view in_marker, int max_entries,
               optional_yield y);

bs::error_code timeindex_trim(RGWSI_RADOS::Obj& obj,
                              ceph::real_time from_time,
                              ceph::real_time to_time,
                              std::optional<std::string_view> from_marker,
                              std::optional<std::string_view> to_marker,
                              optional_yield y);

tl::expected<std::tuple<std::vector<cls_user_bucket_entry>, std::string, bool>,
	     bs::error_code> user_bucket_list(RGWSI_RADOS::Obj& obj,
					      std::string_view in_marker,
					      std::string_view end_marker,
					      int max_entries,
					      optional_yield y);

tl::expected<cls_user_header,
	     bs::error_code> user_get_header(RGWSI_RADOS::Obj obj,
                                             optional_yield y);
}

#endif // CEPH_RGW_RGW_CLS_H
