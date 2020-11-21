// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "cls/log/cls_log_client.h"

#include "rgw_log_backing.h"
#include "rgw_tools.h"
#include "cls_fifo_legacy.h"

static constexpr auto dout_subsys = ceph_subsys_rgw;

enum class shard_check { dne, omap, fifo, corrupt };
inline std::ostream& operator <<(std::ostream& m, const shard_check& t) {
  switch (t) {
  case shard_check::dne:
    return m << "shard_check::dne";
  case shard_check::omap:
    return m << "shard_check::omap";
  case shard_check::fifo:
    return m << "shard_check::fifo";
  case shard_check::corrupt:
    return m << "shard_check::corrupt";
  }

  return m << "shard_check::UNKNOWN=" << static_cast<uint32_t>(t);
}

namespace {
/// Return the shard type, and a bool to see whether it has entries.
std::pair<shard_check, bool>
probe_shard(librados::IoCtx& ioctx, const std::string& oid, optional_yield y)
{
  auto cct = static_cast<CephContext*>(ioctx.cct());
  bool omap = false;
  {
    librados::ObjectReadOperation op;
    cls_log_header header;
    cls_log_info(op, &header);
    auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, y);
    if (r == -ENOENT) {
      return { shard_check::dne, {} };
    }

    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " error probing for omap: r=" << r
		 << ", oid=" << oid << dendl;
      return { shard_check::corrupt, {} };
    }
    if (header != cls_log_header{})
      omap = true;
  }
  std::unique_ptr<rgw::cls::fifo::FIFO> fifo;
  auto r = rgw::cls::fifo::FIFO::open(ioctx, oid,
				      &fifo, y,
				      std::nullopt, true);
  if (r < 0 && !(r == -ENOENT || r == -ENODATA)) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " error probing for fifo: r=" << r
	       << ", oid=" << oid << dendl;
    return { shard_check::corrupt, {} };
  }
  if (fifo && omap) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " fifo and omap found: oid=" << oid << dendl;
    return { shard_check::corrupt, {} };
  }
  if (fifo) {
    bool more = false;
    std::vector<rgw::cls::fifo::list_entry> entries;
    r = fifo->list(1, nullopt, &entries, &more, y);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": unable to list entries: r=" << r
		 << ", oid=" << oid << dendl;
      return { shard_check::corrupt, {} };
    }
    return { shard_check::fifo, !entries.empty() };
  }
  if (omap) {
    std::list<cls_log_entry> entries;
    std::string out_marker;
    bool truncated = false;
    librados::ObjectReadOperation op;
    cls_log_list(op, {}, {}, {}, 1, entries,
		 &out_marker, &truncated);
    auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, y);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed to list: r=" << r << ", oid=" << oid << dendl;
      return { shard_check::corrupt, {} };
    }
    return { shard_check::omap, !entries.empty() };
  }

  // An object exists, but has never had FIFO or cls_log entries written
  // to it. Likely just the marker Omap.
  return { shard_check::dne, {} };
}

tl::expected<log_type, bs::error_code>
handle_dne(librados::IoCtx& ioctx,
	   log_type def,
	   std::string oid,
	   optional_yield y)
{
  auto cct = static_cast<CephContext*>(ioctx.cct());
  if (def == log_type::fifo) {
    std::unique_ptr<rgw::cls::fifo::FIFO> fifo;
    auto r = rgw::cls::fifo::FIFO::create(ioctx, oid,
					  &fifo, y,
					  std::nullopt);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " error creating FIFO: r=" << r
		 << ", oid=" << oid << dendl;
      return tl::unexpected(bs::error_code(-r, bs::system_category()));
    }
  }
  return def;
}
}

tl::expected<log_type, bs::error_code>
log_backing_type(librados::IoCtx& ioctx,
		 log_type def,
		 int shards,
		 const fu2::unique_function<std::string(int) const>& get_oid,
		 optional_yield y)
{
  auto cct = static_cast<CephContext*>(ioctx.cct());
  auto check = shard_check::dne;
  for (int i = 0; i < shards; ++i) {
    auto [c, e] = probe_shard(ioctx, get_oid(i), y);
    if (c == shard_check::corrupt)
      return tl::unexpected(bs::error_code(EIO, bs::system_category()));
    if (c == shard_check::dne) continue;
    if (check == shard_check::dne) {
      check = c;
      continue;
    }

    if (check != c) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " clashing types: check=" << check
		 << ", c=" << c << dendl;
      return tl::unexpected(bs::error_code(EIO, bs::system_category()));
    }
  }
  if (check == shard_check::corrupt) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " should be unreachable!" << dendl;
    return tl::unexpected(bs::error_code(EIO, bs::system_category()));
  }

  if (check == shard_check::dne)
    return handle_dne(ioctx,
		      def,
		      get_oid(0),
		      y);

  return (check == shard_check::fifo ? log_type::fifo : log_type::omap);
}

bs::error_code log_remove(librados::IoCtx& ioctx,
			  int shards,
			  const fu2::unique_function<std::string(int) const>& get_oid,
			  optional_yield y)
{
  bs::error_code ec;
  auto cct = static_cast<CephContext*>(ioctx.cct());
  for (int i = 0; i < shards; ++i) {
    auto oid = get_oid(i);
    rados::cls::fifo::info info;
    uint32_t part_header_size = 0, part_entry_overhead = 0;

    auto r = rgw::cls::fifo::get_meta(ioctx, oid, nullopt, &info,
				      &part_header_size, &part_entry_overhead,
				      0, y, true);
    if (r == -ENOENT) continue;
    if (r == 0 && info.head_part_num > -1) {
      for (auto j = info.tail_part_num; j <= info.head_part_num; ++j) {
	librados::ObjectWriteOperation op;
	op.remove();
	auto part_oid = info.part_oid(j);
	auto subr = rgw_rados_operate(ioctx, part_oid, &op, null_yield);
	if (subr < 0 && subr != -ENOENT) {
	  if (!ec)
	    ec = bs::error_code(-subr, bs::system_category());
	  lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << ": failed removing FIFO part: part_oid=" << part_oid
		     << ", subr=" << subr << dendl;
	}
      }
    }
    if (r < 0 && r != -ENODATA) {
      if (!ec)
	ec = bs::error_code(-r, bs::system_category());
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed checking FIFO part: oid=" << oid
		 << ", r=" << r << dendl;
    }
    librados::ObjectWriteOperation op;
    op.remove();
    r = rgw_rados_operate(ioctx, oid, &op, null_yield);
    if (r < 0 && r != -ENOENT) {
      if (!ec)
	ec = bs::error_code(-r, bs::system_category());
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed removing shard: oid=" << oid
		 << ", r=" << r << dendl;
    }
  }
  return ec;
}
