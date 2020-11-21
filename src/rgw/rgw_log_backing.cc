// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "cls/log/cls_log_client.h"

#include "rgw_log_backing.h"
#include "rgw_tools.h"
#include "cls_fifo_legacy.h"

static constexpr auto dout_subsys = ceph_subsys_rgw;

struct logmark {
  log_type found;
  log_type specified;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(uint32_t(found), bl);
    encode(uint32_t(specified), bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    uint32_t f, s;
    decode(f, bl);
    decode(s, bl);
    found = static_cast<log_type>(f);
    specified = static_cast<log_type>(s);
  }

  bool validate() {
    if (!(found == log_type::omap || found == log_type::fifo)) {
      return false;
    }
    if (found == log_type::omap && !(specified == log_type::omap ||
				     specified == log_type::neither)) {
      return false;
    }
    if (found == log_type::fifo && !(specified == log_type::fifo ||
				     specified == log_type::neither)) {
      return false;
    }
    return true;
  }
};
WRITE_CLASS_ENCODER(logmark)
static inline std::ostream& operator <<(std::ostream& m, const logmark& l) {
  return m << "{ found: " << l.found << ", specified: " << l.specified << " }";
}

inline const auto logmark_omap_key = "logmap_validator"s;

log_type log_quick_check(librados::IoCtx& ioctx,
			 log_type specified,
			 const fu2::unique_function<
			   std::string(int) const>& get_oid,
			 optional_yield y)
{
  auto cct = static_cast<CephContext*>(ioctx.cct());
  librados::ObjectReadOperation op;
  std::map<std::string, ceph::bufferlist> values;
  int r_out = 0;
  op.omap_get_vals_by_keys({ logmark_omap_key }, &values, &r_out);

  auto oid = get_oid(0);
  auto r = rgw_rados_operate(ioctx, oid,
			     &op, nullptr, y);
  if (r < 0 || r_out < 0 || values.empty()) {
    if (r != -ENOENT) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " failed to read omap r=" << r
		 << " on oid=" << oid << dendl;
    }
    return log_type::neither;
  }

  if (auto gotkey = values.begin()->first; gotkey != logmark_omap_key) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " can't happen: asked for key " << logmark_omap_key
	       << " got key " << gotkey << dendl;
    return log_type::neither;
  }

  logmark m;
  try {
    decode(m, values.begin()->second);
  } catch (const buffer::error& e) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " error decoding logmark: " << e.what() << dendl;
    return log_type::neither;
  }
  if (!m.validate()) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " invalid logmark: " << m << dendl;
    return log_type::neither;
  }

  if ((m.found == log_type::omap && specified == log_type::fifo) ||
      (m.found == log_type::fifo && specified == log_type::omap)) {
    ldout(cct, 5) << __PRETTY_FUNCTION__ << ":" << __LINE__
		  << " discordant logmark: found=" << m.found
		  << ", specified=" << specified << dendl;
    return log_type::neither;
  }

  return m.found;
}

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

std::tuple<log_check, bool, log_type>
handle_dne(librados::IoCtx& ioctx,
	   log_type specified,
	   log_type bias,
	   std::string oid,
	   optional_yield y)
{
  auto cct = static_cast<CephContext*>(ioctx.cct());
  auto actual = specified == log_type::neither ? bias : specified;
  if (actual == log_type::fifo) {
    std::unique_ptr<rgw::cls::fifo::FIFO> fifo;
    auto r = rgw::cls::fifo::FIFO::create(ioctx, oid,
					  &fifo, y,
					  std::nullopt);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " error creating FIFO: r=" << r
		 << ", oid=" << oid << dendl;
      return { log_check::corruption, {}, {} };
    }
  }
  logmark m{ actual, specified };
  librados::ObjectWriteOperation op;
  op.create(false);
  ceph::bufferlist bl;
  encode(m, bl);
  op.omap_set({ { logmark_omap_key, bl } });
  auto r = rgw_rados_operate(ioctx, oid, &op, y);
  if (r < 0) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " error setting logmark: r=" << r << dendl;
    return { log_check::corruption, {}, {} };
  }
  return { log_check::concord, false, actual };
}
}

std::tuple<log_check, bool, log_type>
log_setup_backing(librados::IoCtx& ioctx,
		  log_type specified,
		  log_type bias,
		  int shards,
		  const fu2::unique_function<std::string(int) const>& get_oid,
		  optional_yield y)
{
  ceph_assert(bias != log_type::neither);
  auto cct = static_cast<CephContext*>(ioctx.cct());
  {
    librados::ObjectWriteOperation op;
    op.omap_rm_keys({ logmark_omap_key });
    auto oid = get_oid(0);
    auto r = rgw_rados_operate(ioctx, oid, &op, y);
    if (r < 0 && r != -ENOENT) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " error removing logmark: r=" << r << dendl;
      return { log_check::corruption, {}, {} };
    }
  }
  bool zero_exists = false;
  auto check = shard_check::dne;
  bool has_entries = false;
  for (int i = 0; i < shards; ++i) {
    auto [c, e] = probe_shard(ioctx, get_oid(i), y);
    if (i == 0) zero_exists = (c != shard_check::dne);
    if (c == shard_check::corrupt) return { log_check::corruption, {}, {} };
    if (c == shard_check::dne) continue;
    if (check == shard_check::dne) {
      check = c;
      continue;
    }

    if (check != c) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " clashing types: check=" << check
		 << ", c=" << c << dendl;
      return { log_check::corruption, {}, {} };
    }
    if (e)
      has_entries = true;
  }
  if (check == shard_check::corrupt) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " should be unreachable!" << dendl;
    return { log_check::corruption, {}, {} };
  }

  if (check == shard_check::dne)
    return handle_dne(ioctx,
		      specified,
		      bias,
		      get_oid(0),
		      y);

  auto found = check == shard_check::fifo ? log_type::fifo : log_type::omap;
  if ((specified != log_type::neither ) && (found != specified)) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " discord: found=" << found
	       << ", specified=" << specified << dendl;
    return { log_check::discord, has_entries, found };
  }

  logmark m{ found, specified };
  librados::ObjectWriteOperation op;
  if (!zero_exists) {
    op.create(false);
  }
  ceph::bufferlist bl;
  encode(m, bl);
  op.omap_set({ { logmark_omap_key, bl } });
  auto r = rgw_rados_operate(ioctx, get_oid(0), &op, y);
  if (r < 0) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " error setting logmark: r=" << r << dendl;
    return { log_check::corruption, {}, {} };
  }
  return { log_check::concord, has_entries, found };
}

int log_remove(librados::IoCtx& ioctx,
	       int shards,
	       const fu2::unique_function<std::string(int) const>& get_oid,
	       optional_yield y)
{
  int finalr = 0;
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
	  if (finalr != 0) finalr = subr;
	  lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << ": failed removing FIFO part: part_oid=" << part_oid
		     << ", subr=" << subr << dendl;
	}
      }
    }
    if (r < 0 && r != -ENODATA) {
      if (finalr != 0) finalr = r;
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed checking FIFO part: oid=" << oid
		 << ", r=" << r << dendl;
    }
    librados::ObjectWriteOperation op;
    op.remove();
    r = rgw_rados_operate(ioctx, oid, &op, null_yield);
    if (r < 0 && r != -ENOENT) {
      if (finalr != 0) finalr = r;
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed removing shard: oid=" << oid
		 << ", r=" << r << dendl;
    }
  }
  return finalr;
}

log_type log_acquire_backing(librados::IoCtx& ioctx,
			     int shards,
			     log_type specified,
			     log_type bias,
			     const fu2::unique_function<
			       std::string(int) const>& get_oid,
			     optional_yield y)
{
  auto cct = static_cast<CephContext*>(ioctx.cct());
  {
    auto found = log_quick_check(ioctx, specified, get_oid, y);
    if (found != log_type::neither) {
      return found;
    }
  }
  {
    auto [check, has_entries, found] = log_setup_backing(ioctx, specified, bias, shards,
						       get_oid, y);
    if (check == log_check::concord) {
      return found;
    }
    if (check == log_check::corruption) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": corrupt log found, unable to proceed" << dendl;
      return log_type::neither;
    }
    ceph_assert(check == log_check::discord);
    if (has_entries) {
      lderr(cct) << __PRETTY_FUNCTION__  << ":" << __LINE__ << ": "
		 << specified << " specified, but"
		 << found << " found, and log is not empty. Cannot continue."
		 << dendl;
      return log_type::neither;
    }
    ldout(cct, 5) << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": "
		  << specified << " specified, but"
		  << found << " found. Log is empty. Deleting and recreating."
		  << dendl;
    if (log_remove(ioctx, shards, get_oid, y) < 0)
      return log_type::neither;
  }
  auto [check, has_entries, found] = log_setup_backing(ioctx, specified, bias, shards,
						       get_oid, y);
  if (check != log_check::concord) {
    lderr(cct) << __PRETTY_FUNCTION__ << ": " << __LINE__
	       << ": failed to re-initialize log."
	       << dendl;
    return log_type::neither;
  }
  return found;
}
