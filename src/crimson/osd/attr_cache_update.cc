// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/osd/attr_cache_update.h"

#include <fmt/format.h>

#include "include/ceph_assert.h"

namespace crimson::osd {

void update_attr_cache(
  ceph::os::Transaction& txn,
  std::span<const tracked_attrs_t> objs)
{
  using ceph::os::Transaction;
  const auto find = [objs](const ghobject_t& oid) -> attr_map_t* {
    for (const auto& obj : objs) {
      if (*obj.oid == oid.hobj) {
	return obj.attrs;
      }
    }
    return nullptr;
  };
  // The data of the ops is decoded in order, so also decode the data of
  // the ops that do not change xattrs.
  for (auto i = txn.begin(); i.have_op(); ) {
    const auto* op = i.decode_op();
    switch (op->op) {
    case Transaction::OP_SETATTR: {
      auto name = i.decode_string();
      ceph::bufferlist bl;
      i.decode_bl(bl);
      if (auto attrs = find(i.get_oid(op->oid)); attrs) {
	(*attrs)[std::move(name)] = std::move(bl);
      }
      break;
    }
    case Transaction::OP_SETATTRS: {
      attr_map_t aset;
      i.decode_attrset(aset);
      if (auto attrs = find(i.get_oid(op->oid)); attrs) {
	for (auto& [name, bl] : aset) {
	  (*attrs)[name] = std::move(bl);
	}
      }
      break;
    }
    case Transaction::OP_RMATTR: {
      auto name = i.decode_string();
      if (auto attrs = find(i.get_oid(op->oid)); attrs) {
	attrs->erase(name);
      }
      break;
    }
    case Transaction::OP_RMATTRS:
    case Transaction::OP_REMOVE:
      if (auto attrs = find(i.get_oid(op->oid)); attrs) {
	attrs->clear();
      }
      break;
    case Transaction::OP_CLONE:
      // the destination gets all the xattrs of the source
      if (auto dst = find(i.get_oid(op->dest_oid)); dst) {
	auto src = find(i.get_oid(op->oid));
	ceph_assert(src);
	*dst = *src;
      }
      break;
    case Transaction::OP_COLL_MOVE_RENAME:
    case Transaction::OP_TRY_RENAME:
      // only used on temp objects, which have no obc
      ceph_assert(!find(i.get_oid(op->oid)));
      ceph_assert(!find(i.get_oid(op->dest_oid)));
      break;
    case Transaction::OP_COLL_ADD:
    case Transaction::OP_COLL_REMOVE:
      // these ops have no dest_oid
      ceph_assert(!find(i.get_oid(op->oid)));
      break;
    case Transaction::OP_WRITE:
    case Transaction::OP_OMAP_SETHEADER:
    case Transaction::OP_COLL_HINT: {
      ceph::bufferlist bl;
      i.decode_bl(bl);
      break;
    }
    case Transaction::OP_OMAP_SETKEYS: {
      ceph::bufferlist bl;
      i.decode_attrset_bl(&bl);
      break;
    }
    case Transaction::OP_OMAP_RMKEYS: {
      ceph::bufferlist bl;
      i.decode_keyset_bl(&bl);
      break;
    }
    case Transaction::OP_OMAP_RMKEYRANGE:
      std::ignore = i.decode_string();
      std::ignore = i.decode_string();
      break;
    case Transaction::OP_NOP:
    case Transaction::OP_CREATE:
    case Transaction::OP_TOUCH:
    case Transaction::OP_TOUCH_TEMP:
    case Transaction::OP_ZERO:
    case Transaction::OP_TRUNCATE:
    case Transaction::OP_OMAP_CLEAR:
    case Transaction::OP_CLONERANGE2:
    case Transaction::OP_SETALLOCHINT:
    case Transaction::OP_MKCOLL:
    case Transaction::OP_RMCOLL:
    case Transaction::OP_COLL_SET_BITS:
    case Transaction::OP_SPLIT_COLLECTION2:
    case Transaction::OP_MERGE_COLLECTION:
      // no xattr change and no data
      break;
    default:
      ceph_abort_msg(
	fmt::format("unexpected op {} in a client transaction",
		    static_cast<unsigned>(op->op)));
    }
  }
}

} // namespace crimson::osd
