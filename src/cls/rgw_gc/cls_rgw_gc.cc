// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/types.h"
#include "rgw/datum/adapters/ceph-datum.h"

#include <errno.h>
#include <expected>
#include <functional>
#include <limits>
#include <optional>
#include <unordered_map>
#include <utility>

#include <datum/range.h>

#include "objclass/objclass.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw_gc/cls_rgw_gc_types.h"
#include "cls/rgw_gc/cls_rgw_gc_ops.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_const.h"
#include "cls/queue/cls_queue_src.h"

#include "rgw/datum/adapters/cls/queued_gc.h"

#define GC_LIST_DEFAULT_MAX 128

using std::string;

using ceph::bufferlist;
using ceph::salad::datum::try_decode;
using ceph::encode;
using ceph::make_timespan;
using ceph::real_time;

CLS_VER(1,0)
CLS_NAME(rgw_gc)

namespace {

class urgent_data_xattr final
{
  using map_type = std::unordered_map<string, real_time>;
  cls_method_context_t hctx;
  std::optional<map_type> entries;

public:
  explicit urgent_data_xattr(cls_method_context_t hctx) : hctx(hctx) {}

public:
  std::expected<std::reference_wrapper<map_type>, int> read()
  {
    if (entries) {
      return std::ref(*entries);
    }

    bufferlist bytes;
    const int status = cls_cxx_getxattr(hctx, "cls_queue_urgent_data", &bytes);
    if (status < 0 && status != -ENOENT && status != -ENODATA) {
      CLS_LOG(0, "ERROR: reading urgent data xattr returned %d", status);
      return std::unexpected(status);
    }

    map_type candidate;
    if (status >= 0) {
      auto cursor = bytes.cbegin();
      if (!try_decode(candidate, cursor)) {
        CLS_LOG(1, "ERROR: failed to decode urgent data xattr");
        return std::unexpected(-EINVAL);
      }
    }
    entries.emplace(std::move(candidate));

    return std::ref(*entries);
  }

  int write() const
  {
    bufferlist bytes;
    encode(*entries, bytes);

    return cls_cxx_setxattr(hctx, "cls_queue_urgent_data", &bytes);
  }
};

} // anonymous namespace

static int cls_rgw_gc_queue_init(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls::rgw::queued_gc_datum::init_request op;
  if (!try_decode(op, in_iter)) {
    CLS_LOG(5, "ERROR: cls_rgw_gc_queue_init: failed to decode entry\n");
    return -EINVAL;
  }

  cls_rgw_gc_urgent_data urgent_data;
  urgent_data.num_urgent_data_entries = op.num_deferred_entries;

  cls_queue_init_op init_op;

  CLS_LOG(10, "INFO: cls_rgw_gc_queue_init: queue size is %lu\n", op.size);

  init_op.queue_size = op.size;
  init_op.max_urgent_data_size = 0;
  encode(urgent_data, init_op.bl_urgent_data);

  return queue_init(hctx, init_op);
}

static int cls_rgw_gc_queue_enqueue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();
  cls::rgw::gc_datum::set_request op;
  if (!try_decode(op, in_iter)) {
    CLS_LOG(1, "ERROR: cls_rgw_gc_queue_enqueue: failed to decode entry\n");
    return -EINVAL;
  }

  op.info.time = ceph::real_clock::now();
  op.info.time += make_timespan(op.expiration_secs);

  //get head
  cls_queue_head head;
  int ret = queue_read_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_queue_enqueue_op enqueue_op;
  bufferlist bl_data;
  encode(op.info, bl_data);
  enqueue_op.bl_data_vec.emplace_back(bl_data);

  CLS_LOG(20, "INFO: cls_rgw_gc_queue_enqueue: Data size is: %u \n", bl_data.length());

  ret = queue_enqueue(hctx, enqueue_op, head);
  if (ret < 0) {
    return ret;
  }

  //Write back head
  return queue_write_head(hctx, head);
}

static int cls_rgw_gc_queue_list_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();
  cls::rgw::gc_datum::list_request op;
  if (!try_decode(op, in_iter)) {
    CLS_LOG(5, "ERROR: cls_rgw_gc_queue_list_entries(): failed to decode input\n");
    return -EINVAL;
  }

  cls_queue_head head;
  auto ret = queue_read_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_rgw_gc_urgent_data urgent_data;
  if (head.bl_urgent_data.length() > 0) {
    auto iter_urgent_data = head.bl_urgent_data.cbegin();
    if (!try_decode(urgent_data, iter_urgent_data)) {
      CLS_LOG(5, "ERROR: cls_rgw_gc_queue_list_entries(): failed to decode urgent data\n");
      return -EINVAL;
    }
  }

  if (!op.max) {
    op.max = GC_LIST_DEFAULT_MAX;
  }

  namespace datum = ceph::salad::datum;
  urgent_data_xattr xattr(hctx);
  cls_queue_list_op list_op;
  list_op.max = op.max;
  list_op.start_marker = op.marker;
  uint32_t remaining = op.max;
  cls::rgw::gc_datum::reply list_ret;
  bool is_truncated = true;
  string next_marker;
  for (;;) {
    cls_queue_list_ret page;
    const int status = queue_list_entries(hctx, list_op, page, head);
    if (status < 0) {
      return status;
    }
    is_truncated = page.is_truncated;
    next_marker = page.next_marker;
    const auto result = datum::visit_range(page.entries, [&](const auto& row)
        -> std::expected<datum::acceptance, int> {
      cls_rgw_gc_obj_info info;
      if (!try_decode(info, row.data)) {
        return std::unexpected(-EINVAL);
      }

      auto* entries = &urgent_data.urgent_data_map;
      auto entry = entries->find(info.tag);
      if (entry == entries->end() && urgent_data.num_xattr_urgent_entries > 0) {
        auto loaded = xattr.read();
        if (!loaded) {
          return std::unexpected(loaded.error());
        }
        entries = &loaded->get();
        entry = entries->find(info.tag);
      }
      if (entry != entries->end() && entry->second > info.time) {
        return datum::acceptance::skip;
      }
      if (!op.expired_only || info.time <= ceph::real_clock::now()) {
        list_ret.entries.emplace_back(std::move(info));
      }

      // The native quota counts nondeferred rows, even when expired_only excludes them.
      return datum::acceptance::accept;
    });
    if (!result) {
      return result.error().error;
    }
    remaining -= result->accepted;
    if (remaining == 0 || !is_truncated || page.entries.empty()) {
      break;
    }

    list_op.max = remaining;
    list_op.start_marker = next_marker;
    out->clear();
  }

  list_ret.truncated = is_truncated;
  if (list_ret.truncated) {
    list_ret.next_marker = next_marker;
  }
  out->clear();
  encode(list_ret, *out);
  return 0;
}

static int cls_rgw_gc_queue_remove_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls::rgw::queued_gc_datum::remove_request op;
  if (!try_decode(op, in_iter)) {
    CLS_LOG(5, "ERROR: cls_rgw_gc_queue_remove_entries(): failed to decode input\n");
    return -EINVAL;
  }

  cls_queue_head head;
  auto ret = queue_read_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_rgw_gc_urgent_data urgent_data;
  if (head.bl_urgent_data.length() > 0) {
    auto iter_urgent_data = head.bl_urgent_data.cbegin();
    if (!try_decode(urgent_data, iter_urgent_data)) {
      CLS_LOG(5, "ERROR: cls_rgw_gc_queue_remove_entries(): failed to decode urgent data\n");
      return -EINVAL;
    }
  }

  if (!op.num_entries) {
    op.num_entries = GC_LIST_DEFAULT_MAX;
  }

  namespace datum = ceph::salad::datum;
  urgent_data_xattr xattr(hctx);
  bool xattr_changed = false;
  uint64_t remaining = op.num_entries;
  cls_queue_list_op list_op;
  string end_marker;
  for (;;) {
    // Saturate the extra lookahead row at the request's native 64-bit limit.
    list_op.max = remaining + (remaining < std::numeric_limits<uint64_t>::max());
    cls_queue_list_ret page;
    const int status = queue_list_entries(hctx, list_op, page, head);
    if (status < 0) {
      return status;
    }
    if (page.entries.empty()) {
      break;
    }

    const auto result = datum::visit_range(page.entries, [&](const auto& row)
        -> std::expected<datum::acceptance, int> {
      cls_rgw_gc_obj_info info;
      if (!try_decode(info, row.data)) {
        return std::unexpected(-EINVAL);
      }

      auto* entries = &urgent_data.urgent_data_map;
      auto entry = entries->find(info.tag);
      if (entry == entries->end() && urgent_data.num_xattr_urgent_entries > 0) {
        auto loaded = xattr.read();
        if (!loaded) {
          return std::unexpected(loaded.error());
        }
        entries = &loaded->get();
        entry = entries->find(info.tag);
        urgent_data.num_xattr_urgent_entries = entries->size();
      }
      if (entry != entries->end() && entry->second > info.time) {
        return datum::acceptance::skip;
      }
      if (remaining == 0) {
        // The queue removes strictly before this row. Its metadata must survive.
        end_marker = row.marker;
        return datum::acceptance::reject_and_stop;
      }

      --remaining;
      if (entry != entries->end() && entry->second == info.time) {
        entries->erase(entry);
        if (entries == &urgent_data.urgent_data_map) {
          urgent_data.num_head_urgent_entries = entries->size();
        }
        if (entries != &urgent_data.urgent_data_map) {
          urgent_data.num_xattr_urgent_entries = entries->size();
          xattr_changed = true;
        }
      }

      return datum::acceptance::accept;
    });
    if (!result) {
      return result.error().error;
    }
    if (result->stopped()) {
      break;
    }
    if (!page.is_truncated) {
      end_marker = page.next_marker;
      break;
    }

    list_op.start_marker = page.next_marker;
    out->clear();
  }

  if (!end_marker.empty()) {
    cls_queue_remove_op remove;
    remove.end_marker = end_marker;
    const int status = queue_remove_entries(hctx, remove, head);
    if (status < 0) {
      return status;
    }
  }
  if (xattr_changed) {
    const int status = xattr.write();
    if (status < 0) {
      return status;
    }
  }

  //Update urgent data map
  head.bl_urgent_data.clear();
  encode(urgent_data, head.bl_urgent_data);
  CLS_LOG(5, "INFO: cls_rgw_gc_queue_remove_entries(): Urgent data size is %u\n", head.bl_urgent_data.length());

  return queue_write_head(hctx, head);
}

static int cls_rgw_gc_queue_update_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int ret = 0;
  auto in_iter = in->cbegin();

  cls::rgw::gc_datum::set_request op;
  if (!try_decode(op, in_iter)) {
    CLS_LOG(5, "ERROR: cls_rgw_gc_queue_update_entry(): failed to decode input\n");
    return -EINVAL;
  }

  op.info.time = ceph::real_clock::now();
  op.info.time += make_timespan(op.expiration_secs);

  // Read head
  cls_queue_head head;
  ret = queue_read_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  auto bl_iter = head.bl_urgent_data.cbegin();
  cls_rgw_gc_urgent_data urgent_data;
  if (!try_decode(urgent_data, bl_iter)) {
    CLS_LOG(5, "ERROR: cls_rgw_gc_queue_update_entry(): failed to decode urgent data\n");
    return -EINVAL;
  }

  urgent_data_xattr xattr(hctx);
  bool encode_head = false;
  const auto update = [&]() -> int {
    auto entry = urgent_data.urgent_data_map.find(op.info.tag);
    if (entry != urgent_data.urgent_data_map.end()) {
      entry->second = op.info.time;
      urgent_data.num_head_urgent_entries = urgent_data.urgent_data_map.size();
      encode_head = true;
      return 0;
    }

    // Defer also finds legacy xattr entries whose stored count is zero.
    auto loaded = xattr.read();
    if (!loaded) {
      return loaded.error();
    }
    auto& entries = loaded->get();
    encode_head = urgent_data.num_xattr_urgent_entries != entries.size();
    urgent_data.num_xattr_urgent_entries = entries.size();
    const auto overflow_entry = entries.find(op.info.tag);
    if (overflow_entry != entries.end()) {
      overflow_entry->second = op.info.time;
      return xattr.write();
    }

    urgent_data.urgent_data_map.emplace(op.info.tag, op.info.time);
    urgent_data.num_head_urgent_entries = urgent_data.urgent_data_map.size();
    bufferlist candidate;
    encode(urgent_data, candidate);
    if (candidate.length() <= head.max_urgent_data_size) {
      head.bl_urgent_data = std::move(candidate);
      encode_head = false;
      return 0;
    }

    urgent_data.urgent_data_map.erase(op.info.tag);
    urgent_data.num_head_urgent_entries = urgent_data.urgent_data_map.size();
    entries.emplace(op.info.tag, op.info.time);
    urgent_data.num_xattr_urgent_entries = entries.size();
    encode_head = true;
    return xattr.write();
  };
  const int status = update();
  if (status < 0) {
    return status;
  }

  if (uint64_t {urgent_data.num_head_urgent_entries} + urgent_data.num_xattr_urgent_entries >
      urgent_data.num_urgent_data_entries) {
    CLS_LOG(20, "Total num entries %u", urgent_data.num_urgent_data_entries);
    CLS_LOG(20, "Num xattr entries %u", urgent_data.num_xattr_urgent_entries);
    CLS_LOG(20, "Num head entries %u", urgent_data.num_head_urgent_entries);
    CLS_LOG(0, "ERROR: Number of urgent data entries exceeded that requested by user, returning no space!");
    return -ENOSPC;
  }

  std::string first_chain = "<empty-chain>";
  if (! op.info.chain.objs.empty()) {
    first_chain = op.info.chain.objs.cbegin()->key.name;
  }
  CLS_LOG(0,
	  "INFO: refrained from enqueueing GC entry during GC defer"
	  " tag=%s, first_chain=%s\n",
	  op.info.tag.c_str(), first_chain.c_str());

  if (encode_head) {
    head.bl_urgent_data.clear();
    encode(urgent_data, head.bl_urgent_data);
  }

  return queue_write_head(hctx, head);
}

CLS_INIT(rgw_gc)
{
  CLS_LOG(1, "Loaded rgw gc class!");

  cls_handle_t h_class;

  using namespace cls::rgw_gc;
  cls_register(ClassId::name, &h_class);
  ClassRegistrar<ClassId> cls(h_class);

  cls.register_cxx_method(method::init,           cls_rgw_gc_queue_init);
  cls.register_cxx_method(method::enqueue,        cls_rgw_gc_queue_enqueue);
  cls.register_cxx_method(method::list_entries,   cls_rgw_gc_queue_list_entries);
  cls.register_cxx_method(method::remove_entries, cls_rgw_gc_queue_remove_entries);
  cls.register_cxx_method(method::update_entry,   cls_rgw_gc_queue_update_entry);

  return;
}

