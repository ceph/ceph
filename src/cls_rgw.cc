#include <iostream>

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "include/types.h"
#include "include/utime.h"
#include "objclass/objclass.h"
#include "rgw/rgw_cls_api.h"
#include "common/Clock.h"

#include "global/global_context.h"

CLS_VER(1,0)
CLS_NAME(rgw)

cls_handle_t h_class;
cls_method_handle_t h_rgw_bucket_init_index;
cls_method_handle_t h_rgw_bucket_list;
cls_method_handle_t h_rgw_bucket_prepare_op;
cls_method_handle_t h_rgw_bucket_complete_op;
cls_method_handle_t h_rgw_dir_suggest_changes;


#define ROUND_BLOCK_SIZE 4096

static uint64_t get_rounded_size(uint64_t size)
{
  return (size + ROUND_BLOCK_SIZE - 1) & ~(ROUND_BLOCK_SIZE - 1);
}

int rgw_bucket_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator iter = in->begin();

  struct rgw_cls_list_op op;
  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG("ERROR: rgw_bucket_list(): failed to decode request\n");
    return -EINVAL;
  }

  struct rgw_cls_list_ret ret;
  struct rgw_bucket_dir& new_dir = ret.dir;
  bufferlist header_bl;
  int rc = cls_cxx_map_read_header(hctx, &header_bl);
  if (rc < 0)
    return rc;
  bufferlist::iterator header_iter = header_bl.begin();
  try {
    ::decode(new_dir.header, header_iter);
  } catch (buffer::error& err) {
    CLS_LOG("ERROR: rgw_bucket_list(): failed to decode header\n");
    return -EINVAL;
  }

  bufferlist bl;

  map<string, bufferlist> keys;
  rc = cls_cxx_map_read_keys(hctx, op.start_obj, op.filter_prefix, op.num_entries + 1, &keys);
  if (rc < 0)
    return rc;

  std::map<string, struct rgw_bucket_dir_entry>& m = new_dir.m;
  std::map<string, bufferlist>::iterator kiter = keys.begin();
  uint32_t i;

  for (i = 0; i < op.num_entries && kiter != keys.end(); ++i, ++kiter) {
    struct rgw_bucket_dir_entry entry;
    bufferlist& entrybl = kiter->second;
    bufferlist::iterator eiter = entrybl.begin();
    try {
      ::decode(entry, eiter);
    } catch (buffer::error& err) {
      CLS_LOG("ERROR: rgw_bucket_list(): failed to decode entry, key=%s\n", kiter->first.c_str());
      return -EINVAL;
    }
    
    m[kiter->first] = entry;
  }

  ret.is_truncated = (kiter != keys.end());

  ::encode(ret, *out);
  return 0;
}

int rgw_bucket_init_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  bufferlist::iterator iter;

  bufferlist header_bl;
  int rc = cls_cxx_map_read_header(hctx, &header_bl);
  if (rc < 0) {
    switch (rc) {
    case -ENODATA:
    case -ENOENT:
      break;
    default:
      return rc;
    }
  }

  if (header_bl.length() != 0) {
    CLS_LOG("ERROR: index already initialized\n");
    return -EINVAL;
  }

  rgw_bucket_dir dir;
  ::encode(dir.header, header_bl);
  rc = cls_cxx_map_write_header(hctx, &header_bl);
  return rc;
}

int rgw_bucket_prepare_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_obj_prepare_op op;
  bufferlist::iterator iter = in->begin();
  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG("ERROR: rgw_bucket_prepare_op(): failed to decode request\n");
    return -EINVAL;
  }

  if (op.tag.empty()) {
    CLS_LOG("ERROR: tag is empty\n");
    return -EINVAL;
  }

  CLS_LOG("rgw_bucket_prepare_op(): request: op=%d name=%s tag=%s\n", op.op, op.name.c_str(), op.tag.c_str());

  // get on-disk state
  bufferlist cur_value;
  int rc = cls_cxx_map_read_key(hctx, op.name, &cur_value);
  if (rc < 0 && rc != -ENOENT)
    return rc;

  struct rgw_bucket_dir_entry entry;

  bool noent = (rc == -ENOENT);

  rc = 0;

  if (!noent) {
    try {
      bufferlist::iterator biter = cur_value.begin();
      ::decode(entry, biter);
    } catch (buffer::error& err) {
      CLS_LOG("ERROR: rgw_bucket_prepare_op(): failed to decode entry\n");
      /* ignoring error */

      noent = true;
    }
  }

  if (noent) { // no entry, initialize fields
    entry.name = op.name;
    entry.epoch = 0;
    entry.exists = false;
    entry.locator = op.locator;
  }

  // fill in proper state
  struct rgw_bucket_pending_info& info = entry.pending_map[op.tag];
  info.timestamp = ceph_clock_now(g_ceph_context);
  info.state = CLS_RGW_STATE_PENDING_MODIFY;
  info.op = op.op;

  // write out new key to disk
  bufferlist info_bl;
  ::encode(entry, info_bl);
  cls_cxx_map_write_key(hctx, op.name, &info_bl);
  return rc;
}

int rgw_bucket_complete_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_obj_complete_op op;
  bufferlist::iterator iter = in->begin();
  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG("ERROR: rgw_bucket_complete_op(): failed to decode request\n");
    return -EINVAL;
  }
  CLS_LOG("rgw_bucket_complete_op(): request: op=%d name=%s epoch=%lld tag=%s\n", op.op, op.name.c_str(), op.epoch, op.tag.c_str());

  bufferlist header_bl;
  struct rgw_bucket_dir_header header;
  int rc = cls_cxx_map_read_header(hctx, &header_bl);
  if (rc < 0)
    return rc;
  bufferlist::iterator header_iter = header_bl.begin();
  try {
    ::decode(header, header_iter);
  } catch (buffer::error& err) {
    CLS_LOG("ERROR: rgw_bucket_complete_op(): failed to decode header\n");
    return -EINVAL;
  }

  bufferlist current_entry;
  struct rgw_bucket_dir_entry entry;
  bool ondisk = true;
  rc = cls_cxx_map_read_key(hctx, op.name, &current_entry);
  if (rc < 0) {
    if (rc != -ENOENT) {
      return rc;
    } else {
      rc = 0;
      entry.name = op.name;
      entry.epoch = op.epoch;
      entry.meta = op.meta;
      entry.locator = op.locator;
      ondisk = false;
    }
  } else {
    bufferlist::iterator cur_iter = current_entry.begin();
    try {
      ::decode(entry, cur_iter);
      CLS_LOG("rgw_bucket_complete_op(): existing entry: epoch=%lld name=%s locator=%s\n", entry.epoch, entry.name.c_str(), entry.locator.c_str());
    } catch (buffer::error& err) {
      CLS_LOG("ERROR: rgw_bucket_complete_op(): failed to decode entry\n");
    }
  }

  if (op.tag.size()) {
    map<string, struct rgw_bucket_pending_info>::iterator pinter = entry.pending_map.find(op.tag);
    if (pinter == entry.pending_map.end()) {
      CLS_LOG("ERROR: couldn't find tag for pending operation\n");
      return -EINVAL;
    }
    entry.pending_map.erase(pinter);
  }

  bool cancel = false;
  bufferlist update_bl;

  if (op.tag.size() && op.op == CLS_RGW_OP_CANCEL) {
    CLS_LOG("rgw_bucket_complete_op(): cancel requested\n");
    cancel = true;
  } else if (op.epoch <= entry.epoch) {
    CLS_LOG("rgw_bucket_complete_op(): skipping request, old epoch\n");
    cancel = true;
  }

  bufferlist op_bl;
  if (cancel) {
    if (op.tag.size()) {
      bufferlist new_key_bl;
      ::encode(entry, new_key_bl);
      return cls_cxx_map_write_key(hctx, op.name, &new_key_bl);
    } else {
      return 0;
    }
  }

  if (entry.exists) {
    struct rgw_bucket_category_stats& stats = header.stats[entry.meta.category];
    stats.num_entries--;
    stats.total_size -= entry.meta.size;
    stats.total_size_rounded -= get_rounded_size(entry.meta.size);
  }

  switch (op.op) {
  case CLS_RGW_OP_DEL:
    if (ondisk) {
      if (!entry.pending_map.size()) {
	int ret = cls_cxx_map_remove_key(hctx, op.name);
	if (ret < 0)
	  return ret;
      } else {
        entry.exists = false;
        bufferlist new_key_bl;
        ::encode(entry, new_key_bl);
	int ret = cls_cxx_map_write_key(hctx, op.name, &new_key_bl);
	if (ret < 0)
	  return ret;
      }
    } else {
      return -ENOENT;
    }
    break;
  case CLS_RGW_OP_ADD:
    {
      struct rgw_bucket_dir_entry_meta& meta = op.meta;
      struct rgw_bucket_category_stats& stats = header.stats[meta.category];
      entry.meta = meta;
      entry.name = op.name;
      entry.epoch = op.epoch;
      entry.exists = true;
      stats.num_entries++;
      stats.total_size += meta.size;
      stats.total_size_rounded += get_rounded_size(meta.size);
      bufferlist new_key_bl;
      ::encode(entry, new_key_bl);
      int ret = cls_cxx_map_write_key(hctx, op.name, &new_key_bl);
      if (ret < 0)
	return ret;
    }
    break;
  }

  bufferlist new_header_bl;
  ::encode(header, new_header_bl);
  return cls_cxx_map_write_header(hctx, &new_header_bl);
}

int rgw_dir_suggest_changes(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG("rgw_dir_suggest_changes()");

  bufferlist header_bl;
  struct rgw_bucket_dir_header header;
  bool header_changed = false;
  int rc = cls_cxx_map_read_header(hctx, &header_bl);
  if (rc < 0)
    return rc;

  try {
    bufferlist::iterator header_iter = header_bl.begin();
    ::decode(header, header_iter);
  } catch (buffer::error& error) {
    CLS_LOG("ERROR: rgw_dir_suggest_changes(): failed to decode header\n");
    return -EINVAL;
  }

  bufferlist::iterator in_iter = in->begin();
  __u8 op;
  rgw_bucket_dir_entry cur_change;
  rgw_bucket_dir_entry cur_disk;
  bufferlist op_bl;

  while (!in_iter.end()) {
    try {
      ::decode(op, in_iter);
      ::decode(cur_change, in_iter);
    } catch (buffer::error& err) {
      CLS_LOG("ERROR: rgw_dir_suggest_changes(): failed to decode request\n");
      return -EINVAL;
    }

    bufferlist cur_disk_bl;
    int ret = cls_cxx_map_read_key(hctx, cur_change.name, &cur_disk_bl);
    if (ret < 0 && ret != -ENOENT)
      return -EINVAL;

    if (cur_disk_bl.length()) {
      bufferlist::iterator cur_disk_iter = cur_disk_bl.begin();
      try {
        ::decode(cur_disk, cur_disk_iter);
      } catch (buffer::error& error) {
        CLS_LOG("ERROR: rgw_dir_suggest_changes(): failed to decode cur_disk\n");
        return -EINVAL;
      }

      utime_t cur_time = ceph_clock_now(g_ceph_context);
      map<string, struct rgw_bucket_pending_info>::iterator iter =
                cur_disk.pending_map.begin();
      while(iter != cur_disk.pending_map.end()) {
        map<string, struct rgw_bucket_pending_info>::iterator cur_iter=iter++;
        if (cur_time > (cur_iter->second.timestamp + CEPH_RGW_TAG_TIMEOUT)) {
          cur_disk.pending_map.erase(cur_iter);
        }
      }
    }

    if (cur_disk.pending_map.empty()) {
      struct rgw_bucket_category_stats& stats =
          header.stats[cur_disk.meta.category];
      if (cur_disk.exists) {
        stats.num_entries--;
        stats.total_size -= cur_disk.meta.size;
        stats.total_size_rounded -= get_rounded_size(cur_disk.meta.size);
        header_changed = true;
      }
      switch(op) {
      case CEPH_RGW_REMOVE:
	ret = cls_cxx_map_remove_key(hctx, cur_change.name);
	if (ret < 0)
	  return ret;
        break;
      case CEPH_RGW_UPDATE:
        stats.num_entries++;
        stats.total_size += cur_change.meta.size;
        stats.total_size_rounded += get_rounded_size(cur_change.meta.size);
        bufferlist cur_state_bl;
        ::encode(cur_change, cur_state_bl);
        ret = cls_cxx_map_write_key(hctx, cur_change.name, &cur_state_bl);
        if (ret < 0)
	  return ret;
        break;
      }
    }
  }

  bufferlist update_bl;
  if (header_changed) {
    bufferlist new_header_bl;
    ::encode(header, new_header_bl);
    return cls_cxx_map_write_header(hctx, &new_header_bl);
  }
  return 0;
}

void __cls_init()
{
  CLS_LOG("Loaded rgw class!");

  cls_register("rgw", &h_class);
  cls_register_cxx_method(h_class, "bucket_init_index", CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC, rgw_bucket_init_index, &h_rgw_bucket_init_index);
  cls_register_cxx_method(h_class, "bucket_list", CLS_METHOD_RD | CLS_METHOD_PUBLIC, rgw_bucket_list, &h_rgw_bucket_list);
  cls_register_cxx_method(h_class, "bucket_prepare_op", CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC, rgw_bucket_prepare_op, &h_rgw_bucket_prepare_op);
  cls_register_cxx_method(h_class, "bucket_complete_op", CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC, rgw_bucket_complete_op, &h_rgw_bucket_complete_op);
  cls_register_cxx_method(h_class, "dir_suggest_changes", CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC, rgw_dir_suggest_changes, &h_rgw_dir_suggest_changes);

  return;
}

