// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include <errno.h>

#include "objclass/objclass.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_rgw_queue_types.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_rgw_queue_ops.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/queue/cls_queue_src.h"

#include <boost/lexical_cast.hpp>
#include <unordered_map>
#include <sstream>

#include "common/ceph_context.h"
#include "global/global_context.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#define GC_LIST_DEFAULT_MAX 128

CLS_VER(1,0)
CLS_NAME(rgw_queue)

static int cls_gc_init_queue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_gc_init_queue_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_init_queue: failed to decode entry\n");
    return -EINVAL;
  }

  cls_gc_urgent_data urgent_data;
  urgent_data.num_urgent_data_entries = op.num_urgent_data_entries;

  cls_queue_init_op init_op;

  CLS_LOG(10, "INFO: cls_gc_init_queue: queue size is %lu\n", op.size);
  CLS_LOG(10, "INFO: cls_gc_init_queue: queue name is %s\n", op.name.c_str());
  init_op.head.queue_size = op.size;
  init_op.head_size = g_ceph_context->_conf->rgw_gc_queue_head_size;
  init_op.has_urgent_data = true;
  encode(urgent_data, init_op.head.bl_urgent_data);

  return init_queue(hctx, init_op);
}

static int cls_gc_enqueue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();
  cls_rgw_gc_set_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_enqueue: failed to decode entry\n");
    return -EINVAL;
  }

  op.info.time = ceph::real_clock::now();
  op.info.time += make_timespan(op.expiration_secs);

  //get head
  cls_queue_head head;
  int ret = get_queue_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_queue_enqueue_op enqueue_op;
  bufferlist bl_data;
  encode(op.info, bl_data);
  enqueue_op.bl_data_vec.emplace_back(bl_data);

  CLS_LOG(1, "INFO: cls_gc_enqueue: Data size is: %u \n", bl_data.length());

  ret = enqueue(hctx, enqueue_op, head);
  if (ret < 0) {
    return ret;
  }

  //Write back head
  return write_queue_head(hctx, head);
}

static int cls_gc_queue_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(1, "INFO: cls_gc_queue_list(): Entered cls_gc_queue_list \n");
  auto in_iter = in->cbegin();
  cls_rgw_gc_list_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode input\n");
    return -EINVAL;
  }

  cls_queue_head head;
  auto ret = get_queue_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_gc_urgent_data urgent_data;
  if (head.bl_urgent_data.length() > 0) {
    auto iter_urgent_data = head.bl_urgent_data.cbegin();
    try {
      decode(urgent_data, iter_urgent_data);
    } catch (buffer::error& err) {
      CLS_LOG(5, "ERROR: cls_gc_queue_list(): failed to decode urgent data\n");
      return -EINVAL;
    }
  }

  cls_queue_list_op list_op;
  if (! op.max) {
    op.max = GC_LIST_DEFAULT_MAX;
  }
  
  list_op.max = op.max;
  list_op.start_marker = op.marker;

  cls_rgw_gc_list_ret list_ret;
  uint32_t num_entries = 0; //Entries excluding the deferred ones
  bool is_truncated = true;
  string next_marker;
  do {
    CLS_LOG(1, "INFO: cls_gc_queue_list(): Entering queue_list_entries \n");
    cls_queue_list_ret op_ret;
    int ret = queue_list_entries(hctx, list_op, op_ret, head);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: queue_list_entries(): returned error %d\n", ret);
      return ret;
    }
    is_truncated = op_ret.is_truncated;
    next_marker = op_ret.next_marker;
  
    if (op_ret.data.size()) {
      for (auto it : op_ret.data) {
        cls_rgw_gc_obj_info info;
        try {
          decode(info, it);
        } catch (buffer::error& err) {
          CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode gc info\n");
          return -EINVAL;
        }
        bool found = false;
        //Check for info tag in urgent data map
        auto iter = urgent_data.urgent_data_map.find(info.tag);
        if (iter != urgent_data.urgent_data_map.end()) {
          found = true;
          stringstream ss1, ss2;
          ss1 << iter->second;
          ss2 << info.time;
          if (iter->second > info.time) {
            CLS_LOG(1, "INFO: cls_gc_queue_list(): tag found in urgent data: %s\n", info.tag.c_str());
            CLS_LOG(1, "INFO: cls_gc_queue_list(): time found in urgent data: %s\n", ss1.str().c_str());
            CLS_LOG(1, "INFO: cls_gc_queue_list(): time found in queue data: %s\n", ss2.str().c_str());
            continue;
          }
        }
        //Search in xattrs
        if (! found && urgent_data.num_xattr_urgent_entries > 0) {
          bufferlist bl_xattrs;
          int ret = cls_cxx_getxattr(hctx, "cls_queue_urgent_data", &bl_xattrs);
          if (ret < 0 && (ret != -ENOENT && ret != -ENODATA)) {
            CLS_LOG(0, "ERROR: %s(): cls_cxx_getxattrs() returned %d", __func__, ret);
            return ret;
          }
          if (ret != -ENOENT && ret != -ENODATA) {
            std::unordered_map<string,ceph::real_time> xattr_urgent_data_map;
            auto iter = bl_xattrs.cbegin();
            try {
              decode(xattr_urgent_data_map, iter);
            } catch (buffer::error& err) {
              CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode xattrs urgent data map\n");
              return -EINVAL;
            } //end - catch
            auto xattr_iter = xattr_urgent_data_map.find(info.tag);
            if (xattr_iter != xattr_urgent_data_map.end()) {
              if (xattr_iter->second > info.time) {
                CLS_LOG(1, "INFO: cls_gc_queue_list(): tag found in xattrs urgent data map: %s\n", info.tag.c_str());
                continue;
              }
            }
          } // end - ret != ENOENT && ENODATA
        } // end - if not found
        if (op.expired_only) {
          real_time now = ceph::real_clock::now();
          if (info.time <= now) {
            list_ret.entries.emplace_back(info);
          }
          //Can break out here if info.time > now, since all subsequent entries won't have expired
        } else {
          list_ret.entries.emplace_back(info);
        }
        num_entries++;
      }
      CLS_LOG(1, "INFO: cls_gc_queue_list(): num_entries: %u and op.max: %u\n", num_entries, op.max);
      if (num_entries < op.max) {
        list_op.max = (op.max - num_entries);
        list_op.start_marker = op_ret.next_marker;
        out->clear();
      } else {
        //We've reached the max number of entries needed
        break;
      }
    } else {
      //We dont have data to process
      break;
    }
  } while(is_truncated);

  list_ret.truncated = is_truncated;
  if (list_ret.truncated) {
    list_ret.next_marker = next_marker;
  }
  out->clear();
  encode(list_ret, *out);
  return 0;
}

static int cls_gc_queue_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(1, "INFO: cls_gc_queue_remove(): Entered cls_gc_queue_remove \n");

  auto in_iter = in->cbegin();

  cls_rgw_gc_queue_remove_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_remove(): failed to decode input\n");
    return -EINVAL;
  }

  cls_queue_head head;
  auto ret = get_queue_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_gc_urgent_data urgent_data;
  if (head.bl_urgent_data.length() > 0) {
    auto iter_urgent_data = head.bl_urgent_data.cbegin();
    try {
      decode(urgent_data, iter_urgent_data);
    } catch (buffer::error& err) {
      CLS_LOG(5, "ERROR: cls_gc_queue_remove(): failed to decode urgent data\n");
      return -EINVAL;
    }
  }

  // List entries and calculate total number of entries (including invalid entries)
  if (! op.num_entries) {
    op.num_entries = GC_LIST_DEFAULT_MAX;
  }
  cls_queue_list_op list_op;
  list_op.max = op.num_entries + 1; // +1 to get the offset of last + 1 entry
  bool is_truncated = true;
  uint32_t total_num_entries = 0, num_entries = 0;
  string end_marker;
  do {
    CLS_LOG(1, "INFO: cls_gc_queue_remove(): Entering queue_list_entries \n");
    cls_queue_list_ret op_ret;
    int ret = queue_list_entries(hctx, list_op, op_ret, head);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: queue_list_entries(): returned error %d\n", ret);
      return ret;
    }

    is_truncated = op_ret.is_truncated;
    unsigned int index = 0;
    // If data is not empty
    if (op_ret.data.size()) {
      for (auto it : op_ret.data) {
        cls_rgw_gc_obj_info info;
        try {
          decode(info, it);
        } catch (buffer::error& err) {
          CLS_LOG(1, "ERROR: cls_gc_queue_remove(): failed to decode gc info\n");
          return -EINVAL;
        }
        CLS_LOG(1, "INFO: cls_gc_queue_remove(): entry: %s\n", info.tag.c_str());
        total_num_entries++;
        index++;
        bool found = false;
        //Search for tag in urgent data map
        auto iter = urgent_data.urgent_data_map.find(info.tag);
        if (iter != urgent_data.urgent_data_map.end()) {
          found = true;
          if (iter->second > info.time) {
            CLS_LOG(1, "INFO: cls_gc_queue_remove(): tag found in urgent data: %s\n", info.tag.c_str());
            continue;
          } else if (iter->second == info.time) {
            CLS_LOG(1, "INFO: cls_gc_queue_remove(): erasing tag from urgent data: %s\n", info.tag.c_str());
            urgent_data.urgent_data_map.erase(info.tag); //erase entry from map, as it will be removed later from queue
            urgent_data.num_head_urgent_entries -= 1;
          }
        }//end-if map end
        if (! found && urgent_data.num_xattr_urgent_entries > 0) {
          //Search in xattrs
          bufferlist bl_xattrs;
          int ret = cls_cxx_getxattr(hctx, "cls_queue_urgent_data", &bl_xattrs);
          if (ret < 0 && (ret != -ENOENT && ret != -ENODATA)) {
            CLS_LOG(0, "ERROR: %s(): cls_cxx_getxattrs() returned %d", __func__, ret);
            return ret;
          }
          if (ret != -ENOENT && ret != -ENODATA) {
            std::unordered_map<string,ceph::real_time> xattr_urgent_data_map;
            auto iter = bl_xattrs.cbegin();
            try {
              decode(xattr_urgent_data_map, iter);
            } catch (buffer::error& err) {
              CLS_LOG(1, "ERROR: cls_gc_queue_remove(): failed to decode xattrs urgent data map\n");
              return -EINVAL;
            } //end - catch
            auto xattr_iter = xattr_urgent_data_map.find(info.tag);
            if (xattr_iter != xattr_urgent_data_map.end()) {
              if (xattr_iter->second > info.time) {
                CLS_LOG(1, "INFO: cls_gc_queue_remove(): tag found in xattrs urgent data map: %s\n", info.tag.c_str());
                continue;
              } else if (xattr_iter->second == info.time) {
                CLS_LOG(1, "INFO: cls_gc_queue_remove(): erasing tag from xattrs urgent data: %s\n", info.tag.c_str());
                xattr_urgent_data_map.erase(info.tag); //erase entry from map, as it will be removed later
                urgent_data.num_xattr_urgent_entries -= 1;
              }
            }
          } // end - ret != ENOENT && ENODATA
        }// search in xattrs
        num_entries++;
      }//end-for
      
      if (! op_ret.is_truncated && num_entries < (op.num_entries + 1)) {
        end_marker = op_ret.next_marker;
        CLS_LOG(1, "INFO: cls_gc_queue_remove(): truncated and end offset is %s\n", end_marker.c_str());
        break;
      }
      if (num_entries < (op.num_entries + 1)) {
        list_op.max = ((op.num_entries + 1) - num_entries);
        list_op.start_marker = op_ret.next_marker;
        out->clear();
      } else {
        end_marker = op_ret.markers[index - 1];
        CLS_LOG(1, "INFO: cls_gc_queue_remove(): index is %u and end_offset is: %s\n", index, end_marker.c_str());
        break;
      }
    } //end-if
    else {
      break;
    }
  } while(is_truncated);

  CLS_LOG(1, "INFO: cls_gc_queue_remove(): Total number of entries to remove: %d\n", total_num_entries);
  CLS_LOG(1, "INFO: cls_gc_queue_remove(): End offset is %s\n", end_marker.c_str());

  if (! end_marker.empty()) {
    cls_queue_remove_op rem_op;
    rem_op.end_marker = end_marker;

    CLS_LOG(1, "INFO: cls_gc_queue_remove(): Entering queue_remove_entries \n");
    int ret = queue_remove_entries(hctx, rem_op, head);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: queue_remove_entries(): returned error %d\n", ret);
      return ret;
    }
  }

  //Update urgent data map
  encode(urgent_data, head.bl_urgent_data);

  return write_queue_head(hctx, head);
}

static int cls_gc_queue_update_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int ret = 0;
  auto in_iter = in->cbegin();

  cls_gc_defer_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_update_entry(): failed to decode input\n");
    return -EINVAL;
  }

  op.info.time = ceph::real_clock::now();
  op.info.time += make_timespan(op.expiration_secs);

  // Read head
  cls_queue_head head;
  ret = get_queue_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  auto bl_iter = head.bl_urgent_data.cbegin();
  cls_gc_urgent_data urgent_data;
  try {
    decode(urgent_data, bl_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_update_entry(): failed to decode urgent data\n");
    return -EINVAL;
  }

  //has_urgent_data signifies whether urgent data in queue has changed
  bool has_urgent_data = false, tag_found = false;
  //search in unordered map in head
  auto it = urgent_data.urgent_data_map.find(op.info.tag);
  if (it != urgent_data.urgent_data_map.end()) {
    it->second = op.info.time;
    tag_found = true;
    has_urgent_data = true;
  } else { //search in xattrs
    bufferlist bl_xattrs;
    int ret = cls_cxx_getxattr(hctx, "cls_queue_urgent_data", &bl_xattrs);
    if (ret < 0 && (ret != -ENOENT && ret != -ENODATA)) {
      CLS_LOG(0, "ERROR: %s(): cls_cxx_getxattrs() returned %d", __func__, ret);
      return ret;
    }
    if (ret != -ENOENT && ret != -ENODATA) {
      std::unordered_map<string,ceph::real_time> xattr_urgent_data_map;
      auto iter = bl_xattrs.cbegin();
      try {
        decode(xattr_urgent_data_map, iter);
      } catch (buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_gc_queue_update_entry(): failed to decode xattrs urgent data map\n");
        return -EINVAL;
      } //end - catch
      auto xattr_iter = xattr_urgent_data_map.find(op.info.tag);
      if (xattr_iter != xattr_urgent_data_map.end()) {
        it->second = op.info.time;
        tag_found = true;
        //write the updated map back
        bufferlist bl_map;
        encode(xattr_urgent_data_map, bl_map);
        ret = cls_cxx_setxattr(hctx, "cls_queue_urgent_data", &bl_map);
        CLS_LOG(20, "%s(): setting attr: %s", __func__, "cls_queue_urgent_data");
        if (ret < 0) {
          CLS_LOG(0, "ERROR: %s(): cls_cxx_setxattr (attr=%s) returned %d", __func__, "cls_queue_urgent_data", ret);
          return ret;
        }
      }
    }// end ret != ENOENT ...
  }

  if (! tag_found) {
    stringstream ss1;
    ss1 << op.info.time;
    CLS_LOG(1, "INFO: cls_gc_queue_update_entry(): time inserted in urgent data: %s\n", ss1.str().c_str());
    //try inserting in queue head
    urgent_data.urgent_data_map.insert({op.info.tag, op.info.time});
    urgent_data.num_head_urgent_entries += 1;
    has_urgent_data = true;

    bufferlist bl_urgent_data;
    encode(urgent_data, bl_urgent_data);
    //insert as xattrs
    if (bl_urgent_data.length() > head.max_urgent_data_size) {
      //remove inserted entry from urgent data
      urgent_data.urgent_data_map.erase(op.info.tag);
      urgent_data.num_head_urgent_entries -= 1;
      has_urgent_data = false;

      bufferlist bl_xattrs;
      int ret = cls_cxx_getxattr(hctx, "cls_queue_urgent_data", &bl_xattrs);
      if (ret < 0 && (ret != -ENOENT && ret != -ENODATA)) {
        CLS_LOG(0, "ERROR: %s(): cls_cxx_getxattrs() returned %d", __func__, ret);
        return ret;
      }
      std::unordered_map<string,ceph::real_time> xattr_urgent_data_map;
      if (ret != -ENOENT && ret != -ENODATA) {
        auto iter = bl_xattrs.cbegin();
        try {
          decode(xattr_urgent_data_map, iter);
        } catch (buffer::error& err) {
          CLS_LOG(1, "ERROR: cls_gc_queue_remove(): failed to decode xattrs urgent data map\n");
          return -EINVAL;
        } //end - catch
      }
      xattr_urgent_data_map.insert({op.info.tag, op.info.time});
      urgent_data.num_xattr_urgent_entries += 1;
      has_urgent_data = true;
      bufferlist bl_map;
      encode(xattr_urgent_data_map, bl_map);
      ret = cls_cxx_setxattr(hctx, "cls_queue_urgent_data", &bl_map);
      CLS_LOG(20, "%s(): setting attr: %s", __func__, "cls_queue_urgent_data");
      if (ret < 0) {
        CLS_LOG(0, "ERROR: %s(): cls_cxx_setxattr (attr=%s) returned %d", __func__, "cls_queue_urgent_data", ret);
        return ret;
      }
    }
  }

  if ((urgent_data.num_head_urgent_entries + urgent_data.num_xattr_urgent_entries) > urgent_data.num_urgent_data_entries) {
    CLS_LOG(0, "Total num entries %u", urgent_data.num_urgent_data_entries);
    CLS_LOG(0, "Num xattr entries %u", urgent_data.num_xattr_urgent_entries);
    CLS_LOG(0, "Num head entries %u", urgent_data.num_head_urgent_entries);
    CLS_LOG(0, "ERROR: Number of urgent data entries exceeded that requested by user, returning no space!");
    return -ENOSPC;
  }

  cls_queue_enqueue_op enqueue_op;
  bufferlist bl_data;
  stringstream ss1;
  ss1 << op.info.time;
  CLS_LOG(1, "INFO: cls_gc_queue_update_entry(): time inserted in queue: %s\n", ss1.str().c_str());
  encode(op.info, bl_data);
  enqueue_op.bl_data_vec.emplace_back(bl_data);
  CLS_LOG(1, "INFO: cls_gc_update_entry: Data size is: %u \n", bl_data.length());
  
  ret = enqueue(hctx, enqueue_op, head);
  if (ret < 0) {
    return ret;
  }

  if (has_urgent_data) {
    head.bl_urgent_data.clear();
    encode(urgent_data, head.bl_urgent_data);
  }

  return write_queue_head(hctx, head);
}

CLS_INIT(rgw_queue)
{
  CLS_LOG(1, "Loaded rgw queue class!");

  cls_handle_t h_class;
  cls_method_handle_t h_gc_init_queue;
  cls_method_handle_t h_gc_enqueue;
  cls_method_handle_t h_gc_queue_list_entries;
  cls_method_handle_t h_gc_queue_remove_entries;
  cls_method_handle_t h_gc_queue_update_entry;

  cls_register(RGW_QUEUE_CLASS, &h_class);

  /* gc */
  cls_register_cxx_method(h_class, GC_INIT_QUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_init_queue, &h_gc_init_queue);
  cls_register_cxx_method(h_class, GC_ENQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_enqueue, &h_gc_enqueue);
  cls_register_cxx_method(h_class, GC_QUEUE_LIST_ENTRIES, CLS_METHOD_RD, cls_gc_queue_list, &h_gc_queue_list_entries);
  cls_register_cxx_method(h_class, GC_QUEUE_REMOVE_ENTRIES, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_queue_remove, &h_gc_queue_remove_entries);
  cls_register_cxx_method(h_class, GC_QUEUE_UPDATE_ENTRY, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_queue_update_entry, &h_gc_queue_update_entry);

  return;
}

