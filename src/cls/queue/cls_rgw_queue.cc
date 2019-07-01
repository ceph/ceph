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
#include "cls/queue/cls_queue.h"

#include <boost/lexical_cast.hpp>
#include <unordered_map>

#include "common/ceph_context.h"
#include "global/global_context.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#define GC_LIST_DEFAULT_MAX 128

CLS_VER(1,0)
CLS_NAME(rgw_queue)

static int cls_gc_create_queue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_gc_create_queue_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_create_queue: failed to decode entry\n");
    return -EINVAL;
  }

  cls_gc_urgent_data urgent_data;
  urgent_data.num_urgent_data_entries = op.num_urgent_data_entries;

  cls_create_queue_op create_op;

  CLS_LOG(10, "INFO: cls_gc_create_queue: queue size is %lu\n", op.size);
  CLS_LOG(10, "INFO: cls_gc_create_queue: queue name is %s\n", op.name.c_str());
  create_op.head.queue_size = op.size;
  create_op.head_size = g_ceph_context->_conf->rgw_gc_queue_head_size;
  create_op.head.has_urgent_data = true;
  encode(urgent_data, create_op.head.bl_urgent_data);

  in->clear();
  encode(create_op, *in);

  return cls_create_queue(hctx, in, out);
}

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

  cls_init_queue_op init_op;

  CLS_LOG(10, "INFO: cls_gc_init_queue: queue size is %lu\n", op.size);
  CLS_LOG(10, "INFO: cls_gc_init_queue: queue name is %s\n", op.name.c_str());
  init_op.head.queue_size = op.size;
  init_op.head_size = g_ceph_context->_conf->rgw_gc_queue_head_size;
  init_op.head.has_urgent_data = true;
  encode(urgent_data, init_op.head.bl_urgent_data);

  in->clear();
  encode(init_op, *in);

  return cls_init_queue(hctx, in, out);
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

  cls_enqueue_op enqueue_op;
  bufferlist bl_data;
  encode(op.info, bl_data);
  enqueue_op.bl_data_vec.emplace_back(bl_data);

  CLS_LOG(1, "INFO: cls_gc_enqueue: Data size is: %u \n", bl_data.length());

  in->clear();
  encode(enqueue_op, *in);

  return cls_enqueue(hctx, in, out);
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

  cls_queue_list_op list_op;
  if (op.marker.empty()) {
    list_op.start_offset = 0;
  } else {
    list_op.start_offset = boost::lexical_cast<uint64_t>(op.marker.c_str());
  }

  if (! op.max) {
    op.max = GC_LIST_DEFAULT_MAX;
  }
  
  list_op.max = op.max;

  cls_queue_list_ret op_ret;
  cls_rgw_gc_list_ret list_ret;
  uint32_t num_entries = 0; //Entries excluding the deferred ones
  bool urgent_data_decoded = false;
  cls_gc_urgent_data urgent_data;
  do {
    in->clear();
    encode(list_op, *in);

    CLS_LOG(1, "INFO: cls_gc_queue_list(): Entering cls_queue_list_entries \n");
    int ret = cls_queue_list_entries(hctx, in, out);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_list_entries(): returned error %d\n", ret);
      return ret;
    }

    auto iter = out->cbegin();
    try {
      decode(op_ret, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode output\n");
      return -EINVAL;
    }

    //Each cls_queue_list_entries will fetch the same urgent data, decode it only once
    if (! urgent_data_decoded) {
        auto iter_urgent_data = op_ret.bl_urgent_data.cbegin();
        try {
          decode(urgent_data, iter_urgent_data);
          urgent_data_decoded = true;
        } catch (buffer::error& err) {
          CLS_LOG(5, "ERROR: cls_gc_queue_list(): failed to decode urgent data\n");
          return -EINVAL;
        }
    }

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
        if (urgent_data.urgent_data_map.size() > 0) {
          auto it = urgent_data.urgent_data_map.find(info.tag);
          if (it != urgent_data.urgent_data_map.end()) {
            found = true;
            if (it->second > info.time) {
              CLS_LOG(1, "INFO: cls_gc_queue_list(): tag found in urgent data: %s\n", info.tag.c_str());
              continue;
            }
          }
        }
        //Search in xattrs
        if (! found) {
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
            if (xattr_urgent_data_map.size() > 0) {
              auto it = xattr_urgent_data_map.find(info.tag);
              if (it != xattr_urgent_data_map.end()) {
                if (it->second > info.time) {
                  CLS_LOG(1, "INFO: cls_gc_queue_list(): tag found in xattrs urgent data map: %s\n", info.tag.c_str());
                  continue;
                }
              }
            } // end - if xattrs size ...
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
        list_op.start_offset = op_ret.next_offset;
        out->clear();
      } else {
        //We've reached the max number of entries needed
        break;
      }
    } else {
      //We dont have data to process
      break;
    }
  } while(op_ret.is_truncated);

  list_ret.truncated = op_ret.is_truncated;
  if (list_ret.truncated) {
    list_ret.next_marker = boost::lexical_cast<string>(op_ret.next_offset);
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

  // List entries and calculate total number of entries (including invalid entries)
  cls_queue_list_op list_op;
  if (op.marker.empty()) {
    list_op.start_offset = 0;
  } else {
    list_op.start_offset = boost::lexical_cast<uint64_t>(op.marker.c_str());
  }

  if (! op.num_entries) {
    op.num_entries = GC_LIST_DEFAULT_MAX;
  }
  
  list_op.max = op.num_entries;
  bool is_truncated = true;
  uint32_t total_num_entries = 0, num_entries = 0;
  cls_gc_urgent_data urgent_data;
  bool urgent_data_decoded = false;
  uint64_t end_offset = 0;
  do {
    in->clear();
    encode(list_op, *in);

    CLS_LOG(1, "INFO: cls_gc_queue_remove(): Entering cls_queue_list_entries \n");
    int ret = cls_queue_list_entries(hctx, in, out);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_list_entries(): returned error %d\n", ret);
      return ret;
    }

    cls_queue_list_ret op_ret;
    auto iter = out->cbegin();
    try {
      decode(op_ret, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode output\n");
      return -EINVAL;
    }
    is_truncated = op_ret.is_truncated;
    unsigned int index = 0;
    if (! urgent_data_decoded) {
      auto iter_urgent_data = op_ret.bl_urgent_data.cbegin();
      try {
        decode(urgent_data, iter_urgent_data);
        urgent_data_decoded = true;
      } catch (buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_gc_queue_list(): failed to decode urgent data\n");
        return -EINVAL;
      }
    }
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
        if (urgent_data.urgent_data_map.size() > 0) {
          auto it = urgent_data.urgent_data_map.find(info.tag);
          if (it != urgent_data.urgent_data_map.end()) {
            found = true;
            if (it->second > info.time) {
              CLS_LOG(1, "INFO: cls_gc_queue_remove(): tag found in urgent data: %s\n", info.tag.c_str());
              continue;
            } else if (it->second == info.time) {
              CLS_LOG(1, "INFO: cls_gc_queue_remove(): erasing tag from urgent data: %s\n", info.tag.c_str());
              urgent_data.urgent_data_map.erase(info.tag); //erase entry from map, as it will be removed later from queue
              urgent_data.num_head_urgent_entries -= 1;
            }
          }//end-if map end
        }//end-if urgent data
        if (! found) {
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
            if (xattr_urgent_data_map.size() > 0) {
              auto found = xattr_urgent_data_map.find(info.tag);
              if (found != xattr_urgent_data_map.end()) {
                if (found->second > info.time) {
                  CLS_LOG(1, "INFO: cls_gc_queue_remove(): tag found in xattrs urgent data map: %s\n", info.tag.c_str());
                  continue;
                } else if (found->second == info.time) {
                  CLS_LOG(1, "INFO: cls_gc_queue_remove(): erasing tag from xattrs urgent data: %s\n", info.tag.c_str());
                  xattr_urgent_data_map.erase(info.tag); //erase entry from map, as it will be removed later
                  urgent_data.num_xattr_urgent_entries -= 1;
                }
              }
            } // end - if xattrs size ...
          } // end - ret != ENOENT && ENODATA
        }// search in xattrs
        num_entries++;
      }//end-for
      
      if (num_entries < op.num_entries) {
        list_op.max = (op.num_entries - num_entries);
        list_op.start_offset = op_ret.next_offset;
        out->clear();
      } else {
        end_offset = op_ret.offsets[index - 1];
        CLS_LOG(1, "INFO: cls_gc_queue_remove(): index is %u and end_offset is: %lu\n", index, end_offset);
        break;
      }
    } //end-if
    else {
      break;
    }
  } while(is_truncated);
  CLS_LOG(1, "INFO: cls_gc_queue_remove(): Total number of entries to remove: %d\n", total_num_entries);

  if (end_offset != 0) {
    cls_queue_remove_op rem_op;
    if (op.marker.empty()) {
      rem_op.start_offset = 0;
    } else {
      rem_op.start_offset = boost::lexical_cast<uint64_t>(op.marker.c_str());
    }

    rem_op.end_offset = end_offset;
    CLS_LOG(1, "INFO: cls_gc_queue_remove(): start offset: %lu and end offset: %lu\n", rem_op.start_offset, rem_op.end_offset);

    encode(urgent_data, rem_op.bl_urgent_data);

    in->clear();
    encode(rem_op, *in);

    CLS_LOG(1, "INFO: cls_gc_queue_remove(): Entering cls_queue_remove_entries \n");
    int ret = cls_queue_remove_entries(hctx, in, out);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_queue_remove_entries(): returned error %d\n", ret);
      return ret;
    }
  }

  return 0;
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

  //Read urgent data
  in->clear();
  out->clear();
  
  ret = cls_queue_read_urgent_data(hctx, in, out);
  
  auto out_iter = out->cbegin();

  cls_queue_urgent_data_ret op_ret;
  try {
    decode(op_ret, out_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_urgent_data_ret(): failed to decode ouput\n");
    return -EINVAL;
  }

  auto bl_iter = op_ret.bl_urgent_data.cbegin();
  cls_gc_urgent_data urgent_data;
  try {
    decode(urgent_data, bl_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_urgent_data_ret(): failed to decode urgent data\n");
    return -EINVAL;
  }

  bool is_last_entry = false;
  in->clear();
  out->clear();
  ret = cls_queue_get_last_entry(hctx, in, out);
  if (ret < 0) {
    return ret;
  }

  cls_rgw_gc_obj_info info;
  auto iter = out->cbegin();
  try {
    decode(info, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_gc_queue_update_entry(): failed to decode entry\n");
    return -EINVAL;
  }

  CLS_LOG(1, "INFO: tag of gc info is %s\n", info.tag.c_str());
  if (info.tag == op.info.tag) {
    is_last_entry = true;
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
        CLS_LOG(1, "ERROR: cls_gc_queue_remove(): failed to decode xattrs urgent data map\n");
        return -EINVAL;
      } //end - catch
      if (xattr_urgent_data_map.size() > 0) {
        auto found = xattr_urgent_data_map.find(info.tag);
        if (found != xattr_urgent_data_map.end()) {
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
      } // end - if xattrs size ...
    }// end ret != ENOENT ...
  }

  bufferlist bl_urgent_data;
  if (! tag_found) {
    //try inserting in queue head
    urgent_data.urgent_data_map.insert({op.info.tag, op.info.time});
    urgent_data.num_head_urgent_entries += 1;
    has_urgent_data = true;

    //check if urgent data can fit in head
    out->clear();
    bool can_fit = false;
    encode(urgent_data, bl_urgent_data);
    ret = cls_queue_can_urgent_data_fit(hctx, &bl_urgent_data, out);
    if (ret < 0) {
      return ret;
    }
    iter = out->cbegin();
    decode(can_fit, iter);
    CLS_LOG(1, "INFO: Can urgent data fit: %d \n", can_fit);

    //insert as xattrs
    if (! can_fit) {
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

  bl_urgent_data.clear();
  encode(urgent_data, bl_urgent_data);
  in->clear();
  if (! is_last_entry) {
    cls_enqueue_op enqueue_op;
    bufferlist bl_data;
    encode(op.info, bl_data);
    enqueue_op.bl_data_vec.emplace_back(bl_data);
    CLS_LOG(1, "INFO: cls_gc_update_entry: Data size is: %u \n", bl_data.length());
    if (has_urgent_data) {
      enqueue_op.bl_urgent_data = bl_urgent_data;
    }
    encode(enqueue_op, *in);
    ret = cls_enqueue(hctx, in, out);
    if (ret < 0) {
      return ret;
    }
  } else {
    cls_queue_update_last_entry_op update_op;
    encode(op.info, update_op.bl_data);
    CLS_LOG(1, "INFO: cls_gc_update_entry: Data size is: %u \n", update_op.bl_data.length());
    if (has_urgent_data) {
      update_op.bl_urgent_data = bl_urgent_data;
    }
    encode(update_op, *in);
    ret = cls_queue_update_last_entry(hctx, in, out);
    if (ret < 0) {
      return ret;
    }
  }

  return 0;
}

CLS_INIT(rgw_queue)
{
  CLS_LOG(1, "Loaded rgw queue class!");

  cls_handle_t h_class;
  cls_method_handle_t h_gc_create_queue;
  cls_method_handle_t h_gc_init_queue;
  cls_method_handle_t h_gc_enqueue;
  cls_method_handle_t h_gc_queue_list_entries;
  cls_method_handle_t h_gc_queue_remove_entries;
  cls_method_handle_t h_gc_queue_update_entry;

  cls_register(RGW_QUEUE_CLASS, &h_class);

  /* gc */
  cls_register_cxx_method(h_class, GC_CREATE_QUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_create_queue, &h_gc_create_queue);
  cls_register_cxx_method(h_class, GC_INIT_QUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_init_queue, &h_gc_init_queue);
  cls_register_cxx_method(h_class, GC_ENQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_enqueue, &h_gc_enqueue);
  cls_register_cxx_method(h_class, GC_QUEUE_LIST_ENTRIES, CLS_METHOD_RD, cls_gc_queue_list, &h_gc_queue_list_entries);
  cls_register_cxx_method(h_class, GC_QUEUE_REMOVE_ENTRIES, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_queue_remove, &h_gc_queue_remove_entries);
  cls_register_cxx_method(h_class, GC_QUEUE_UPDATE_ENTRY, CLS_METHOD_RD | CLS_METHOD_WR, cls_gc_queue_update_entry, &h_gc_queue_update_entry);

  return;
}

