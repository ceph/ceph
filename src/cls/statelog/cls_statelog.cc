// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "include/types.h"
#include "include/utime.h"
#include "objclass/objclass.h"

#include "cls_statelog_types.h"
#include "cls_statelog_ops.h"

#include "global/global_context.h"

CLS_VER(1,0)
CLS_NAME(statelog)

cls_handle_t h_class;
cls_method_handle_t h_statelog_add;
cls_method_handle_t h_statelog_list;
cls_method_handle_t h_statelog_remove;
cls_method_handle_t h_statelog_check_state;

static string statelog_index_by_client_prefix = "1_";
static string statelog_index_by_object_prefix = "2_";


static int write_statelog_entry(cls_method_context_t hctx, const string& index, const cls_statelog_entry& entry)
{
  bufferlist bl;
  ::encode(entry, bl);

  int ret = cls_cxx_map_set_val(hctx, index, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static void get_index_by_client(const string& client_id, const string& op_id, string& index)
{
  index = statelog_index_by_client_prefix;
  index.append(client_id + "_" + op_id);
}

static void get_index_by_client(cls_statelog_entry& entry, string& index)
{
  get_index_by_client(entry.client_id, entry.op_id, index);
}

static void get_index_by_object(const string& object, const string& op_id, string& index)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "%d_", (int)object.size());

  index = statelog_index_by_object_prefix + buf; /* append object length to ensure uniqueness */
  index.append(object + "_" + op_id);
}

static void get_index_by_object(cls_statelog_entry& entry, string& index)
{
  get_index_by_object(entry.object, entry.op_id, index);
}

static int get_existing_entry(cls_method_context_t hctx, const string& client_id,
                               const string& op_id, const string& object,
                               cls_statelog_entry& entry)
{
  if ((object.empty() && client_id.empty()) || op_id.empty()) {
    return -EINVAL;
  }

  string obj_index;
  if (!object.empty()) {
    get_index_by_object(object, op_id, obj_index);
  } else {
    get_index_by_client(client_id, op_id, obj_index);
  }

  bufferlist bl;
  int rc = cls_cxx_map_get_val(hctx, obj_index, &bl);
  if (rc < 0) {
    CLS_LOG(0, "could not find entry %s", obj_index.c_str());
    return rc;
  }
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(entry, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: failed to decode entry %s", obj_index.c_str());
    return -EIO;
  }

  if ((!object.empty() && entry.object != object) ||
      (!client_id.empty() && entry.client_id != client_id)){
    /* ouch, we were passed inconsistent client_id / object */
    CLS_LOG(0, "data mismatch: object=%s client_id=%s entry: object=%s client_id=%s",
            object.c_str(), client_id.c_str(), entry.object.c_str(), entry.client_id.c_str());
    return -EINVAL;
  }

  return 0;
}

static int cls_statelog_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_statelog_add_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_statelog_add_op(): failed to decode op");
    return -EINVAL;
  }

  for (list<cls_statelog_entry>::iterator iter = op.entries.begin();
       iter != op.entries.end(); ++iter) {
    cls_statelog_entry& entry = *iter;

    string index_by_client;

    get_index_by_client(entry, index_by_client);

    CLS_LOG(0, "storing entry by client/op at %s", index_by_client.c_str());

    int ret = write_statelog_entry(hctx, index_by_client, entry);
    if (ret < 0)
      return ret;

    string index_by_obj;

    get_index_by_object(entry, index_by_obj);

    CLS_LOG(0, "storing entry by object at %s", index_by_obj.c_str());
    ret = write_statelog_entry(hctx, index_by_obj, entry);
    if (ret < 0)
      return ret;

  }
  
  return 0;
}

static int cls_statelog_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_statelog_list_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_statelog_list_op(): failed to decode op");
    return -EINVAL;
  }

  map<string, bufferlist> keys;

  string from_index;
  string match_prefix;

  if (!op.client_id.empty()) {
    get_index_by_client(op.client_id, op.op_id, match_prefix);
  } else if (!op.object.empty()) {
    get_index_by_object(op.object, op.op_id, match_prefix);
  } else {
    match_prefix = statelog_index_by_object_prefix;
  }

  if (op.marker.empty()) {
    from_index = match_prefix;
  } else {
    from_index = op.marker;
  }

#define MAX_ENTRIES 1000
  size_t max_entries = op.max_entries;
  if (!max_entries || max_entries > MAX_ENTRIES)
    max_entries = MAX_ENTRIES;

  int rc = cls_cxx_map_get_vals(hctx, from_index, match_prefix, max_entries + 1, &keys);
  if (rc < 0)
    return rc;

  CLS_LOG(20, "from_index=%s match_prefix=%s", from_index.c_str(), match_prefix.c_str());
  cls_statelog_list_ret ret;

  list<cls_statelog_entry>& entries = ret.entries;
  map<string, bufferlist>::iterator iter = keys.begin();

  bool done = false;
  string marker;

  size_t i;
  for (i = 0; i < max_entries && iter != keys.end(); ++i, ++iter) {
    const string& index = iter->first;
    marker = index;

    bufferlist& bl = iter->second;
    bufferlist::iterator biter = bl.begin();
    try {
      cls_statelog_entry e;
      ::decode(e, biter);
      entries.push_back(e);
    } catch (buffer::error& err) {
      CLS_LOG(0, "ERROR: cls_statelog_list: could not decode entry, index=%s", index.c_str());
    }
  }

  if (iter == keys.end())
    done = true;
  else
    ret.marker = marker;

  ret.truncated = !done;

  ::encode(ret, *out);

  return 0;
}

static int cls_statelog_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_statelog_remove_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_statelog_remove_op(): failed to decode op");
    return -EINVAL;
  }

  cls_statelog_entry entry;

  int rc = get_existing_entry(hctx, op.client_id, op.op_id, op.object, entry);
  if (rc < 0)
    return rc;

  string obj_index;
  get_index_by_object(entry.object, entry.op_id, obj_index);

  rc = cls_cxx_map_remove_key(hctx, obj_index);
  if (rc < 0) {
    CLS_LOG(0, "ERROR: failed to remove key");
    return rc;
  }

  string client_index;
  get_index_by_client(entry.client_id, entry.op_id, client_index);

  rc = cls_cxx_map_remove_key(hctx, client_index);
  if (rc < 0) {
    CLS_LOG(0, "ERROR: failed to remove key");
    return rc;
  }

  return 0;
}

static int cls_statelog_check_state(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_statelog_check_state_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_statelog_check_state_op(): failed to decode op");
    return -EINVAL;
  }

  if (op.object.empty() || op.op_id.empty()) {
    CLS_LOG(0, "object name or op id not specified");
    return -EINVAL;
  }

  string obj_index;
  get_index_by_object(op.object, op.op_id, obj_index);

  bufferlist bl;
  int rc = cls_cxx_map_get_val(hctx, obj_index, &bl);
  if (rc < 0) {
    CLS_LOG(0, "could not find entry %s", obj_index.c_str());
    return rc;
  }

  cls_statelog_entry entry;

  rc = get_existing_entry(hctx, op.client_id, op.op_id, op.object, entry);
  if (rc < 0)
    return rc;

  if (entry.state != op.state)
    return -ECANCELED;

  return 0;
}

void __cls_init()
{
  CLS_LOG(1, "Loaded log class!");

  cls_register("statelog", &h_class);

  /* log */
  cls_register_cxx_method(h_class, "add", CLS_METHOD_RD | CLS_METHOD_WR, cls_statelog_add, &h_statelog_add);
  cls_register_cxx_method(h_class, "list", CLS_METHOD_RD, cls_statelog_list, &h_statelog_list);
  cls_register_cxx_method(h_class, "remove", CLS_METHOD_RD | CLS_METHOD_WR, cls_statelog_remove, &h_statelog_remove);
  cls_register_cxx_method(h_class, "check_state", CLS_METHOD_RD, cls_statelog_check_state, &h_statelog_check_state);

  return;
}

