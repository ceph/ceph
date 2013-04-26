// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include "rgw_rest.h"
#include "rgw_op.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_metadata.h"
#include "rgw_client_io.h"
#include "common/errno.h"
#define dout_subsys ceph_subsys_rgw


const char *RGWOp_Metadata_Get::name() {
  return "get_metadata";
}

static inline void frame_metadata_key(req_state *s, string& out) {
  bool exists;
  string key = s->args.get("key", &exists);

  string metadata_key;
  string section;
  if (s->bucket_name) {
    section = s->bucket_name;
  } else {
    section = key;
    key.clear();
  }

  out = (section + string(":") + key);

  if (!key.empty()) {
    out += string(":") + key;
  }
}

void RGWOp_Metadata_Get::execute() {
  string metadata_key;

  frame_metadata_key(s, metadata_key);

  /* Get keys */
  http_ret = store->meta_mgr->get(metadata_key, s->formatter);
  if (http_ret < 0) {
    dout(5) << "ERROR: can't get key: " << cpp_strerror(http_ret) << dendl;
    return;
  }

  http_ret = 0;
}

const char *RGWOp_Metadata_List::name() {
  return "list_metadata";
}

void RGWOp_Metadata_List::execute() {
  string metadata_key;

  frame_metadata_key(s, metadata_key);
  /* List keys */
  void *handle;
  int max = 1000;

  http_ret = store->meta_mgr->list_keys_init(metadata_key, &handle);
  if (http_ret < 0) {
    dout(5) << "ERROR: can't get key: " << cpp_strerror(http_ret) << dendl;
    return;
  }

  bool truncated;

  s->formatter->open_array_section("keys");

  do {
    list<string> keys;
    http_ret = store->meta_mgr->list_keys_next(handle, max, keys, &truncated);
    if (http_ret < 0) {
      dout(5) << "ERROR: lists_keys_next(): " << cpp_strerror(http_ret) << dendl;
      return;
    }

    for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
      s->formatter->dump_string("key", *iter);
    }

  } while (truncated);

  s->formatter->close_section();

  store->meta_mgr->list_keys_complete(handle);

  http_ret = 0;
}

int RGWOp_Metadata_Put::get_data(bufferlist& bl) {
  size_t cl = 0;
  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    char *data = (char *)malloc(cl + 1);
    if (!data) {
       return -ENOMEM;
    }
    int read_len;
    int r = s->cio->read(data, cl, &read_len);
    if (cl != (size_t)read_len) {
      dout(10) << "cio->read incomplete" << dendl;
    }
    if (r < 0) {
      free(data);
      return r;
    }
    bl.append(data, read_len);
    free(data);
  }

  return 0;
}

void RGWOp_Metadata_Put::execute() {
  bufferlist bl;
  string metadata_key;

  http_ret = get_data(bl);
  if (http_ret < 0) {
    return;
  }
  
  frame_metadata_key(s, metadata_key);
  
  http_ret = store->meta_mgr->put(metadata_key, bl);
  if (http_ret < 0) {
    dout(5) << "ERROR: can't put key: " << cpp_strerror(http_ret) << dendl;
    return;
  }
  http_ret = 0;
}

void RGWOp_Metadata_Delete::execute() {
  string metadata_key;

  frame_metadata_key(s, metadata_key);
  http_ret = store->meta_mgr->remove(metadata_key);
  if (http_ret < 0) {
    dout(5) << "ERROR: can't remove key: " << cpp_strerror(http_ret) << dendl;
    return;
  }
  http_ret = 0;
}

RGWOp *RGWHandler_Metadata::op_get() {
  if (s->args.exists("key"))
    return new RGWOp_Metadata_Get;
  else
    return new RGWOp_Metadata_List;
}

RGWOp *RGWHandler_Metadata::op_put() {
  return new RGWOp_Metadata_Put;
}

RGWOp *RGWHandler_Metadata::op_delete() {
  return new RGWOp_Metadata_Delete;
}
