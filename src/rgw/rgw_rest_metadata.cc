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

int RGWHandler_Metadata::init_from_header(struct req_state *s) {
  const char *p;
  const string prefix = "/admin/metadata";
  string section, req;

  p = s->request_params.c_str();
  if(p[0] != '\0'){
    s->args.set(p);
    s->args.parse();
  }

  /*Allocate a JSON formatter*/
  s->format = RGW_FORMAT_JSON;
  s->formatter = new JSONFormatter(false);

  /*Get the section name*/  
  if (s->decoded_uri.compare(0, prefix.length(), prefix) != 0) {
    /*This is a safety check, may never happen*/
    return -EINVAL;
  } else {
    unsigned start_off = prefix.length();

    start_off += (s->decoded_uri.length() > prefix.length())?1:0;
    s->meta_section_str.assign(s->decoded_uri, 
                               start_off, 
                               string::npos);
  }
  return 0;
}

int RGWHandler_Metadata::init(RGWRados *store, 
                              struct req_state *state, 
                              RGWClientIO *cio) {
  int ret = init_from_header(state);
  if (ret < 0) 
    return ret;

  return RGWHandler_ObjStore::init(store, state, cio);
}

int RGWHandler_Metadata::authorize() {
  int ret = RGW_Auth_S3::authorize(store, s);
  if (ret < 0) {
    return ret;
  }
  /*TODO : check if the user is admin?*/
  return 0;
}

const char *RGWOp_Metadata_Get::name() {
  bool exists;

  (void)s->args.get("key", &exists);
  if (exists) {
    return "get_metadata";
  } else {
    return "list_metadata";
  }
}

static inline void frame_metadata_key(string& section, string& key, string& out) {
  out = (section + string(":") + key);
}

void RGWOp_Metadata_Get::execute() {
  bool exists;
  string key = s->args.get("key", &exists);
  unsigned num_params = s->args.get_num_params();
  string metadata_key;

  frame_metadata_key(s->meta_section_str, key, metadata_key);
  if ((num_params > 0) && !exists) {
    /*Invalid parameter*/
    http_ret = -EINVAL;
    return;
  }
  if (!exists) {
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
  } else {
    /* Get keys */
    http_ret = store->meta_mgr->get(metadata_key, s->formatter);
    if (http_ret < 0) {
      dout(5) << "ERROR: can't get key: " << cpp_strerror(http_ret) << dendl;
      return;
    }
  }

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
  string key = s->args.get("key");
  string metadata_key;

  http_ret = get_data(bl);
  if (http_ret < 0) {
    return;
  }
  
  frame_metadata_key(s->meta_section_str, key, metadata_key);
  
  http_ret = store->meta_mgr->put(metadata_key, bl);
  if (http_ret < 0) {
    dout(5) << "ERROR: can't put key: " << cpp_strerror(http_ret) << dendl;
    return;
  }
  http_ret = 0;
}

void RGWOp_Metadata_Delete::execute() {
  string key = s->args.get("key");
  string metadata_key;

  frame_metadata_key(s->meta_section_str, key, metadata_key);
  http_ret = store->meta_mgr->remove(metadata_key);
  if (http_ret < 0) {
    dout(5) << "ERROR: can't remove key: " << cpp_strerror(http_ret) << dendl;
    return;
  }
  http_ret = 0;
}

RGWOp *RGWHandler_Metadata::op_get() {
  return new RGWOp_Metadata_Get;
}

RGWOp *RGWHandler_Metadata::op_put() {
  return new RGWOp_Metadata_Put;
}

RGWOp *RGWHandler_Metadata::op_delete() {
  return new RGWOp_Metadata_Delete;
}
