// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include "include/page.h"

#include "rgw_rest.h"
#include "rgw_op.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_remoted4n.h"
#include "rgw_client_io.h"
#include "rgw_mdlog_types.h"
#include "driver/d4n/rgw_sal_d4n.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "rgw/rgw_b64.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

static inline void get_remoted4n_key(req_state *s, string& out) {
  bool exists;
  string key;// = s->info.args.get("key", &exists);
  if (!rgw::sal::Object::empty(s->object.get()))
      key = s->object->get_name();
  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << ": key is: " << key << dendl;

  string section;
  if (!s->init_state.url_bucket.empty()) {
    section = s->init_state.url_bucket;
  } else {
    section = key;
    key.clear();
  }

  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << ": section is: " << section << dendl;
  out = section;

  if (!key.empty()) {
    out += string("_") + key;
  }
  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << ": out is: " << out << dendl;
}

void RGWOp_RemoteD4N_Get::execute(optional_yield y) {
  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << ": " << dendl;
  string key;

  get_remoted4n_key(s, key);

  //op_ret = static_cast<rgw::sal::D4NFilterDriver*>(driver)->get_cache_driver()->get(s, key, offset, len , bl, attrs, y); //FIXME: object length
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't get key: " << cpp_strerror(op_ret) << dendl;
    return;
  }

  op_ret = 0;
}

/*
void RGWOp_RemoteD4N_Get_Myself::execute(optional_yield y) {
  const std::string owner_id = s->owner.id.to_str();
  s->info.args.append("key", owner_id);

  return RGWOp_RemoteD4N_Get::execute(y);
}
*/

int RGWOp_RemoteD4N_Put::get_data(bufferlist& bl) {
  
  size_t cl = 0;
  int read_len;
  char *data;
  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    data = (char *)malloc(cl + 1);
    if (!data) {
       return -ENOMEM;
    }
    read_len = recv_body(s, data, cl);
    if (cl != (size_t)read_len) {
      ldpp_dout(this, 10) << "recv_body incomplete" << dendl;
    }
    if (read_len < 0) {
      free(data);
      return read_len;
    }
    bl.append(data, read_len);
  } else {
    int chunk_size = CEPH_PAGE_SIZE;
    const char *enc = s->info.env->get("HTTP_TRANSFER_ENCODING");
    if (!enc || strcmp(enc, "chunked")) {
      return -ERR_LENGTH_REQUIRED;
    }
    data = (char *)malloc(chunk_size);
    if (!data) {
      return -ENOMEM;
    }
    do {
      read_len = recv_body(s, data, chunk_size);
      if (read_len < 0) {
	free(data);
	return read_len;
      }
      bl.append(data, read_len);
    } while (read_len == chunk_size);
  }
  free(data);
  return 0;
}

void RGWOp_RemoteD4N_Put::execute(optional_yield y) {
  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << dendl;
  bufferlist bl;
  rgw::sal::Attrs attrs;

  string key;
  get_remoted4n_key(s, key);
  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << ": key is: " << key << dendl;


  op_ret = get_data(bl);
  if (op_ret < 0) {
    return;
  }
  ldpp_dout(s, 20) << "AMIN: " << __func__ << __LINE__ << ": data is: " << bl.to_str() << dendl;

  if (!rgw::sal::Object::empty(s->object.get()))
      attrs = s->object->get_attrs();

  int ofs = 0; //FIXME: AMIN: what if we have several blocks from a remote cache? how can we pass this info to a remote cache?
  string oid_in_cache = key + std::to_string(ofs) + std::to_string(bl.length()); //FIXME: AMIN: should we consider the object clean?

  op_ret = static_cast<rgw::sal::D4NFilterDriver*>(driver)->get_cache_driver()->put(s, oid_in_cache, bl, bl.length(), attrs, y);
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't write object into the cache: " << cpp_strerror(op_ret) << dendl;
    return;
  }
  // translate internal codes into return header
  if (op_ret == 0)
    update_status = "created";
  ldpp_dout(s, 20) << "AMIN: " << __func__ << ": " << __LINE__ << dendl;
}

void RGWOp_RemoteD4N_Put::send_response() {
  int op_return_code = op_ret;
  if ((op_ret == STATUS_NO_APPLY) || (op_ret == STATUS_APPLIED))
    op_return_code = STATUS_NO_CONTENT;
  set_req_state_err(s, op_return_code);
  dump_errno(s);
  stringstream ver_stream;
  ver_stream << "ver:" << ondisk_version.ver
	     <<",tag:" << ondisk_version.tag;
  dump_header_if_nonempty(s, "RGWX_UPDATE_STATUS", update_status);
  dump_header_if_nonempty(s, "RGWX_UPDATE_VERSION", ver_stream.str());
  end_header(s);
}

void RGWOp_RemoteD4N_Delete::execute(optional_yield y) {
  string key;

  get_remoted4n_key(s, key);
  op_ret = static_cast<rgw::sal::D4NFilterDriver*>(driver)->get_cache_driver()->del(s, key, y);
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't remove key: " << cpp_strerror(op_ret) << dendl;
    return;
  }
}

RGWOp *RGWHandler_RemoteD4N::op_get() {
  return new RGWOp_RemoteD4N_Get;
}

RGWOp *RGWHandler_RemoteD4N::op_put() {
  return new RGWOp_RemoteD4N_Put;
}

RGWOp *RGWHandler_RemoteD4N::op_delete() {
  return new RGWOp_RemoteD4N_Delete;
}

