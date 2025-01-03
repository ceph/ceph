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
#include "rgw_rest_metadata.h"
#include "rgw_client_io.h"
#include "rgw_mdlog_types.h"
#include "rgw_sal_rados.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "rgw/rgw_b64.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

static inline void frame_metadata_key(req_state *s, string& out) {
  bool exists;
  string key = s->info.args.get("key", &exists);

  string section;
  if (!s->init_state.url_bucket.empty()) {
    section = s->init_state.url_bucket;
  } else {
    section = key;
    key.clear();
  }

  out = section;

  if (!key.empty()) {
    out += string(":") + key;
  }
}

void RGWOp_Metadata_Get::execute(optional_yield y) {
  string metadata_key;

  frame_metadata_key(s, metadata_key);

  auto meta_mgr = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr;

  /* Get keys */
  op_ret = meta_mgr->get(metadata_key, s->formatter, s->yield, s);
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't get key: " << cpp_strerror(op_ret) << dendl;
    return;
  }

  op_ret = 0;
}

void RGWOp_Metadata_Get_Myself::execute(optional_yield y) {
  s->info.args.append("key", to_string(s->owner.id));

  return RGWOp_Metadata_Get::execute(y);
}

void RGWOp_Metadata_List::execute(optional_yield y) {
  string marker;
  ldpp_dout(this, 16) << __func__
		    << " raw marker " << s->info.args.get("marker")
		    << dendl;

  try {
    marker = s->info.args.get("marker");
    if (!marker.empty()) {
      marker = rgw::from_base64(marker);
    }
    ldpp_dout(this, 16) << __func__
	     << " marker " << marker << dendl;
  } catch (...) {
    marker = std::string("");
  }

  bool max_entries_specified;
  string max_entries_str =
    s->info.args.get("max-entries", &max_entries_specified);

  bool extended_response = (max_entries_specified); /* for backward compatibility, if max-entries is not specified
                                                    we will send the old response format */
  uint64_t max_entries = 0;

  if (max_entries_specified) {
    string err;
    max_entries = (unsigned)strict_strtol(max_entries_str.c_str(), 10, &err);
    if (!err.empty()) {
      ldpp_dout(this, 5) << "Error parsing max-entries " << max_entries_str << dendl;
      op_ret = -EINVAL;
      return;
    }
  }

  string metadata_key;

  frame_metadata_key(s, metadata_key);
  /* List keys */
  void *handle;
  int max = 1000;

  /* example markers:
     marker = "3:b55a9110:root::bu_9:head";
     marker = "3:b9a8b2a6:root::sorry_janefonda_890:head";
     marker = "3:bf885d8f:root::sorry_janefonda_665:head";
  */

  op_ret = driver->meta_list_keys_init(this, metadata_key, marker, &handle);
  if (op_ret < 0) {
    ldpp_dout(this, 5) << "ERROR: can't get key: " << cpp_strerror(op_ret) << dendl;
    return;
  }

  bool truncated;
  uint64_t count = 0;

  if (extended_response) {
    s->formatter->open_object_section("result");
  }

  s->formatter->open_array_section("keys");

  uint64_t left;
  do {
    list<string> keys;
    left = (max_entries_specified ? max_entries - count : max);
    op_ret = driver->meta_list_keys_next(this, handle, left, keys, &truncated);
    if (op_ret < 0) {
      ldpp_dout(this, 5) << "ERROR: lists_keys_next(): " << cpp_strerror(op_ret)
	      << dendl;
      return;
    }

    for (list<string>::iterator iter = keys.begin(); iter != keys.end();
	 ++iter) {
      s->formatter->dump_string("key", *iter);
      ++count;
    }

  } while (truncated && left > 0);

  s->formatter->close_section();

  if (extended_response) {
    encode_json("truncated", truncated, s->formatter);
    encode_json("count", count, s->formatter);
    if (truncated) {
      string esc_marker =
	rgw::to_base64(driver->meta_get_marker(handle));
      encode_json("marker", esc_marker, s->formatter);
    }
    s->formatter->close_section();
  }
  driver->meta_list_keys_complete(handle);

  op_ret = 0;
}

int RGWOp_Metadata_Put::get_data(bufferlist& bl) {
  size_t cl = 0;
  char *data;
  int read_len;

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

static bool string_to_sync_type(const string& sync_string,
                                RGWMDLogSyncType& type) {
  if (sync_string.compare("update-by-version") == 0)
    type = APPLY_UPDATES;
  else if (sync_string.compare("update-by-timestamp") == 0)
    type = APPLY_NEWER;
  else if (sync_string.compare("always") == 0)
    type = APPLY_ALWAYS;
  else
    return false;
  return true;
}

void RGWOp_Metadata_Put::execute(optional_yield y) {
  bufferlist bl;
  string metadata_key;

  op_ret = get_data(bl);
  if (op_ret < 0) {
    return;
  }

  op_ret = do_aws4_auth_completion();
  if (op_ret < 0) {
    return;
  }

  frame_metadata_key(s, metadata_key);

  RGWMDLogSyncType sync_type = RGWMDLogSyncType::APPLY_ALWAYS;

  bool mode_exists = false;
  string mode_string = s->info.args.get("update-type", &mode_exists);
  if (mode_exists) {
    bool parsed = string_to_sync_type(mode_string,
                                      sync_type);
    if (!parsed) {
      op_ret = -EINVAL;
      return;
    }
  }

  op_ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->put(metadata_key, bl, s->yield, s, sync_type,
				       false, &ondisk_version);
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't put key: " << cpp_strerror(op_ret) << dendl;
    return;
  }
  // translate internal codes into return header
  if (op_ret == STATUS_NO_APPLY)
    update_status = "skipped";
  else if (op_ret == STATUS_APPLIED)
    update_status = "applied";
}

void RGWOp_Metadata_Put::send_response() {
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

void RGWOp_Metadata_Delete::execute(optional_yield y) {
  string metadata_key;

  frame_metadata_key(s, metadata_key);
  op_ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->remove(metadata_key, s->yield, s);
  if (op_ret < 0) {
    ldpp_dout(s, 5) << "ERROR: can't remove key: " << cpp_strerror(op_ret) << dendl;
    return;
  }
  op_ret = 0;
}

RGWOp *RGWHandler_Metadata::op_get() {
  if (s->info.args.exists("myself"))
    return new RGWOp_Metadata_Get_Myself;
  if (s->info.args.exists("key"))
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

