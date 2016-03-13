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
#include "common/ceph_json.h"
#include "common/strtol.h"
#include "rgw_rest.h"
#include "rgw_op.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_opstate.h"
#include "rgw_client_io.h"
#include "common/errno.h"
#include "include/assert.h"

#define OPSTATE_LIST_MAX_ENTRIES 1000
#define dout_subsys ceph_subsys_rgw

void RGWOp_Opstate_List::execute() {
  string client_id = s->info.args.get("client-id"),
         op_id = s->info.args.get("op-id"),
         object = s->info.args.get("object"),
         max_entries_str = s->info.args.get("max-entries");
  unsigned max_entries = OPSTATE_LIST_MAX_ENTRIES;
  
  if (!max_entries_str.empty()) {
    string err;
    max_entries = (unsigned)strict_strtol(max_entries_str.c_str(), 10, &err);
    if (!err.empty()) {
      dout(5) << "Error parsing max-entries " << max_entries_str << dendl;
      http_ret = -EINVAL;
      return;
    }
  } 
  
  RGWOpState oc = RGWOpState(store);
  void *handle;
  bool done;
  list<cls_statelog_entry> entries;

  oc.init_list_entries(client_id, op_id, object, &handle);

  http_ret = 0;
  send_response();
  do {
    int ret = oc.list_entries(handle, max_entries, entries, &done);

    if (ret < 0) {
      dout(5) << "oc.list_entries failed with error " << ret << dendl;
      http_ret = ret;
      oc.finish_list_entries(handle);
      return;
    }

    if (!max_entries_str.empty()) 
      max_entries -= entries.size();

    send_response(entries);
  } while (!done && max_entries > 0);

  oc.finish_list_entries(handle);
  send_response_end();
}

void RGWOp_Opstate_List::send_response() {
  if (sent_header)
    return;

  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  sent_header = true;

  if (http_ret < 0)
    return;

  s->formatter->open_array_section("entries");
}

void RGWOp_Opstate_List::send_response(list<cls_statelog_entry> entries) {
  RGWOpState oc(store);
  for (list<cls_statelog_entry>::iterator it = entries.begin();
       it != entries.end(); ++it) {
    oc.dump_entry(*it, s->formatter);
    flusher.flush();
  }
}

void RGWOp_Opstate_List::send_response_end() {
  s->formatter->close_section();
  flusher.flush();
}

void RGWOp_Opstate_Set::execute() {
  string client_id = s->info.args.get("client-id"),
         op_id = s->info.args.get("op-id"),
         object = s->info.args.get("object"),
         state_str = s->info.args.get("state");

  if (client_id.empty() ||
      op_id.empty() ||
      object.empty() ||
      state_str.empty()) {
    dout(5) << "Error - invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }

  RGWOpState oc(store);
  RGWOpState::OpState state;

  http_ret = oc.state_from_str(state_str, &state);
  if (http_ret < 0) {
    dout(5) << "Error - invalid state" << dendl;
    return;
  }

  http_ret = oc.set_state(client_id, op_id, object, state);
  if (http_ret < 0) {
    dout(5) << "Error - Unable to set state" << dendl;
    return;
  }
}

void RGWOp_Opstate_Renew::execute() {
  string client_id = s->info.args.get("client-id"),
         op_id = s->info.args.get("op-id"),
         object = s->info.args.get("object"),
         state_str = s->info.args.get("state");

  if (client_id.empty() ||
      op_id.empty() ||
      object.empty() ||
      state_str.empty()) {
    dout(5) << "Error - invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }
  RGWOpState oc(store);
  RGWOpState::OpState state;

  http_ret = oc.state_from_str(state_str, &state);
  if (http_ret < 0) {
    dout(5) << "Error - invalid state" << dendl;
    return;
  }

  http_ret = oc.renew_state(client_id, op_id, object, state);
  if (http_ret < 0) {
    dout(5) << "Error - Unable to renew state" << dendl;
    return;
  }
}

void RGWOp_Opstate_Delete::execute() {
  string client_id = s->info.args.get("client-id"),
         op_id = s->info.args.get("op-id"),
         object = s->info.args.get("object");

  if (client_id.empty() ||
      op_id.empty() ||
      object.empty()) {
    dout(5) << "Error - invalid parameter list" << dendl;
    http_ret = -EINVAL;
    return;
  }
  
  RGWOpState oc(store);

  http_ret = oc.remove_entry(client_id, op_id, object);
  if (http_ret < 0) {
    dout(5) << "Error - Unable to remove entry" << dendl;
  }
}

RGWOp *RGWHandler_Opstate::op_post() {
  if (s->info.args.exists("renew")) {
    return new RGWOp_Opstate_Renew;
  } 
  return new RGWOp_Opstate_Set;
}
