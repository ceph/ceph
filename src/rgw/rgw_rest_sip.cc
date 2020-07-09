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

#include "rgw_rest_sip.h"
#include "rgw_rest_conn.h"
#include "rgw_sync_info.h"
#include "rgw_sal_rados.h"

#include "common/errno.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw


void RGWOp_SIP_GetInfo::execute() {
  auto opt_instance = s->info.args.get_std_optional("instance");

  sip = store->ctl()->si.mgr->find_sip(provider, opt_instance);
  if (!sip) {
    ldout(s->cct, 20) << "ERROR: sync info provider not found" << dendl;
    op_ret = -ENOENT;
    return;
  }
}

void RGWOp_SIP_GetInfo::send_response() {
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);

  if (op_ret < 0)
    return;

  encode_json("info", sip->get_info(), s->formatter);
  flusher.flush();
}

void RGWOp_SIP_GetStageStatus::execute() {
  auto opt_instance = s->info.args.get_std_optional("instance");

  auto sip = store->ctl()->si.mgr->find_sip(provider, opt_instance);
  if (!sip) {
    ldout(s->cct, 5) << "ERROR: sync info provider not found" << dendl;
    http_ret = -ENOENT;
    return;
  }

  auto opt_stage_id = s->info.args.get_std_optional("stage-id");
  if (!opt_stage_id) {
    ldout(s->cct,  5) << "ERROR: missing 'stage-id' param" << dendl;
    http_ret = -EINVAL;
    return;
  }
  auto& sid = *opt_stage_id;

  int shard_id;
  http_ret = s->info.args.get_int("shard-id", &shard_id, 0);
  if (http_ret < 0) {
    ldout(s->cct, 5) << "ERROR: invalid 'shard-id' param: " << http_ret << dendl;
    return;
  }

  http_ret = sip->get_start_marker(sid, shard_id, &start_marker);
  if (http_ret < 0) {
    ldout(s->cct, 5) << "ERROR: sip->get_start_marker() returned error: ret=" << http_ret << dendl;
    return;
  }

  http_ret = sip->get_cur_state(sid, shard_id, &cur_marker);
  if (http_ret < 0) {
    ldout(s->cct, 5) << "ERROR: sip->get_cur_state() returned error: ret=" << http_ret << dendl;
    return;
  }
}

void RGWOp_SIP_GetStageStatus::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  {
    Formatter::ObjectSection top_section(*s->formatter, "result");
    Formatter::ObjectSection markers_section(*s->formatter, "markers");
    encode_json("start", start_marker, s->formatter);
    encode_json("current", cur_marker, s->formatter);
  }
  flusher.flush();
}

void RGWOp_SIP_List::execute() {
  providers = static_cast<rgw::sal::RGWRadosStore*>(store)->ctl()->si.mgr->list_sip();
}

void RGWOp_SIP_List::send_response() {
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);

  if (op_ret < 0)
    return;

  encode_json("providers", providers, s->formatter);
  flusher.flush();
}

void RGWOp_SIP_Fetch::execute() {
  auto opt_instance = s->info.args.get_std_optional("instance");
  auto opt_stage_id = s->info.args.get_std_optional("stage-id");
  auto marker = s->info.args.get("marker");

  int max_entries;
  op_ret = s->info.args.get_int("max", &max_entries, default_max);
  if (op_ret < 0) {
    ldout(s->cct, 5) << "ERROR: invalid 'max' param: " << op_ret << dendl;
    return;
  }

  int shard_id;
  op_ret = s->info.args.get_int("shard-id", &shard_id, 0);
  if (op_ret < 0) {
    ldout(s->cct, 5) << "ERROR: invalid 'shard-id' param: " << op_ret << dendl;
    return;
  }

  sip = static_cast<rgw::sal::RGWRadosStore*>(store)->ctl()->si.mgr->find_sip(provider, opt_instance);
  if (!sip) {
    ldout(s->cct, 20) << "ERROR: sync info provider not found" << dendl;
    return;
  }

  stage_id = opt_stage_id.value_or(sip->get_first_stage());

  http_ret = sip->fetch(stage_id, shard_id, marker, max_entries, &result);
  if (http_ret < 0) {
    ldout(s->cct, 0) << "ERROR: failed to fetch entries: " << http_ret << dendl;
    return;
  }
}

void RGWOp_SIP_Fetch::send_response() {
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);

  if (op_ret < 0)
    return;

  auto formatter = s->formatter;

  {
    Formatter::ObjectSection top_section(*formatter, "result");
    encode_json("more", result.more, formatter);
    encode_json("done", result.done, formatter);

    Formatter::ArraySection as(*formatter, "entries");

    for (auto& e : result.entries) {
      Formatter::ObjectSection hs(*formatter, "handler");
      encode_json("key", e.key, formatter);
      int r = sip->handle_entry(stage_id, e, [&](SIProvider::EntryInfoBase& info) {
                                encode_json("info", info, formatter);
                                return 0;
                                });
      if (r < 0) {
        ldout(s->cct, 0) << "ERROR: provider->handle_entry() failed: r=" << r << dendl;
        break;
      }
    }
  }
  flusher.flush();
}

void RGWOp_SIP_Trim::execute() {
  auto opt_instance = s->info.args.get_std_optional("instance");
  auto opt_stage_id = s->info.args.get_std_optional("stage-id");
  auto marker = s->info.args.get("marker");

  int shard_id;
  op_ret = s->info.args.get_int("shard-id", &shard_id, 0);
  if (op_ret < 0) {
    ldout(s->cct, 5) << "ERROR: invalid 'shard-id' param: " << op_ret << dendl;
    return;
  }

  auto sip = static_cast<rgw::sal::RGWRadosStore*>(store)->ctl()->si.mgr->find_sip(provider, opt_instance);
  if (!sip) {
    ldout(s->cct, 20) << "ERROR: sync info provider not found" << dendl;
    return;
  }

  auto stage_id = opt_stage_id.value_or(sip->get_first_stage());

  op_ret = sip->trim(stage_id, shard_id, marker);
  if (op_ret < 0) {
    ldout(s->cct, 0) << "ERROR: failed to fetch entries: " << op_ret << dendl;
    return;
  }
}

RGWOp *RGWHandler_SIP::op_get() {
  auto provider = s->info.args.get_std_optional("provider");
  if (!provider) {
    return new RGWOp_SIP_List;
  }

  if (s->info.args.exists("info")) {
    return new RGWOp_SIP_GetInfo(std::move(*provider));
  }

  if (s->info.args.exists("status")) {
    return new RGWOp_SIP_GetStageStatus(std::move(*provider));
  }
  return new RGWOp_SIP_Fetch(std::move(*provider));
}

RGWOp *RGWHandler_SIP::op_delete() {
  auto provider = s->info.args.get_std_optional("provider");
  if (!provider) {
    return nullptr;
  }

  return new RGWOp_SIP_Trim(std::move(*provider));
}

