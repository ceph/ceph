// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Lluis Pamies-Juarez <lluis@pamies.cat>
 * Copyright (C) 2013 HGST
 *
 * Author: Lluis Pamies-Juarez <lluis@pamies.cat>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "PG.h"
#include "EncodedPG.h"
#include "OSD.h"
#include "OpRequest.h"

#include "common/errno.h"
#include "common/perf_counters.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"

#include "messages/MOSDPing.h"
#include "messages/MWatchNotify.h"

#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPushReply.h"

#include "Watch.h"

#include "common/config.h"
#include "include/compat.h"
#include "common/cmdparse.h"

#include "osdc/Objecter.h"

#include "json_spirit/json_spirit_value.h"
#include "json_spirit/json_spirit_reader.h"
#include "include/assert.h"

#include <sstream>
#include <utility>

#include <errno.h>

EncodedPG::EncodedPG(OSDService *o, OSDMapRef curmap,
			   const PGPool &_pool, pg_t p, const hobject_t& oid,
			   const hobject_t& ioid) : PG(o, curmap, _pool, p, oid, ioid) {
}

void EncodedPG::mark_all_unfound_lost(int how) {
}

void EncodedPG::dump_recovery_info(ceph::Formatter* f) const {
}

void EncodedPG::calc_trim_to() {
}

void EncodedPG::check_local() {
}

int EncodedPG::start_recovery_ops(int max, PG::RecoveryCtx *prctx,
    ThreadPool::TPHandle &handle) {
  return 0;
}

void EncodedPG::_clear_recovery_state() {
}

void EncodedPG::check_recovery_sources(OSDMapRef newmap) {
}

void EncodedPG::_split_into(pg_t child_pgid,
    PG *child, unsigned split_bits) {
}

coll_t EncodedPG::get_temp_coll() {
}

bool EncodedPG::have_temp_coll() {
  return true;
}

void EncodedPG::on_removal(ObjectStore::Transaction *t) {
    osd->clog.error() << "ENCODEDPG - on_removal\n";
}

void EncodedPG::do_request(OpRequestRef op, ThreadPool::TPHandle &handle){
    osd->clog.error() << "ENCODEDPG - do_request\n";
}

void EncodedPG::handle_watch_timeout(WatchRef watch) {
    osd->clog.error() << "ENCODEDPG - do_op\n";
}

void EncodedPG::do_op(OpRequestRef op){
    osd->clog.error() << "ENCODEDPG - do_op\n";
}

void EncodedPG::do_sub_op(OpRequestRef op) {
    osd->clog.error() << "ENCODEDPG - do_sub_op\n";
}

void EncodedPG::do_sub_op_reply(OpRequestRef op) {
    osd->clog.error() << "ENCODEDPG - do_sub_op_reply\n";
}

void EncodedPG::do_scan(OpRequestRef op, ThreadPool::TPHandle &handle) {
    osd->clog.error() << "ENCODEDPG - do_scan\n";
}

void EncodedPG::do_backfill(OpRequestRef op) {
    osd->clog.error() << "ENCODEDPG - do_backfill\n";
}

void EncodedPG::do_push(OpRequestRef op){
    osd->clog.error() << "ENCODEDPG - do_push\n";
}

void EncodedPG::do_pull(OpRequestRef op){
    osd->clog.error() << "ENCODEDPG - do_pull\n";
}

void EncodedPG::do_push_reply(OpRequestRef op){
    osd->clog.error() << "ENCODEDPG - do_push_reply\n";
}

void EncodedPG::snap_trimmer(){
}

int EncodedPG::do_command(cmdmap_t cmdmap, ostream& ss,
  		 bufferlist& idata, bufferlist& odata){
  osd->clog.error() << "ENCODEDPG - do_command\n";
  return 0;
}

bool EncodedPG::same_for_read_since(epoch_t e){
  return true;
}

bool EncodedPG::same_for_modify_since(epoch_t e){
  return true;
}

bool EncodedPG::same_for_rep_modify_since(epoch_t e){
  return true;
}

void EncodedPG::on_role_change(){
}

void EncodedPG::on_change(ObjectStore::Transaction *t){
}

void EncodedPG::on_activate(){
}

void EncodedPG::on_flushed(){
}

void EncodedPG::on_shutdown(){
}

void EncodedPG::check_blacklisted_watchers(){
}

void EncodedPG::get_watchers(std::list<obj_watch_item_t>&) {
}
