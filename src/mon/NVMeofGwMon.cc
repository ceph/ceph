// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <boost/tokenizer.hpp>
#include "include/stringify.h"
#include "NVMeofGwMon.h"
#include "messages/MNVMeofGwBeacon.h"
#include "messages/MNVMeofGwMap.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix *_dout << "nvmeofgw " << __PRETTY_FUNCTION__ << " "

using std::string;

void NVMeofGwMon::init(){
    dout(4) <<  "called " << dendl;
    g_conf().add_observer(this);
}

void NVMeofGwMon::on_restart(){
    dout(4) <<  "called " << dendl;
    last_beacon.clear();
    last_tick = ceph::coarse_mono_clock::now();
}


void NVMeofGwMon::synchronize_last_beacon(){
    dout(4) <<  "called " << dendl;
    last_beacon.clear();
    last_tick = ceph::coarse_mono_clock::now();
    // Initialize last_beacon to identify transitions of available  GWs to unavailable state
    for (const auto& created_map_pair: pending_map.Created_gws) {
      const auto& group_key = created_map_pair.first;
      const NvmeGwCreatedMap& gw_created_map = created_map_pair.second;
      for (const auto& gw_created_pair: gw_created_map) {
          const auto& gw_id = gw_created_pair.first;
          if (gw_created_pair.second.availability == GW_AVAILABILITY_E::GW_AVAILABLE){
             dout(4) << "synchronize last_beacon for  GW :" << gw_id << dendl;
             LastBeacon lb = {gw_id, group_key};
             last_beacon[lb] = last_tick;
          }
      }
    }
}

void NVMeofGwMon::on_shutdown() {
     g_conf().remove_observer(this);
}

void NVMeofGwMon::tick(){
   // static int cnt=0;
    if(map.delay_propose){
       check_subs(false);  // to send map to clients
       map.delay_propose = false;
    }

    if (!is_active() || !mon.is_leader()){
        dout(10) << "NVMeofGwMon leader : " << mon.is_leader() << "active : " << is_active()  << dendl;
        last_leader = false;
        return;
    }
    bool _propose_pending = false;
  
    const auto now = ceph::coarse_mono_clock::now();
    const auto nvmegw_beacon_grace = g_conf().get_val<std::chrono::seconds>("mon_nvmeofgw_beacon_grace"); 
    dout(10) <<  "NVMeofGwMon leader got a real tick, pending epoch "<< pending_map.epoch     << dendl;

    const auto mgr_tick_period = g_conf().get_val<std::chrono::seconds>("mgr_tick_period");

    if (last_tick != ceph::coarse_mono_clock::zero()
          && (now - last_tick > (nvmegw_beacon_grace - mgr_tick_period))) {
        // This case handles either local slowness (calls being delayed
        // for whatever reason) or cluster election slowness (a long gap
        // between calls while an election happened)
        dout(4) << ": resetting beacon timeouts due to mon delay "
                "(slow election?) of " << now - last_tick << " seconds" << dendl;
        for (auto &i : last_beacon) {
          i.second = now;
        }
    }

    last_tick = now;
    bool propose = false;

    pending_map.update_active_timers(propose);  // Periodic: check active FSM timers
    _propose_pending |= propose;


    //handle exception of tick overdued in order to avoid false detection of overdued beacons , see MgrMonitor::tick
    const auto cutoff = now - nvmegw_beacon_grace;
    for(auto &itr : last_beacon){// Pass over all the stored beacons
        auto& lb = itr.first;
        auto last_beacon_time = itr.second;
        if(last_beacon_time < cutoff){
            dout(4) << "beacon timeout for GW " << lb.gw_id << dendl;
            pending_map.process_gw_map_gw_down( lb.gw_id, lb.group_key, propose);
            _propose_pending |= propose;
            last_beacon.erase(lb);
        }
        else {
           dout(20) << "beacon live for GW key: " << lb.gw_id << dendl;
        }
    }

    pending_map.handle_abandoned_ana_groups(propose); // Periodic: take care of not handled ANA groups
    _propose_pending |= propose;

    if(_propose_pending){
       //pending_map.delay_propose = true; // not to send map to clients immediately in "update_from_paxos"
       dout(4) << "decision to delayed_map" <<dendl;
       propose_pending();
    }

    // if propose_pending returned true , call propose_pending method of the paxosService
    // todo understand the logic of paxos.plugged for sending several propose_pending see MgrMonitor::tick
}

const char **NVMeofGwMon::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    NULL
  };
  return KEYS;
}

void NVMeofGwMon::handle_conf_change(const ConfigProxy& conf,
                                    const std::set<std::string> &changed)
{
  dout(4) << "changed " << changed << dendl;
}

void NVMeofGwMon::create_pending(){

    pending_map = map;// deep copy of the object
    // TODO  since "pending_map"  can be reset  each time during paxos re-election even in the middle of the changes ...
    pending_map.epoch++;
    dout(4) << " pending " << pending_map  << dendl;
    if(last_leader == false){ // peon becomes leader and gets updated map , need to synchronize the last_beacon
        synchronize_last_beacon();
        last_leader = true;
    }
}

void NVMeofGwMon::encode_pending(MonitorDBStore::TransactionRef t){

    dout(10) <<  dendl;
    ceph_assert(get_last_committed() + 1 == pending_map.epoch);
    bufferlist bl;
    pending_map.encode(bl);
    put_version(t, pending_map.epoch, bl);
    put_last_committed(t, pending_map.epoch);
}

void NVMeofGwMon::update_from_paxos(bool *need_bootstrap){
    version_t version = get_last_committed();

    //dout(4) <<  MY_MON_PREFFIX << __func__ << " version "  << version  << " map.epoch " << map.epoch << dendl;

    if (version != map.epoch) {
        dout(4) << " NVMeGW loading version " << version  << " " << map.epoch << dendl;

        bufferlist bl;
        int err = get_version(version, bl);
        ceph_assert(err == 0);

        auto p = bl.cbegin();
        map.decode(p);
        if(!mon.is_leader()) {
            dout(4) << "leader map: " << map <<  dendl;
        }
        check_subs(true);
    }
}

void NVMeofGwMon::check_sub(Subscription *sub)
{
   /* MgrMonitor::check_sub*/
    //if (sub->type == "NVMeofGw") {
    dout(10) << "sub->next , map-epoch " << sub->next << " " << map.epoch << dendl;
    if (sub->next <= map.epoch)
    {
      dout(4) << "Sending map to subscriber " << sub->session->con << " " << sub->session->con->get_peer_addr() << dendl;
      sub->session->con->send_message2(make_message<MNVMeofGwMap>(map));

      if (sub->onetime) {
        mon.session_map.remove_sub(sub);
      } else {
        sub->next = map.epoch + 1;
      }
    }
}

void NVMeofGwMon::check_subs(bool t)
{
  const std::string type = "NVMeofGw";
  dout(4) <<  "count " << mon.session_map.subs.count(type) << dendl;

  if (mon.session_map.subs.count(type) == 0){
      return;
  }
  for (auto sub : *(mon.session_map.subs[type])) {
    dout(4) << "sub-type "<< sub->type <<  " delay_propose until next tick" << t << dendl;
    if (t) map.delay_propose = true;
    else  check_sub(sub);
  }
}

bool NVMeofGwMon::preprocess_query(MonOpRequestRef op){
    dout(20) << dendl;

    auto m = op->get_req<PaxosServiceMessage>();
      switch (m->get_type()) {
        case MSG_MNVMEOF_GW_BEACON:
          return preprocess_beacon(op);

        case MSG_MON_COMMAND:
          try {
        return preprocess_command(op);
          } catch (const bad_cmd_get& e) {
          bufferlist bl;
          mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
          return true;
        }

        default:
          mon.no_reply(op);
          derr << "Unhandled message type " << m->get_type() << dendl;
          return true;
      }
    return false;
}

bool NVMeofGwMon::prepare_update(MonOpRequestRef op){
    auto m = op->get_req<PaxosServiceMessage>();
      switch (m->get_type()) {
        case MSG_MNVMEOF_GW_BEACON:
          return prepare_beacon(op);

        case MSG_MON_COMMAND:
          try {
        return prepare_command(op);
          } catch (const bad_cmd_get& e) {
        bufferlist bl;
        mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
        return false; /* nothing to propose! */
          }

        default:
          mon.no_reply(op);
          dout(1) << "Unhandled message type " << m->get_type() << dendl;
          return false; /* nothing to propose! */
      }
    return true;
}

bool NVMeofGwMon::preprocess_command(MonOpRequestRef op)
{
    dout(4) << dendl;
    auto m = op->get_req<MMonCommand>();
    std::stringstream ss;
    bufferlist rdata;

    cmdmap_t cmdmap;
    if (!cmdmap_from_json(m->cmd, &cmdmap, ss))
    {
        string rs = ss.str();
        dout(1) << "Invalid command "  << m->cmd << "Error " << rs << dendl;
        mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
        return true;
    }

    string prefix;
    cmd_getval(cmdmap, "prefix", prefix);
    dout(4) << "MonCommand : "<< prefix <<  dendl;

   /* MonSession *session = op->get_session();
    if (!session)
    {
        dout(4) << "MonCommand : "<< prefix << " access denied due to lack of session" <<  dendl;
        mon.reply_command(op, -EACCES, "access denied", rdata,
                          get_last_committed());
        return true;
    }
   */
    string format = cmd_getval_or<string>(cmdmap, "format", "plain");
    boost::scoped_ptr<Formatter> f(Formatter::create(format));

    // TODO   need to check formatter per preffix  - if f is NULL

    return false;
}

bool NVMeofGwMon::prepare_command(MonOpRequestRef op)
{
    dout(4)  << dendl;
    auto m = op->get_req<MMonCommand>();
    int rc;
    std::stringstream ss;
    bufferlist rdata;
    string rs;
    int err = 0;
    cmdmap_t cmdmap;

    if (!cmdmap_from_json(m->cmd, &cmdmap, ss))
    {
        string rs = ss.str();
        mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
        return true;
    }

    MonSession *session = op->get_session();
    if (!session)
    {
        mon.reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
        return true;
    }

    string format = cmd_getval_or<string>(cmdmap, "format", "plain");
    boost::scoped_ptr<Formatter> f(Formatter::create(format));

    const auto prefix = cmd_getval_or<string>(cmdmap, "prefix", string{});

    dout(4) << "MonCommand : "<< prefix <<  dendl;
    if( prefix == "nvme-gw create" || prefix == "nvme-gw delete" ) {
        std::string id, pool, group;

        cmd_getval(cmdmap, "id", id);
        cmd_getval(cmdmap, "pool", pool);
        cmd_getval(cmdmap, "group", group);
        auto group_key = std::make_pair(pool, group);
        dout(4) << " id "<< id <<" pool "<< pool << " group "<< group << dendl;
        if(prefix == "nvme-gw create"){
            rc = pending_map.cfg_add_gw(id, group_key);
            ceph_assert(rc!= -EINVAL);
        }
        else{
            rc = pending_map.cfg_delete_gw(id, group_key);
            if(rc== -EINVAL){
                dout (1) << "Error: GW not found in the database " << id << " " << pool << " " << group << "  rc " << rc << dendl;
                err = rc;
                ss.str("");
            }
        }
        if((rc != -EEXIST) && (rc != -EINVAL)){
            //propose pending would be generated by the PaxosService
            goto update;
        }
        else {
            goto reply_no_propose;
        }
    }
    else if ( prefix == "nvme-gw show" ){
        std::string  pool, group;
        if (!f) {
          f.reset(Formatter::create(format, "json-pretty", "json-pretty"));
        }
        cmd_getval(cmdmap, "pool", pool);
        cmd_getval(cmdmap, "group", group);
        auto group_key = std::make_pair(pool, group);
        dout(4) <<"nvme-gw show  pool "<< pool << " group "<< group << dendl;

        if( map.Created_gws[group_key].size()){
            f->open_object_section("common");
            f->dump_string("pool", pool);
            f->dump_string("group", group);
            f->dump_unsigned("num gws", map.Created_gws[group_key].size());
            ss <<"[ ";
            NvmeAnaGrpId anas[MAX_SUPPORTED_ANA_GROUPS];
            int i = 0;
            for (auto& gw_created_pair: map.Created_gws[group_key]) {
                 auto& st = gw_created_pair.second;
                 ss << st.ana_grp_id+1 << " ";
                 anas[i++] = st.ana_grp_id;
            }
            ss << "]";
            f->dump_string("Anagrp list", ss.str());
            f->close_section();

            for (auto& gw_created_pair: map.Created_gws[group_key]) {
                 auto& gw_id = gw_created_pair.first;
                 auto& state = gw_created_pair.second;
                 f->open_object_section("stat");
                 f->dump_string("gw-id", gw_id);
                 f->dump_unsigned("anagrp-id",state.ana_grp_id+1);
                 std::stringstream  ss1;
                 ss1 << state.availability;
                 f->dump_string("Availability", ss1.str());
                 ss1.str("");
                 for (size_t i = 0; i < map.Created_gws[group_key].size(); i++) {
                         ss1 << " " << anas[i]+1 <<": " << state.sm_state[anas[i]] << ",";
                 }
                 f->dump_string("ana states", ss1.str());
                 f->close_section();
            }
            f->flush(rdata);
            ss.str("");
        }
        else {
            ss << "num_gws  0";
        }
    }

  reply_no_propose:
    getline(ss, rs);
    if (err < 0 && rs.length() == 0)
    {
        rs = cpp_strerror(err);
        dout(1) << "Error command  err : "<< err  << " rs-len: " << rs.length() <<  dendl;
    }
    mon.reply_command(op, err, rs, rdata, get_last_committed());
    return false; /* nothing to propose */

  update:
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                            get_last_committed() + 1));
    return true;
}


bool NVMeofGwMon::preprocess_beacon(MonOpRequestRef op){
    //dout(4)   << dendl;
    auto m = op->get_req<MNVMeofGwBeacon>();
    const BeaconSubsystems& sub = m->get_subsystems();
     //mon.no_reply(op); // we never reply to beacons
     dout(10) << "beacon from " << m->get_type() << " GW : " << m->get_gw_id()  << " num subsystems " << sub.size() <<  dendl;
     MonSession *session = op->get_session();
     if (!session){
         dout(4) << "beacon no session "  << dendl;
         return true;
     }

    return false; // allways  return false to call leader's prepare beacon
}


//#define BYPASS_GW_CREATE_CLI

bool NVMeofGwMon::prepare_beacon(MonOpRequestRef op){
    //dout(4)  << dendl;
    auto m = op->get_req<MNVMeofGwBeacon>();

    dout(20) << "availability " <<  m->get_availability() << " GW : " << m->get_gw_id() <<
        " osdmap_epoch " << m->get_last_osd_epoch() << " subsystems " << m->get_subsystems() << dendl;

    NvmeGwId gw_id = m->get_gw_id();
    NvmeGroupKey group_key = std::make_pair(m->get_gw_pool(),  m->get_gw_group());
    GW_AVAILABILITY_E  avail = m->get_availability();
    bool propose = false;
    bool nonce_propose = false;
    bool timer_propose = false;
    bool gw_created = true;
    NVMeofGwMap ack_map;
    auto& group_gws = map.Created_gws[group_key];
    auto gw = group_gws.find(gw_id);
    const BeaconSubsystems& sub = m->get_subsystems();

    if (avail == GW_AVAILABILITY_E::GW_CREATED){
        if (gw == group_gws.end()) {
           gw_created = false;
           dout(4) << "Warning: GW " << gw_id << " group_key " << group_key << " was not found in the  map.Created_gws "<< map.Created_gws <<dendl;
        }
        goto set_propose;
    }

    // At this stage the gw has to be in the Created_gws
    if(gw == group_gws.end()){
        dout(1) << "Error : Administratively deleted GW sends beacon " << gw_id <<dendl;
        goto false_return; // not sending ack to this beacon
    }

    // deep copy the whole nonce map of this GW
    if(m->get_nonce_map().size()) {
        if(pending_map.Created_gws[group_key][gw_id].nonce_map != m->get_nonce_map())
        {
            dout(4) << "nonce map of GW  changed , propose pending " << gw_id << dendl;
            pending_map.Created_gws[group_key][gw_id].nonce_map = m->get_nonce_map();
            dout(4) << "nonce map of GW " << gw_id << " "<< pending_map.Created_gws[group_key][gw_id].nonce_map  << dendl;
            nonce_propose = true;
        }
    }
    else  {
        dout(4) << "Warning: received empty nonce map in the beacon of GW " << gw_id << " "<< dendl;
    }

    //pending_map.handle_removed_subsystems(gw_id, group_key, configured_subsystems, propose);

    //if  no subsystem configured set gw as avail = GW_AVAILABILITY_E::GW_UNAVAILABLE

    if(sub.size() == 0) {
        avail = GW_AVAILABILITY_E::GW_UNAVAILABLE;
    }
    pending_map.Created_gws[group_key][gw_id].subsystems =  sub;

    if(avail == GW_AVAILABILITY_E::GW_AVAILABLE)
    {
        //dout(4) <<"subsystems from beacon " << pending_map.Created_gws << dendl;
        auto now = ceph::coarse_mono_clock::now();
        // check pending_map.epoch vs m->get_version() - if different - drop the beacon

        LastBeacon lb = {gw_id, group_key};
        last_beacon[lb] = now;
        epoch_t last_osd_epoch = m->get_last_osd_epoch();
        pending_map.process_gw_map_ka(gw_id, group_key, last_osd_epoch, propose);
    }
    else if(avail == GW_AVAILABILITY_E::GW_UNAVAILABLE){ // state set by GW client application
        //  TODO: remove from last_beacon if found . if gw was found in last_beacon call process_gw_map_gw_down

        LastBeacon lb = {gw_id, group_key};

        auto it = last_beacon.find(lb);
        if (it != last_beacon.end()){
            last_beacon.erase(lb);
            pending_map.process_gw_map_gw_down(gw_id, group_key, propose);
        }
    }
    pending_map.update_active_timers(timer_propose);  // Periodic: check active FSM timers
    propose |= timer_propose;
    propose |= nonce_propose;

set_propose:
    if(!propose) {
      if(gw_created){
          ack_map.Created_gws[group_key][gw_id] = map.Created_gws[group_key][gw_id];// respond with a map slice correspondent to the same GW
      }
      ack_map.epoch = map.epoch;
      dout(20) << "ack_map " << ack_map <<dendl;
      auto msg = make_message<MNVMeofGwMap>(ack_map);
      mon.send_reply(op, msg.detach());
    }
false_return:
    if (propose){
      dout(4) << "decision to delayed_map in prepare_beacon" <<dendl;
      return true;
    }
    else 
     return false; // if no changes are need in the map
}
