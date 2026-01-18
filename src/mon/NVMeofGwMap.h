// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#ifndef MON_NVMEOFGWMAP_H_
#define MON_NVMEOFGWMAP_H_
#include <map>
#include <iostream>
#include "include/encoding.h"
#include "include/utime.h"
#include "common/Formatter.h"
#include "common/ceph_releases.h"
#include "common/version.h"
#include "common/options.h"
#include "common/Clock.h"
#include "msg/Message.h"
#include "common/ceph_time.h"
#include "NVMeofGwTypes.h"

using ceph::coarse_mono_clock;

class health_check_map_t;

class Monitor;
/*-------------------*/
class NVMeofGwMap
{
public:
  Monitor *mon = NULL;

  // epoch is for Paxos synchronization  mechanizm
  epoch_t epoch = 0;
  bool delay_propose = false;

  std::map<NvmeGroupKey, NvmeGwMonStates>  created_gws;

  // map that handles timers started by all Gateway FSMs
  std::map<NvmeGroupKey, NvmeGwTimers> fsm_timers;
  /**
   * gw_epoch
   *
   * Mapping from NvmeGroupKey -> epoch_t e such that e is the most recent
   * map epoch which affects NvmeGroupKey.
   *
   * The purpose of this map is to allow us to determine whether a particular
   * gw needs to be sent the current map.  If a gw with NvmeGroupKey key already
   * has map epoch e, we only need to send a new map if gw_epoch[key] > e.  See
   * check_sub for this logic.
   *
   * Map mutators generally need to invoke increment_gw_epoch(group_key) when
   * updating the map with a change affecting gws in group_key.
   */
  std::map<NvmeGroupKey, epoch_t> gw_epoch;

  /* in stretched cluster configuration:
   * map stores disaster state per location - can be several locations in disaster state
   * location in disaster:
   *   failbacks/failovers to disaster location are not allowed
   * in disaster recovery state:
   *  periodic process to relocate ana groups, allowed failbacks between locations
   * disaster_locations map should be updated when last location is removed
   * */
  std::map<NvmeGroupKey, LocationStates> disaster_locations;

  void to_gmap(std::map<NvmeGroupKey, NvmeGwMonClientStates>& Gmap) const;
  void track_deleting_gws(const NvmeGroupKey& group_key,
    const BeaconSubsystems&  subs, bool &propose_pending);
  void check_all_gws_in_deleting_state(const NvmeGwId &gw_id,
    const NvmeGroupKey& group_key);
  int cfg_add_gw(const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    uint64_t features);
  int cfg_delete_gw(const NvmeGwId &gw_id, const NvmeGroupKey& group_key);
  int cfg_admin_state_change(const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
		  gw_admin_state_t state, bool &propose_pending);
  int cfg_set_location(const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
		  std::string &location, bool &propose_pending);
  bool is_last_gw_in_location(const NvmeGwId &gw_id,
              const NvmeGroupKey& group_key, NvmeLocation& location);
  int cfg_location_disaster_set(const NvmeGroupKey& group_key,
          std::string &location, bool &propose_pending);
  int cfg_location_disaster_clear(const NvmeGroupKey& group_key,
          std::string &location, bool &propose_pending);
  void process_gw_map_ka(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    epoch_t& last_osd_epoch,  bool &propose_pending);
  int process_gw_map_gw_down(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    bool &propose_pending);
  int process_gw_map_gw_no_subsys_no_listeners(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    bool &propose_pending);
  void update_active_timers(bool &propose_pending);
  void handle_abandoned_ana_groups(bool &propose_pending);
  void handle_removed_subsystems(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    const std::vector<NvmeNqnId> &current_subsystems, bool &propose_pending);
  void start_timer(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    NvmeAnaGrpId anagrpid, uint8_t value);
  void handle_gw_performing_fast_reboot(const NvmeGwId &gw_id,
       const NvmeGroupKey& group_key, bool &map_modified);
  void gw_performed_startup(const NvmeGwId &gw_id,
       const NvmeGroupKey& group_key, bool &propose_pending);
  void set_addr_vect(const NvmeGwId &gw_id,
      const NvmeGroupKey& group_key, const entity_addr_t &addr_vect);
  void skip_failovers_for_group(const NvmeGroupKey& group_key,
      int interval_sec = 0);
  bool put_gw_beacon_sequence_number(const NvmeGwId &gw_id, int gw_version,
      const NvmeGroupKey& group_key, uint64_t beacon_sequence,
      uint64_t& old_sequence);
  bool set_gw_beacon_sequence_number(const NvmeGwId &gw_id, int gw_version,
         const NvmeGroupKey& group_key, uint64_t beacon_sequence);
  bool is_location_in_disaster(const NvmeGroupKey& group_key,
             NvmeLocation& location, bool &cleanup_in_process);
private:
  int  do_delete_gw(const NvmeGwId &gw_id, const NvmeGroupKey& group_key);
  int  do_erase_gw_id(const NvmeGwId &gw_id,
      const NvmeGroupKey& group_key);
  void add_grp_id(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    const NvmeAnaGrpId grpid);
  void remove_grp_id(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    const NvmeAnaGrpId grpid);
  void fsm_handle_gw_down(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    gw_states_per_group_t state, NvmeAnaGrpId grpid,  bool &map_modified);
  void fsm_handle_gw_no_subsystems(
     const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
     gw_states_per_group_t state, NvmeAnaGrpId grpid,  bool &map_modified);
  void fsm_handle_gw_delete(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    gw_states_per_group_t state, NvmeAnaGrpId grpid,  bool &map_modified);
  void fsm_handle_gw_alive(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    NvmeGwMonState & gw_state, gw_states_per_group_t state,
    NvmeAnaGrpId grpid, epoch_t& last_osd_epoch, bool &map_modified);
  void fsm_handle_to_expired(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    NvmeAnaGrpId grpid,  bool &map_modified);
  void find_failover_candidate(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    NvmeAnaGrpId grpid, bool &propose_pending);
  void find_failback_gw(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    bool &propose_pending);
  void set_failover_gw_for_ANA_group(
    const NvmeGwId &failed_gw_id, const NvmeGroupKey& group_key,
    const NvmeGwId &gw_id, NvmeAnaGrpId groupid);
  int get_num_namespaces(const NvmeGwId &gw_id,
    const NvmeGroupKey& group_key, const BeaconSubsystems&  subs );
  int get_timer(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    NvmeAnaGrpId anagrpid);
  void cancel_timer(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    NvmeAnaGrpId anagrpid);
  void validate_gw_map(
    const NvmeGroupKey& group_key);
  void increment_gw_epoch(const NvmeGroupKey& group_key);
  int find_failover_gw_logic(const NvmeGroupKey& group_key,
    NvmeGwMonStates& gws_states, NvmeLocation& location,
    NvmeGwId& min_loaded_gw_id, bool ignore_locations);
  bool get_location_in_disaster_cleanup(const NvmeGroupKey& group_key,
             NvmeLocation& returned_location);
  void disaster_map_remove_location(const NvmeGroupKey& group_key,
             NvmeLocation& location);
  bool validate_number_locations(int num_gws, int num_locations);
  void check_relocate_ana_groups(const NvmeGroupKey& group_key,
           bool &propose);
  int relocate_ana_grp(const NvmeGwId &src_gw_id,
   const NvmeGroupKey& group_key, NvmeAnaGrpId grpid,
   NvmeLocation& location, bool &propose);

public:
  int blocklist_gw(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    NvmeAnaGrpId ANA_groupid, epoch_t &epoch, bool failover);

  void encode(ceph::buffer::list &bl, uint64_t features) const ;

  void decode(ceph::buffer::list::const_iterator &bl);
  void get_health_checks(health_check_map_t *checks);
};

#include "NVMeofGwSerialize.h"

#endif /* SRC_MON_NVMEOFGWMAP_H_ */
