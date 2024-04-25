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
class Monitor;
/*-------------------*/
class NVMeofGwMap
{
public:
    Monitor*                            mon           = NULL;
    epoch_t                             epoch         = 0;      // epoch is for Paxos synchronization  mechanizm
    bool                                delay_propose = false;

    std::map<NvmeGroupKey, NvmeGwCreatedMap>  Created_gws;
    std::map<NvmeGroupKey, NvmeGwMetaDataMap> Gmetadata;
    void to_gmap(std::map<NvmeGroupKey, NvmeGwMap>& Gmap) const;

    int   cfg_add_gw                    (const NvmeGwId &gw_id, const NvmeGroupKey& group_key);
    int   cfg_delete_gw                 (const NvmeGwId &gw_id, const NvmeGroupKey& group_key);
    void  process_gw_map_ka             (const NvmeGwId &gw_id, const NvmeGroupKey& group_key, epoch_t& last_osd_epoch,  bool &propose_pending);
    int   process_gw_map_gw_down        (const NvmeGwId &gw_id, const NvmeGroupKey& group_key, bool &propose_pending);
    void  update_active_timers          (bool &propose_pending);
    void  handle_abandoned_ana_groups   (bool &propose_pending);
    void  handle_removed_subsystems     (const NvmeGwId &gw_id, const NvmeGroupKey& group_key, const std::vector<NvmeNqnId> &current_subsystems, bool &propose_pending);
    void  start_timer (const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId anagrpid, uint8_t value);
private:
    NvmeGwCreated&   find_already_created_gw(const NvmeGwId &gw_id, const NvmeGroupKey& group_key);
    void fsm_handle_gw_down    (const NvmeGwId &gw_id, const NvmeGroupKey& group_key,  GW_STATES_PER_AGROUP_E state, NvmeAnaGrpId grpid,  bool &map_modified);
    void fsm_handle_gw_delete  (const NvmeGwId &gw_id, const NvmeGroupKey& group_key,  GW_STATES_PER_AGROUP_E state, NvmeAnaGrpId grpid,  bool &map_modified);
    void fsm_handle_gw_alive   (const NvmeGwId &gw_id, const NvmeGroupKey& group_key,  NvmeGwCreated & gw_state, GW_STATES_PER_AGROUP_E state,
                                                                                   NvmeAnaGrpId grpid, epoch_t& last_osd_epoch, bool &map_modified);
    void fsm_handle_to_expired (const NvmeGwId &gw_id, const NvmeGroupKey& group_key,  NvmeAnaGrpId grpid,  bool &map_modified);

    void find_failover_candidate(const NvmeGwId &gw_id, const NvmeGroupKey& group_key,  NvmeAnaGrpId grpid, bool &propose_pending);
    void find_failback_gw       (const NvmeGwId &gw_id, const NvmeGroupKey& group_key,  bool &propose_pending);
    void set_failover_gw_for_ANA_group (const NvmeGwId &failed_gw_id, const NvmeGroupKey& group_key, const NvmeGwId &gw_id,
                                                                                                     NvmeAnaGrpId groupid);
    int  blocklist_gw(const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId ANA_groupid, epoch_t &epoch, bool failover);

    int  get_timer   (const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId anagrpid);
    void cancel_timer(const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId anagrpid);
    void validate_gw_map(const NvmeGroupKey& group_key);

public:
    void encode(ceph::buffer::list &bl) const {
        using ceph::encode;
        ENCODE_START(1, 1, bl);
        encode(epoch, bl);// global map epoch

        encode(Created_gws, bl); //Encode created GWs
        encode(Gmetadata, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator &bl) {
        using ceph::decode;
        DECODE_START(1, bl);
        decode(epoch, bl);

        decode(Created_gws, bl);
        decode(Gmetadata, bl);
        DECODE_FINISH(bl);
    }
};

#include "NVMeofGwSerialize.h"

#endif /* SRC_MON_NVMEOFGWMAP_H_ */
