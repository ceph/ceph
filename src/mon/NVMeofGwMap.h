/*
 * NVMeofGwMap.h
 *
 *  Created on: Oct 17, 2023
 *      Author: 227870756
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
#include "PaxosService.h"
#include "msg/Message.h"
#include "common/ceph_time.h"
#include "NVMeofGwTypes.h"

using ceph::coarse_mono_clock;
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define MODULE_PREFFIX "nvmeofgw "
#define dout_prefix *_dout << MODULE_PREFFIX << __PRETTY_FUNCTION__ << " "
/*-------------------*/
class NVMeofGwMap
{
public:
    Monitor*                            mon           = NULL;
    epoch_t                             epoch         = 0;      // epoch is for Paxos synchronization  mechanizm
    bool                                delay_propose = false;

    std::map<GROUP_KEY, GW_CREATED_MAP> Created_gws;
    std::map<GROUP_KEY, GWMETADATA>     Gmetadata;
    void to_gmap(std::map<GROUP_KEY, GWMAP>& Gmap) const;

    int   cfg_add_gw                    (const GW_ID_T &gw_id, const GROUP_KEY& group_key);
    int   cfg_delete_gw                 (const GW_ID_T &gw_id, const GROUP_KEY& group_key);
    void  process_gw_map_ka             (const GW_ID_T &gw_id, const GROUP_KEY& group_key, epoch_t& last_osd_epoch,  bool &propose_pending);
    int   process_gw_map_gw_down        (const GW_ID_T &gw_id, const GROUP_KEY& group_key, bool &propose_pending);
    void  update_active_timers          (bool &propose_pending);
    void  handle_abandoned_ana_groups   (bool &propose_pending);
    void  handle_removed_subsystems     (const GW_ID_T &gw_id, const GROUP_KEY& group_key, const std::vector<NQN_ID_T> &current_subsystems, bool &propose_pending);
    void  start_timer (const GW_ID_T &gw_id, const GROUP_KEY& group_key, ANA_GRP_ID_T anagrpid, uint8_t value);
private:
    GW_CREATED_T&   find_already_created_gw(const GW_ID_T &gw_id, const GROUP_KEY& group_key);
    void fsm_handle_gw_down    (const GW_ID_T &gw_id, const GROUP_KEY& group_key,  GW_STATES_PER_AGROUP_E state, ANA_GRP_ID_T grpid,  bool &map_modified);
    void fsm_handle_gw_delete  (const GW_ID_T &gw_id, const GROUP_KEY& group_key,  GW_STATES_PER_AGROUP_E state, ANA_GRP_ID_T grpid,  bool &map_modified);
    void fsm_handle_gw_alive   (const GW_ID_T &gw_id, const GROUP_KEY& group_key,  GW_CREATED_T & gw_state, GW_STATES_PER_AGROUP_E state,
                                                                                   ANA_GRP_ID_T grpid, epoch_t& last_osd_epoch, bool &map_modified);
    void fsm_handle_to_expired (const GW_ID_T &gw_id, const GROUP_KEY& group_key,  ANA_GRP_ID_T grpid,  bool &map_modified);

    void find_failover_candidate(const GW_ID_T &gw_id, const GROUP_KEY& group_key,  ANA_GRP_ID_T grpid, bool &propose_pending);
    void find_failback_gw       (const GW_ID_T &gw_id, const GROUP_KEY& group_key,  bool &propose_pending);
    void set_failover_gw_for_ANA_group (const GW_ID_T &failed_gw_id, const GROUP_KEY& group_key, const GW_ID_T &gw_id,
                                                                                                     ANA_GRP_ID_T groupid);
    int  blocklist_gw(const GW_ID_T &gw_id, const GROUP_KEY& group_key, ANA_GRP_ID_T ANA_groupid, epoch_t &epoch, bool failover);

    int  get_timer   (const GW_ID_T &gw_id, const GROUP_KEY& group_key, ANA_GRP_ID_T anagrpid);
    void cancel_timer(const GW_ID_T &gw_id, const GROUP_KEY& group_key, ANA_GRP_ID_T anagrpid);

public:
    static NVMeofGwMap create_null_map() {
        NVMeofGwMap null_map;
       /* Use the largest epoch so it's always bigger than whatever the MDS has. */
       null_map.epoch = std::numeric_limits<decltype(epoch)>::max();
       return null_map;
     }

    void encode(ceph::buffer::list &bl, bool full_encode = true) const {
        using ceph::encode;
        __u8 struct_v = 0;
        encode(struct_v, bl); // version
        encode(epoch, bl);// global map epoch

        encode(Created_gws, bl); //Encode created GWs
        if (full_encode) {
            encode(Gmetadata, bl);
        }
    }

    void decode(ceph::buffer::list::const_iterator &bl, bool full_decode = true) {
        using ceph::decode;
        __u8 struct_v;
        decode(struct_v, bl);
        //dout(0) << "version: " << struct_v << dendl;
        ceph_assert(struct_v == 0);
        decode(epoch, bl);
       // dout(0) << "epoch: " << epoch << dendl;

        decode(Created_gws, bl);
       //dout(0) << "Created_gws: " << Created_gws << dendl;
       // dout(0) << "Gmap: " << Gmap << dendl;
        if (full_decode) {
            decode(Gmetadata, bl);
        }
    }
};

#undef dout_subsys
#include "NVMeofGwSerialize.h"

#endif /* SRC_MON_NVMEOFGWMAP_H_ */
