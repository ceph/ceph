// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

/* 
 * This is the top level monitor. It runs on each machine in the Monitor   
 * Cluster. The election of a leader for the paxos algorithm only happens 
 * once per machine via the elector. There is a separate paxos instance (state) 
 * kept for each of the system components: Object Store Device (OSD) Monitor, 
 * Placement Group (PG) Monitor, Metadata Server (MDS) Monitor, and Client Monitor.
 */

#ifndef CEPH_MONITOR_H
#define CEPH_MONITOR_H

#include "include/types.h"
#include "msg/Messenger.h"

#include "common/Timer.h"

#include "MonMap.h"
#include "Elector.h"
#include "Paxos.h"
#include "Session.h"

#include "osd/OSDMap.h"

#include "common/LogClient.h"
#include "common/SimpleRNG.h"
#include "common/cmdparse.h"

#include "auth/cephx/CephxKeyServer.h"
#include "auth/AuthMethodList.h"
#include "auth/KeyRing.h"

#include "perfglue/heap_profiler.h"

#include "messages/MMonCommand.h"
#include "mon/MonitorStore.h"
#include "mon/MonitorDBStore.h"

#include <memory>
#include <tr1/memory>
#include <errno.h>


#define CEPH_MON_PROTOCOL     11 /* cluster internal */


enum {
  l_cluster_first = 555000,
  l_cluster_num_mon,
  l_cluster_num_mon_quorum,
  l_cluster_num_osd,
  l_cluster_num_osd_up,
  l_cluster_num_osd_in,
  l_cluster_osd_epoch,
  l_cluster_osd_kb,
  l_cluster_osd_kb_used,
  l_cluster_osd_kb_avail,
  l_cluster_num_pool,
  l_cluster_num_pg,
  l_cluster_num_pg_active_clean,
  l_cluster_num_pg_active,
  l_cluster_num_pg_peering,
  l_cluster_num_object,
  l_cluster_num_object_degraded,
  l_cluster_num_object_unfound,
  l_cluster_num_bytes,
  l_cluster_num_mds_up,
  l_cluster_num_mds_in,
  l_cluster_num_mds_failed,
  l_cluster_mds_epoch,
  l_cluster_last,
};

class QuorumService;
class PaxosService;

class PerfCounters;
class AdminSocketHook;

class MMonGetMap;
class MMonGetVersion;
class MMonSync;
class MMonProbe;
class MMonSubscribe;
class MAuthRotating;
class MRoute;
class MForward;
class MTimeCheck;
class MMonHealth;

#define COMPAT_SET_LOC "feature_set"

class Monitor : public Dispatcher {
public:
  // me
  string name;
  int rank;
  Messenger *messenger;
  Mutex lock;
  SafeTimer timer;
  
  /// true if we have ever joined a quorum.  if false, we are either a
  /// new cluster, a newly joining monitor, or a just-upgraded
  /// monitor.
  bool has_ever_joined;

  PerfCounters *logger, *cluster_logger;
  bool cluster_logger_registered;

  void register_cluster_logger();
  void unregister_cluster_logger();

  MonMap *monmap;

  set<entity_addr_t> extra_probe_peers;

  LogClient clog;
  KeyRing keyring;
  KeyServer key_server;

  AuthMethodList auth_cluster_required;
  AuthMethodList auth_service_required;

  CompatSet features;

private:
  void new_tick();
  friend class C_Mon_Tick;

  // -- local storage --
public:
  MonitorDBStore *store;
  static const string MONITOR_NAME;
  static const string MONITOR_STORE_PREFIX;

  // -- monitor state --
private:
  enum {
    STATE_PROBING = 1,
    STATE_SYNCHRONIZING,
    STATE_ELECTING,
    STATE_LEADER,
    STATE_PEON,
    STATE_SHUTDOWN
  };
  int state;

public:
  static const char *get_state_name(int s) {
    switch (s) {
    case STATE_PROBING: return "probing";
    case STATE_SYNCHRONIZING: return "synchronizing";
    case STATE_ELECTING: return "electing";
    case STATE_LEADER: return "leader";
    case STATE_PEON: return "peon";
    default: return "???";
    }
  }
  const string get_state_name() const {
    string sn(get_state_name(state));
    string sync_name(get_sync_state_name());
    sn.append(sync_name);
    return sn;
  }

  bool is_probing() const { return state == STATE_PROBING; }
  bool is_synchronizing() const { return state == STATE_SYNCHRONIZING; }
  bool is_electing() const { return state == STATE_ELECTING; }
  bool is_leader() const { return state == STATE_LEADER; }
  bool is_peon() const { return state == STATE_PEON; }

  const utime_t &get_leader_since() const;

  // -- elector --
private:
  Paxos *paxos;
  Elector elector;
  friend class Elector;
  
  int leader;            // current leader (to best of knowledge)
  set<int> quorum;       // current active set of monitors (if !starting)
  utime_t leader_since;  // when this monitor became the leader, if it is the leader
  utime_t exited_quorum; // time detected as not in quorum; 0 if in
  uint64_t quorum_features;  ///< intersection of quorum member feature bits

  set<string> outside_quorum;

  /**
   * @defgroup Synchronization
   * @{
   */
  /**
   * Obtain the synchronization target prefixes in set form.
   *
   * We consider a target prefix all those that are relevant when
   * synchronizing two stores. That is, all those that hold paxos service's
   * versions, as well as paxos versions, or any control keys such as the
   * first or last committed version.
   *
   * Given the current design, this function should return the name of all and
   * any available paxos service, plus the paxos name.
   *
   * @returns a set of strings referring to the prefixes being synchronized
   */
  set<string> get_sync_targets_names();
  /**
   * Handle a sync-related message
   *
   * This function will call the appropriate handling functions for each
   * operation type.
   *
   * @param m A sync-related message (i.e., of type MMonSync)
   */
  void handle_sync(MMonSync *m);
  /**
   * Handle a sync-related message of operation type OP_ABORT.
   *
   * @param m A sync-related message of type OP_ABORT
   */
  void handle_sync_abort(MMonSync *m);
  /**
   * Reset the monitor's sync-related data structures and state.
   */
  void reset_sync(bool abort = false);

  /**
   * @defgroup Synchronization_Roles
   * @{
   */
  /**
   * The monitor has no role in any on-going synchronization.
   */
  static const uint8_t SYNC_ROLE_NONE	    = 0x0;
  /**
   * The monitor is the Leader in at least one synchronization.
   */
  static const uint8_t SYNC_ROLE_LEADER	    = 0x1;
  /**
   * The monitor is the Provider in at least one synchronization.
   */
  static const uint8_t SYNC_ROLE_PROVIDER   = 0x2;
  /**
   * The monitor is a requester in the on-going synchronization.
   */
  static const uint8_t SYNC_ROLE_REQUESTER  = 0x4;

  /**
   * The monitor's current role in on-going synchronizations, if any.
   *
   * A monitor can either be part of no synchronization at all, in which case
   * @p sync_role shall hold the value @p SYNC_ROLE_NONE, or it can be part of
   * an on-going synchronization, in which case it may be playing either one or
   * two roles at the same time:
   *
   *  - If the monitor is the sync requester (i.e., be the one synchronizing
   *    against some other monitor), the @p sync_role field will hold only the
   *    @p SYNC_ROLE_REQUESTER value.
   *  - Otherwise, the monitor can be either a sync leader, or a sync provider,
   *    or both, in which case @p sync_role will hold a binary OR of both
   *    @p SYNC_ROLE_LEADER and @p SYNC_ROLE_PROVIDER.
   */
  uint8_t sync_role;
  /**
   * @}
   */
  /**
   * @defgroup Leader-specific
   * @{
   */
  /**
   * Guarantee mutual exclusion access to the @p trim_timeouts map.
   *
   * We need this mutex specially when we have a monitor starting a sync with
   * the leader and another one finishing or aborting an on-going sync, that
   * happens to be the last on-going trim on the map. Given that we will
   * enable the Paxos trim once we deplete the @p trim_timeouts map, we must
   * then ensure that we either add the new sync start to the map before
   * removing the one just finishing, or that we remove the finishing one
   * first and enable the trim before we add the new one. If we fail to do
   * this, nasty repercussions could follow.
   */
  Mutex trim_lock;
  /**
   * Map holding all on-going syncs' timeouts.
   *
   * An on-going sync leads to the Paxos trim to be suspended, and this map
   * will associate entities to the timeouts to be triggered if the monitor
   * being synchronized fails to check-in with the leader, letting him know
   * that the sync is still in effect and that in no circumstances should the
   * Paxos trim be enabled.
   */
  map<entity_inst_t, Context*> trim_timeouts;
  map<entity_inst_t, uint8_t> trim_entities_states;
  /**
   * Map associating monitors to a sync state.
   *
   * This map is used by both the Leader and the Sync Provider, and has the
   * sole objective of keeping track of the state each monitor's sync process
   * is in.
   */
  map<entity_inst_t, uint8_t> sync_entities_states;
  /**
   * Timer that will enable the Paxos trim.
   *
   * This timer is set after the @p trim_timeouts map is depleted, and once
   * fired it will enable the Paxos trim (if still disabled). By setting
   * this timer, we avoid a scenario in which a monitor has just finished
   * synchronizing, but because the Paxos trim had been disabled for a long,
   * long time and a lot of trims were proposed in the timespan of the monitor
   * finishing its sync and actually joining the cluster, the monitor happens
   * to be out-of-sync yet again. Backing off enabling the Paxos trim will
   * allow the other monitor to join the cluster before actually trimming.
   */
  Context *trim_enable_timer;

  /**
   * Callback class responsible for finishing a monitor's sync session on the
   * leader's side, because the said monitor failed to acknowledge its
   * liveliness in a timely manner, thus being assumed as failed.
   */
  struct C_TrimTimeout : public Context {
    Monitor *mon;
    entity_inst_t entity;

    C_TrimTimeout(Monitor *m, entity_inst_t& entity)
      : mon(m), entity(entity) { }
    void finish(int r) {
      mon->sync_finish(entity);
    }
  };

  /**
   * Callback class responsible for enabling the Paxos trim if there are no
   * more on-going syncs.
   */
  struct C_TrimEnable : public Context {
    Monitor *mon;

    C_TrimEnable(Monitor *m) : mon(m) { }
    void finish(int r) {
      Mutex::Locker(mon->trim_lock);
      // even if we are no longer the leader, we should re-enable trim if
      // we have disabled it in the past. It doesn't mean we are going to
      // do anything about it, but if we happen to become the leader
      // sometime down the future, we sure want to have the trim enabled.
      if (mon->trim_timeouts.empty())
	mon->paxos->trim_enable();
      mon->trim_enable_timer = NULL;
    }
  };

  void sync_store_init();
  void sync_store_cleanup();
  bool is_sync_on_going();

  /**
   * Send a heartbeat message to another entity.
   *
   * The sent message may be a heartbeat reply if the @p reply parameter is
   * set to true.
   *
   * This function is used both by the leader (always with @p reply = true),
   * and by the sync requester (always with @p reply = false).
   *
   * @param other The target monitor's entity instance.
   * @param reply Whether the message to be sent should be a heartbeat reply.
   */
  void sync_send_heartbeat(entity_inst_t &other, bool reply = false);
  /**
   * Handle a Sync Start request.
   *
   * Monitors wanting to synchronize with the cluster will have to first ask
   * the leader to do so. The only objective with this is so that the we can
   * gurantee that the leader won't trim the paxos state.
   *
   * The leader may not be the only one receiving this request. A sync provider
   * may also receive it when it is taken as the point of entry onto the
   * cluster. In this scenario, the provider must then forward this request to
   * the leader, if he know of one, or assume himself as the leader for this
   * sync purpose (this may happen if there is no formed quorum).
   *
   * @param m Sync message with operation type MMonSync::OP_START
   */
  void handle_sync_start(MMonSync *m);
  /**
   * Handle a Heartbeat sent by a sync requester.
   *
   * We use heartbeats as a way to guarantee that both the leader and the sync
   * requester are still alive. Receiving this message means that the requester
   * if still working on getting his store synchronized.
   *
   * @param m Sync message with operation type MMonSync::OP_HEARTBEAT
   */
  void handle_sync_heartbeat(MMonSync *m);
  /**
   * Handle a Sync Finish.
   *
   * A MMonSync::OP_FINISH is the way the sync requester has to inform the
   * leader that he finished synchronizing his store.
   *
   * @param m Sync message with operation type MMonSync::OP_FINISH
   */
  void handle_sync_finish(MMonSync *m);
  /**
   * Finish a given monitor's sync process on the leader's side.
   *
   * This means cleaning up the state referring to the monitor whose sync has
   * finished (may it have been finished successfully, by receiving a message
   * with type MMonSync::OP_FINISH, or due to the assumption that the said
   * monitor failed).
   *
   * If we happen to know of no other monitor synchronizing, we may then enable
   * the paxos trim.
   *
   * @param entity Entity instance of the monitor whose sync we are considering
   *		   as finished.
   * @param abort If true, we consider this sync has finished due to an abort.
   */
  void sync_finish(entity_inst_t &entity, bool abort = false);
  /**
   * Abort a given monitor's sync process on the leader's side.
   *
   * This function is a wrapper for Monitor::sync_finish().
   *
   * @param entity Entity instance of the monitor whose sync we are aborting.
   */
  void sync_finish_abort(entity_inst_t &entity) {
    sync_finish(entity, true);
  }
  /**
   * @} // Leader-specific
   */
  /**
   * @defgroup Synchronization Provider-specific
   * @{
   */
  /**
   * Represents a participant in a synchronization, along with its state.
   *
   * This class is used to track down all the sync requesters we are providing
   * to. In such scenario, it won't be uncommon to have the @p synchronizer
   * field set with a connection to the MonitorDBStore, the @p timeout field
   * containing a timeout event and @p entity containing the entity instance
   * of the monitor we are here representing.
   *
   * The sync requester will also use this class to represent both the sync
   * leader and the sync provider.
   */
  struct SyncEntityImpl {

    /**
     * Store synchronization related Sync state.
     */
    enum {
      /**
       * No state whatsoever. We are not providing any sync suppport.
       */
      STATE_NONE   = 0,
      /**
       * This entity's sync effort is currently focused on reading and sharing
       * our whole store state with @p entity. This means all the entries in
       * the key/value space.
       */
      STATE_WHOLE  = 1,
      /**
       * This entity's sync effor is currently focused on reading and sharing
       * our Paxos state with @p entity. This means all the Paxos-related
       * key/value entries, such as the Paxos versions.
       */
      STATE_PAXOS  = 2
    };

    /**
     * The entity instace of the monitor whose sync effort we are representing.
     */
    entity_inst_t entity;
    /**
     * Our Monitor.
     */
    Monitor *mon;
    /**
     * The Paxos version we are focusing on.
     *
     * @note This is not used at the moment. We are still assessing whether we
     *	     need it.
     */
    version_t version;
    /**
     * Timeout event. Its type and purpose varies depending on the situation.
     */
    Context *timeout;
    /**
     * Last key received during a sync effort.
     *
     * This field is mainly used by the sync requester to track the last
     * received key, in case he needs to switch providers due to failure. The
     * sync provider will also use this field whenever the requester specifies
     * a last received key when requesting the provider to start sending his
     * store chunks.
     */
    pair<string,string> last_received_key;
    /**
     * Hold the Store Synchronization related Sync State.
     */
    int sync_state;
    /**
     * The MonitorDBStore's chunk iterator instance we are currently using
     * to obtain the store's chunks and pack them to the sync requester.
     */
    MonitorDBStore::Synchronizer synchronizer;
    MonitorDBStore::Synchronizer paxos_synchronizer;
    /* Should only be used for debugging purposes */
    /**
     * crc of the contents read from the store.
     *
     * @note may not always be available, as it is used only on specific
     *	     points in time during the sync process.
     * @note depends on '--mon-sync-debug' being set.
     */
    __u32 crc;
    /**
     * Should be true if @p crc has been set.
     */
    bool crc_available;
    /**
     * Total synchronization attempts.
     */
    int attempts;

    SyncEntityImpl(entity_inst_t &entity, Monitor *mon)
      : entity(entity),
	mon(mon),
	version(0),
	timeout(NULL),
	sync_state(STATE_NONE),
	crc(0),
	crc_available(false),
	attempts(0)
    { }

    /**
     * Obtain current Sync State name.
     *
     * @returns Name of current sync state.
     */
    string get_state() {
      switch (sync_state) {
	case STATE_NONE: return "none";
	case STATE_WHOLE: return "whole";
	case STATE_PAXOS: return "paxos";
	default: return "unknown";
      }
    }
    /**
     * Obtain the paxos version at which this sync started.
     *
     * @returns Paxos version at which this sync started
     */
    version_t get_version() {
      return version;
    }
    /**
     * Set a timeout event for this sync entity.
     *
     * @param event Timeout class to be called after @p fire_after seconds.
     * @param fire_after Number of seconds until we fire the @p event event.
     */
    void set_timeout(Context *event, double fire_after) {
      cancel_timeout();
      timeout = event;
      mon->timer.add_event_after(fire_after, timeout);
    }
    /**
     * Cancel the currently set timeout, if any.
     */
    void cancel_timeout() {
      if (timeout)
	mon->timer.cancel_event(timeout);
      timeout = NULL;
    }
    /**
     * Initiate the required fields for obtaining chunks out of the
     * MonitorDBStore.
     *
     * This function will initiate @p synchronizer with a chunk iterator whose
     * scope is all the keys/values that belong to one of the sync targets
     * (i.e., paxos services or paxos).
     *
     * Calling @p Monitor::sync_update() will be essential during the efforts
     * of providing a correct store state to the requester, since we will need
     * to eventually update the iterator in order to start packing the Paxos
     * versions.
     */
    void sync_init() {
      sync_state = STATE_WHOLE;
      set<string> sync_targets = mon->get_sync_targets_names();

      string prefix("paxos");
      paxos_synchronizer = mon->store->get_synchronizer(prefix);
      version = mon->paxos->get_version();
      generic_dout(10) << __func__ << " version " << version << dendl;

      synchronizer = mon->store->get_synchronizer(last_received_key,
						  sync_targets);
      sync_update();
      assert(synchronizer->has_next_chunk());
    }
    /**
     * Update the @p synchronizer chunk iterator, if needed.
     *
     * Whenever we reach the end of the iterator during @p STATE_WHOLE, we
     * must update the @p synchronizer to an iterator focused on reading only
     * Paxos versions. This is an essential part of the sync store approach,
     * and it will guarantee that we end up with a consistent store.
     */
    void sync_update() {
      assert(sync_state != STATE_NONE);
      assert(synchronizer.use_count() != 0);

      if (!synchronizer->has_next_chunk()) {
	crc_set(synchronizer->crc());
	if (sync_state == STATE_WHOLE) {
          assert(paxos_synchronizer.use_count() != 0);
	  sync_state = STATE_PAXOS;
          synchronizer = paxos_synchronizer;
	}
      }
    }

    /* For debug purposes only */
    /**
     * Check if we have a CRC available.
     *
     * @returns true if crc is available; false otherwise.
     */
    bool has_crc() {
      return (g_conf->mon_sync_debug && crc_available);
    }
    /**
     * Set @p crc to @p to_set
     *
     * @param to_set a crc value to set.
     */
    void crc_set(__u32 to_set) {
      crc = to_set;
      crc_available = true;
    }
    /**
     * Get the current CRC value from @p crc
     *
     * @returns the currenct CRC value from @p crc
     */
    __u32 crc_get() {
      return crc;
    }
    /**
     * Clear the current CRC.
     */
    void crc_clear() {
      crc_available = false;
    }
  };
  typedef std::tr1::shared_ptr< SyncEntityImpl > SyncEntity;
  /**
   * Get a Monitor::SyncEntity instance.
   *
   * @param entity The monitor's entity instance that we want to associate
   *		   with this Monitor::SyncEntity.
   * @param mon The Monitor.
   *
   * @returns A Monitor::SyncEntity
   */
  SyncEntity get_sync_entity(entity_inst_t &entity, Monitor *mon) {
    return std::tr1::shared_ptr<SyncEntityImpl>(
	new SyncEntityImpl(entity, mon));
  }
  /**
   * Callback class responsible for dealing with the consequences of a sync
   * process timing out.
   */
  struct C_SyncTimeout : public Context {
    Monitor *mon;
    entity_inst_t entity;

    C_SyncTimeout(Monitor *mon, entity_inst_t &entity)
      : mon(mon), entity(entity)
    { }

    void finish(int r) {
      mon->sync_timeout(entity);
    }
  };
  /**
   * Map containing all the monitor entities to whom we are acting as sync
   * providers.
   */
  map<entity_inst_t, SyncEntity> sync_entities;
  /**
   * RNG used for the sync (currently only used to pick random monitors)
   */
  SimpleRNG sync_rng;
  /**
   * Obtain random monitor from the monmap.
   *
   * @param other Any monitor other than the one with rank @p other
   * @returns The picked monitor's name.
   */
  int _pick_random_mon(int other = -1);
  int _pick_random_quorum_mon(int other = -1);
  /**
   * Deal with the consequences of @p entity's sync timing out.
   *
   * @note Both the sync provider and the sync requester make use of this
   *	   function, since both use the @p Monitor::C_SyncTimeout callback.
   *
   * Being the sync provider, whenever a Monitor::C_SyncTimeout is triggered,
   * we only have to clean up the sync requester's state we are maintaining.
   *
   * Being the sync requester, we will have to choose a new sync provider, and
   * resume our sync from where it was left.
   *
   * @param entity Entity instance of the monitor whose sync has timed out.
   */
  void sync_timeout(entity_inst_t &entity);
  /**
   * Cleanup the state we, the provider, are keeping during @p entity's sync.
   *
   * @param entity Entity instance of the monitor whose sync state we are
   *		   cleaning up.
   */
  void sync_provider_cleanup(entity_inst_t &entity);
  /**
   * Handle a Sync Start Chunks request from a sync requester.
   *
   * This request will create the necessary state our the provider's end, and
   * the provider will then be able to send chunks of his own store to the
   * requester.
   *
   * @param m Sync message with operation type MMonSync::OP_START_CHUNKS
   */
  void handle_sync_start_chunks(MMonSync *m);
  /**
   * Handle a requester's reply to the last chunk we sent him.
   *
   * We will only send a new chunk to the sync requester once he has acked the
   * reception of the last chunk we sent them.
   *
   * That's also how we will make sure that, on their end, they became aware
   * that there are no more chunks to send (since we shall tag a message with
   * MMonSync::FLAG_LAST when we are sending them the last chunk of all),
   * allowing us to clean up the requester's state.
   *
   * @param m Sync message with operation type MMonSync::OP_CHUNK_REPLY
   */
  void handle_sync_chunk_reply(MMonSync *m);
  /**
   * Send a chunk to the sync entity represented by @p sync.
   *
   * This function will send the next chunk available on the synchronizer. If
   * it happens to be the last chunk, then the message shall be marked as
   * such using MMonSync::FLAG_LAST.
   *
   * @param sync A Monitor::SyncEntity representing a sync requester monitor.
   */
  void sync_send_chunks(SyncEntity sync);
  /**
   * @} // Synchronization Provider-specific
   */
  /**
   * @defgroup Synchronization Requester-specific
   * @{
   */
  /**
   * The state in which we (the sync leader, provider or requester) are in
   * regard to our sync process (if we are the requester) or any entity that
   * we may be leading or providing to.
   */
  enum {
    /**
     * We are not part of any synchronization effort, or it has not began yet.
     */
    SYNC_STATE_NONE   = 0,
    /**
     * We have started our role in the synchronization.
     *
     * This state may have multiple meanings, depending on which entity is
     * employing it and within which context.
     *
     * For instance, the leader will consider a sync requester to enter
     * SYNC_STATE_START whenever it receives a MMonSync::OP_START from the
     * said requester. On the other hand, the provider will consider that the
     * requester enters this state after receiving a MMonSync::OP_START_CHUNKS.
     * The sync requester will enter this state as soon as it begins its sync
     * efforts.
     */
    SYNC_STATE_START  = 1,
    /**
     * We are synchronizing chunks.
     *
     * This state is not used by the sync leader; only the sync requester and
     * the sync provider will.
     */
    SYNC_STATE_CHUNKS = 2,
    /**
     * We are stopping the sync effort.
     */
    SYNC_STATE_STOP   = 3
  };
  /**
   * The current sync state.
   *
   * This field is only used by the sync requester, being the only one that
   * will take this state as part of its global state. The sync leader and the
   * sync provider will only associate sync states to other entities (i.e., to
   * sync requesters), and those shall be kept in the @p sync_entities_states
   * map.
   */
  int sync_state;
  /**
   * Callback class responsible for dealing with the consequences of the sync
   * requester not receiving a MMonSync::OP_START_REPLY in a timely manner.
   */
  struct C_SyncStartTimeout : public Context {
    Monitor *mon;

    C_SyncStartTimeout(Monitor *mon)
      : mon(mon)
    { }

    void finish(int r) {
      mon->sync_start_reply_timeout();
    }
  };
  /**
   * Callback class responsible for retrying a Sync Start after a given
   * backoff period, whenever the Sync Leader flags a MMonSync::OP_START_REPLY
   * with the MMonSync::FLAG_RETRY flag.
   */
  struct C_SyncStartRetry : public Context {
    Monitor *mon;
    entity_inst_t entity;

    C_SyncStartRetry(Monitor *mon, entity_inst_t &entity)
      : mon(mon), entity(entity)
    { }

    void finish(int r) {
      mon->bootstrap();
    }
  };
  /**
   * We use heartbeats to check if both the Leader and the Synchronization
   * Requester are both still alive, so we can determine if we should continue
   * with the synchronization process, granted that trim is disabled.
   */
  struct C_HeartbeatTimeout : public Context {
    Monitor *mon;

    C_HeartbeatTimeout(Monitor *mon)
      : mon(mon)
    { }

    void finish(int r) {
      mon->sync_requester_abort();
    }
  };
  /**
   * Callback class responsible for sending a heartbeat message to the sync
   * leader. We use this callback to keep an assynchronous heartbeat with
   * the sync leader at predefined intervals.
   */
  struct C_HeartbeatInterval : public Context {
    Monitor *mon;
    entity_inst_t entity;

    C_HeartbeatInterval(Monitor *mon, entity_inst_t &entity)
      : mon(mon), entity(entity)
    { }

    void finish(int r) {
      mon->sync_leader->set_timeout(new C_HeartbeatTimeout(mon),
				    g_conf->mon_sync_heartbeat_timeout);
      mon->sync_send_heartbeat(entity);
    }
  };
  /**
   * Callback class responsible for dealing with the consequences of never
   * receiving a reply to a MMonSync::OP_FINISH sent to the sync leader.
   */
  struct C_SyncFinishReplyTimeout : public Context {
    Monitor *mon;

    C_SyncFinishReplyTimeout(Monitor *mon)
      : mon(mon)
    { }

    void finish(int r) {
      mon->sync_finish_reply_timeout();
    }
  };
  /**
   * The entity we, the sync requester, consider to be our sync leader. If
   * there is a formed quorum, the @p sync_leader should represent the actual
   * cluster Leader; otherwise, it can be any monitor and will likely be the
   * same as @p sync_provider.
   */
  SyncEntity sync_leader;
  /**
   * The entity we, the sync requester, are synchronizing against. This entity
   * will be our source of store chunks, and we will ultimately obtain a store
   * state equal (or very similar, maybe off by a couple of versions) as their
   * own.
   */
  SyncEntity sync_provider;
  /**
   * Clean up the Sync Requester's state (both in-memory and in-store).
   */
  void sync_requester_cleanup();
  /**
   * Abort the current sync effort.
   *
   * This will be translated into a MMonSync::OP_ABORT sent to the sync leader
   * and to the sync provider, and ultimately it will also involve calling
   * @p Monitor::sync_requester_cleanup() to clean up our current sync state.
   */
  void sync_requester_abort();
  /**
   * Deal with a timeout while waiting for a MMonSync::OP_FINISH_REPLY.
   *
   * This will be assumed as a leader failure, and having been exposed to the
   * side-effects of a new Leader being elected, we have no other choice but
   * to abort our sync process and start fresh.
   */
  void sync_finish_reply_timeout();
  /**
   * Deal with a timeout while waiting for a MMonSync::OP_START_REPLY.
   *
   * This will be assumed as a leader failure. Since we didn't get to do
   * much work (as we haven't even started our sync), we will simply bootstrap
   * and start off fresh with a new sync leader.
   */
  void sync_start_reply_timeout();
  /**
   * Start the synchronization efforts.
   *
   * This function should be called whenever we find the need to synchronize
   * our store state with the remaining cluster.
   *
   * Starting the sync process means that we will have to request the cluster
   * Leader (if there is a formed quorum) to stop trimming the Paxos state and
   * allow us to start synchronizing with the sync provider we picked.
   *
   * @param entity An entity instance referring to the sync provider we picked.
   */
  void sync_start(entity_inst_t &entity);
  /**
   * Request the provider to start sending the chunks of his store, in order
   * for us to obtain a consistent store state similar to the one shared by
   * the cluster.
   *
   * @param provider The SyncEntity representing the Sync Provider.
   */
  void sync_start_chunks(SyncEntity provider);
  /**
   * Handle a MMonSync::OP_START_REPLY sent by the Sync Leader.
   *
   * Reception of this message may be twofold: if it was marked with the
   * MMonSync::FLAG_RETRY flag, we must backoff for a while and retry starting
   * the sync at a later time; otherwise, we have the green-light to request
   * the Sync Provider to start sharing his chunks with us.
   *
   * @param m Sync message with operation type MMonSync::OP_START_REPLY
   */
  void handle_sync_start_reply(MMonSync *m);
  /**
   * Handle a Heartbeat reply sent by the Sync Leader.
   *
   * We use heartbeats to keep the Sync Leader aware that we are keeping our
   * sync efforts alive. We also use them to make sure our Sync Leader is
   * still alive. If the Sync Leader fails, we will have to abort our on-going
   * sync, or we could incurr in an inconsistent store state due to a trim on
   * the Paxos state of the monitor provinding us with his store chunks.
   *
   * @param m Sync message with operation type MMonSync::OP_HEARTBEAT_REPLY
   */
  void handle_sync_heartbeat_reply(MMonSync *m);
  /**
   * Handle a chunk sent by the Sync Provider.
   *
   * We will receive the Sync Provider's store in chunks. These are encoded
   * in bufferlists containing a transaction that will be directly applied
   * onto our MonitorDBStore.
   *
   * Whenever we receive such a message, we must reply to the Sync Provider,
   * as a way of acknowledging the reception of its last chunk. If the message
   * is tagged with a MMonSync::FLAG_LAST, we can then consider we have
   * received all the chunks the Sync Provider had to offer, and finish our
   * sync efforts with the Sync Leader.
   *
   * @param m Sync message with operation type MMonSync::OP_CHUNK
   */
  void handle_sync_chunk(MMonSync *m);
  /**
   * Handle a reply sent by the Sync Leader to a MMonSync::OP_FINISH.
   *
   * As soon as we receive this message, we know we finally have a store state
   * consistent with the remaining cluster (give or take a couple of versions).
   * We may then bootstrap and attempt to join the other monitors in the
   * cluster.
   *
   * @param m Sync message with operation type MMonSync::OP_FINISH_REPLY
   */
  void handle_sync_finish_reply(MMonSync *m);
  /**
   * Stop our synchronization effort by sending a MMonSync::OP_FINISH to the
   * Sync Leader.
   *
   * Once we receive the last chunk from the Sync Provider, we are in
   * conditions of officially finishing our sync efforts. With that purpose in
   * mind, we must then send a MMonSync::OP_FINISH to the Leader, letting him
   * know that we no longer require the Paxos state to be preserved.
   */
  void sync_stop();
  /**
   * @} // Synchronization Requester-specific
   */
  const string get_sync_state_name(int s) const {
    switch (s) {
    case SYNC_STATE_NONE: return "none";
    case SYNC_STATE_START: return "start";
    case SYNC_STATE_CHUNKS: return "chunks";
    case SYNC_STATE_STOP: return "stop";
    }
    return "???";
  }
  /**
   * Obtain a string describing the current Sync State.
   *
   * @returns A string describing the current Sync State, if any, or an empty
   *	      string if no sync (or sync effort we know of) is in progress.
   */
  const string get_sync_state_name() const {
    string sn;

    if (sync_role == SYNC_ROLE_NONE)
      return "";

    sn.append(" sync(");

    if (sync_role & SYNC_ROLE_LEADER)
      sn.append(" leader");
    if (sync_role & SYNC_ROLE_PROVIDER)
      sn.append(" provider");
    if (sync_role & SYNC_ROLE_REQUESTER)
      sn.append(" requester");

    sn.append(" state ");
    sn.append(get_sync_state_name(sync_state));

    sn.append(" )");

    return sn;
  }

  /**
   * @} // Synchronization
   */

  list<Context*> waitfor_quorum;
  list<Context*> maybe_wait_for_quorum;

  /**
   * @defgroup Monitor_h_TimeCheck Monitor Clock Drift Early Warning System
   * @{
   *
   * We use time checks to keep track of any clock drifting going on in the
   * cluster. This is accomplished by periodically ping each monitor in the
   * quorum and register its response time on a map, assessing how much its
   * clock has drifted. We also take this opportunity to assess the latency
   * on response.
   *
   * This mechanism works as follows:
   *
   *  - Leader sends out a 'PING' message to each other monitor in the quorum.
   *    The message is timestamped with the leader's current time. The leader's
   *    current time is recorded in a map, associated with each peon's
   *    instance.
   *  - The peon replies to the leader with a timestamped 'PONG' message.
   *  - The leader calculates a delta between the peon's timestamp and its
   *    current time and stashes it.
   *  - The leader also calculates the time it took to receive the 'PONG'
   *    since the 'PING' was sent, and stashes an approximate latency estimate.
   *  - Once all the quorum members have pong'ed, the leader will share the
   *    clock skew and latency maps with all the monitors in the quorum.
   */
  map<entity_inst_t, utime_t> timecheck_waiting;
  map<entity_inst_t, double> timecheck_skews;
  map<entity_inst_t, double> timecheck_latencies;
  // odd value means we are mid-round; even value means the round has
  // finished.
  version_t timecheck_round;
  unsigned int timecheck_acks;
  utime_t timecheck_round_start;
  /**
   * Time Check event.
   */
  Context *timecheck_event;

  struct C_TimeCheck : public Context {
    Monitor *mon;
    C_TimeCheck(Monitor *m) : mon(m) { }
    void finish(int r) {
      mon->timecheck_start_round();
    }
  };

  void timecheck_start();
  void timecheck_finish();
  void timecheck_start_round();
  void timecheck_finish_round(bool success = true);
  void timecheck_cancel_round();
  void timecheck_cleanup();
  void timecheck_report();
  void timecheck();
  health_status_t timecheck_status(ostringstream &ss,
                                   const double skew_bound,
                                   const double latency);
  void handle_timecheck_leader(MTimeCheck *m);
  void handle_timecheck_peon(MTimeCheck *m);
  void handle_timecheck(MTimeCheck *m);
  /**
   * @}
   */
  /**
   * @defgroup Monitor_h_stats Keep track of monitor statistics
   * @{
   */
  struct MonStatsEntry {
    // data dir
    uint64_t kb_total;
    uint64_t kb_used;
    uint64_t kb_avail;
    unsigned int latest_avail_ratio;
    utime_t last_update;
  };

  struct MonStats {
    MonStatsEntry ours;
    map<entity_inst_t,MonStatsEntry> others;
  };

  MonStats stats;

  void stats_update();
  /**
   * @}
   */

  Context *probe_timeout_event;  // for probing

  struct C_ProbeTimeout : public Context {
    Monitor *mon;
    C_ProbeTimeout(Monitor *m) : mon(m) {}
    void finish(int r) {
      mon->probe_timeout(r);
    }
  };

  void reset_probe_timeout();
  void cancel_probe_timeout();
  void probe_timeout(int r);

public:
  epoch_t get_epoch();
  int get_leader() { return leader; }
  const set<int>& get_quorum() { return quorum; }
  set<string> get_quorum_names() {
    set<string> q;
    for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
      q.insert(monmap->get_name(*p));
    return q;
  }
  uint64_t get_quorum_features() const {
    return quorum_features;
  }

  void bootstrap();
  void reset();
  void start_election();
  void win_standalone_election();
  void win_election(epoch_t epoch, set<int>& q,
		    uint64_t features);         // end election (called by Elector)
  void lose_election(epoch_t epoch, set<int>& q, int l,
		     uint64_t features); // end election (called by Elector)
  void finish_election();

  void update_logger();

  /**
   * Vector holding the Services serviced by this Monitor.
   */
  vector<PaxosService*> paxos_service;

  PaxosService *get_paxos_service_by_name(const string& name);

  class PGMonitor *pgmon() {
    return (class PGMonitor *)paxos_service[PAXOS_PGMAP];
  }

  class MDSMonitor *mdsmon() {
    return (class MDSMonitor *)paxos_service[PAXOS_MDSMAP];
  }

  class MonmapMonitor *monmon() {
    return (class MonmapMonitor *)paxos_service[PAXOS_MONMAP];
  }

  class OSDMonitor *osdmon() {
    return (class OSDMonitor *)paxos_service[PAXOS_OSDMAP];
  }

  class AuthMonitor *authmon() {
    return (class AuthMonitor *)paxos_service[PAXOS_AUTH];
  }

  class LogMonitor *logmon() {
    return (class LogMonitor*) paxos_service[PAXOS_LOG];
  }

  friend class Paxos;
  friend class OSDMonitor;
  friend class MDSMonitor;
  friend class MonmapMonitor;
  friend class PGMonitor;
  friend class LogMonitor;

  QuorumService *health_monitor;
  QuorumService *config_key_service;

  // -- sessions --
  MonSessionMap session_map;
  AdminSocketHook *admin_hook;

  void check_subs();
  void check_sub(Subscription *sub);

  void send_latest_monmap(Connection *con);

  // messages
  void handle_get_version(MMonGetVersion *m);
  void handle_subscribe(MMonSubscribe *m);
  void handle_mon_get_map(MMonGetMap *m);
  bool _allowed_command(MonSession *s, map<std::string, cmd_vartype>& cmd);
  void _mon_status(ostream& ss);
  void _quorum_status(ostream& ss);
  void _sync_status(ostream& ss);
  void _sync_force(ostream& ss);
  void _add_bootstrap_peer_hint(string cmd, string args, ostream& ss);
  void handle_command(class MMonCommand *m);
  void handle_route(MRoute *m);

  /**
   * Generate health report
   *
   * @param status one-line status summary
   * @param detailbl optional bufferlist* to fill with a detailed report
   */
  void get_health(string& status, bufferlist *detailbl, Formatter *f);
  void get_status(stringstream &ss, Formatter *f);

  void reply_command(MMonCommand *m, int rc, const string &rs, version_t version);
  void reply_command(MMonCommand *m, int rc, const string &rs, bufferlist& rdata, version_t version);

  /**
   * Handle Synchronization-related messages.
   */
  void handle_probe(MMonProbe *m);
  /**
   * Handle a Probe Operation, replying with our name, quorum and known versions.
   *
   * We use the MMonProbe message class for anything and everything related with
   * Monitor probing. One of the operations relates directly with the probing
   * itself, in which we receive a probe request and to which we reply with
   * our name, our quorum and the known versions for each Paxos service. Thus the
   * redundant function name. This reply will obviously be sent to the one
   * probing/requesting these infos.
   *
   * @todo Add @pre and @post
   *
   * @param m A Probe message, with an operation of type Probe.
   */
  void handle_probe_probe(MMonProbe *m);
  void handle_probe_reply(MMonProbe *m);

  // request routing
  struct RoutedRequest {
    uint64_t tid;
    bufferlist request_bl;
    MonSession *session;
    Connection *con;
    entity_inst_t client_inst;

    ~RoutedRequest() {
      if (session)
	session->put();
      if (con)
	con->put();
    }
  };
  uint64_t routed_request_tid;
  map<uint64_t, RoutedRequest*> routed_requests;
  
  void forward_request_leader(PaxosServiceMessage *req);
  void handle_forward(MForward *m);
  void try_send_message(Message *m, const entity_inst_t& to);
  void send_reply(PaxosServiceMessage *req, Message *reply);
  void no_reply(PaxosServiceMessage *req);
  void resend_routed_requests();
  void remove_session(MonSession *s);
  void remove_all_sessions();
  void waitlist_or_zap_client(Message *m);

  void send_command(const entity_inst_t& inst,
		    const vector<string>& com, version_t version);

public:
  struct C_Command : public Context {
    Monitor *mon;
    MMonCommand *m;
    int rc;
    string rs;
    bufferlist rdata;
    version_t version;
    C_Command(Monitor *_mm, MMonCommand *_m, int r, string s, version_t v) :
      mon(_mm), m(_m), rc(r), rs(s), version(v){}
    C_Command(Monitor *_mm, MMonCommand *_m, int r, string s, bufferlist rd, version_t v) :
      mon(_mm), m(_m), rc(r), rs(s), rdata(rd), version(v){}
    void finish(int r) {
      if (r >= 0)
	mon->reply_command(m, rc, rs, rdata, version);
      else if (r == -ECANCELED)
	m->put();
      else if (r == -EAGAIN)
	mon->_ms_dispatch(m);
      else
	assert(0 == "bad C_Command return value");
    }
  };

 private:
  class C_RetryMessage : public Context {
    Monitor *mon;
    Message *msg;
  public:
    C_RetryMessage(Monitor *m, Message *ms) : mon(m), msg(ms) {}
    void finish(int r) {
      if (r == -EAGAIN || r >= 0)
	mon->_ms_dispatch(msg);
      else if (r == -ECANCELED)
	msg->put();
      else
	assert(0 == "bad C_RetryMessage return value");
    }
  };

  //ms_dispatch handles a lot of logic and we want to reuse it
  //on forwarded messages, so we create a non-locking version for this class
  bool _ms_dispatch(Message *m);
  bool ms_dispatch(Message *m) {
    lock.Lock();
    bool ret = _ms_dispatch(m);
    lock.Unlock();
    return ret;
  }
  //mon_caps is used for un-connected messages from monitors
  MonCap * mon_caps;
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
  bool ms_verify_authorizer(Connection *con, int peer_type,
			    int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			    bool& isvalid, CryptoKey& session_key);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

  int write_default_keyring(bufferlist& bl);
  void extract_save_mon_key(KeyRing& keyring);

  // features
  static CompatSet get_supported_features();
  static CompatSet get_legacy_features();
  void read_features();
  void write_features(MonitorDBStore::Transaction &t);

 public:
  Monitor(CephContext *cct_, string nm, MonitorDBStore *s,
	  Messenger *m, MonMap *map);
  ~Monitor();

  static int check_features(MonitorDBStore *store);

  int preinit();
  int init();
  void init_paxos();
  void shutdown();
  void tick();

  void handle_signal(int sig);

  int mkfs(bufferlist& osdmapbl);

  /**
   * check cluster_fsid file
   *
   * @return EEXIST if file exists and doesn't match, 0 on match, or negative error code
   */
  int check_fsid();

  /**
   * write cluster_fsid file
   *
   * @return 0 on success, or negative error code
   */
  int write_fsid();
  int write_fsid(MonitorDBStore::Transaction &t);

  void do_admin_command(std::string command, std::string args, ostream& ss);

private:
  // don't allow copying
  Monitor(const Monitor& rhs);
  Monitor& operator=(const Monitor &rhs);

public:
  class StoreConverter {
    const string path;
    boost::scoped_ptr<MonitorDBStore> db;
    boost::scoped_ptr<MonitorStore> store;

    set<version_t> gvs;
    map<version_t, set<pair<string,version_t> > > gv_map;

    version_t highest_last_pn;
    version_t highest_accepted_pn;

   public:
    StoreConverter(const string &path)
      : path(path), db(NULL), store(NULL),
	highest_last_pn(0), highest_accepted_pn(0)
    { }

    /**
     * Check if store needs to be converted from old format to a
     * k/v store.
     *
     * @returns 0 if store doesn't need conversion; 1 if it does; <0 if error
     */
    int needs_conversion();
    int convert();

   private:

    bool _check_gv_store();

    void _init() {
      MonitorDBStore *db_ptr = new MonitorDBStore(path);
      db.reset(db_ptr);

      MonitorStore *store_ptr = new MonitorStore(path);
      store.reset(store_ptr);
    }

    void _deinit() {
      db.reset(NULL);
      store.reset(NULL);
    }

    set<string> _get_machines_names() {
      set<string> names;
      names.insert("auth");
      names.insert("logm");
      names.insert("mdsmap");
      names.insert("monmap");
      names.insert("osdmap");
      names.insert("pgmap");

      return names;
    }

    void _mark_convert_start() {
      MonitorDBStore::Transaction tx;
      tx.put("mon_convert", "on_going", 1);
      db->apply_transaction(tx);
    }

    void _convert_finish_features(MonitorDBStore::Transaction &t);
    void _mark_convert_finish() {
      MonitorDBStore::Transaction tx;
      tx.erase("mon_convert", "on_going");
      _convert_finish_features(tx);
      db->apply_transaction(tx);
    }

    void _convert_monitor();
    void _convert_machines(string machine);
    void _convert_osdmap_full();
    void _convert_machines();
    void _convert_paxos();
  };
};

#define CEPH_MON_FEATURE_INCOMPAT_BASE CompatSet::Feature (1, "initial feature set (~v.18)")
#define CEPH_MON_FEATURE_INCOMPAT_GV CompatSet::Feature (2, "global version sequencing (v0.52)")
#define CEPH_MON_FEATURE_INCOMPAT_SINGLE_PAXOS CompatSet::Feature (3, "single paxos with k/v store (v0.\?)")

long parse_pos_long(const char *s, ostream *pss = NULL);


#endif
