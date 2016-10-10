// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <errno.h>

#include "osd/OSD.h"
#include "osd/OSDMap.h"

#include "msg/Messenger.h"
#include "msg/Message.h"
#include "mon/MonClient.h"

#include "common/ceph_context.h"
#include "common/errno.h"
#include "include/assert.h"
#include "include/types.h"
#include "common/config.h"


/**
 * Recover broken osd maps automatically by requesting the broken maps
 * from the monitors.
 *
 * This class will create its own messenger and monclient, keeping itself
 * separated from the OSD's so as to be as unobtrusive as possible during
 * the OSD's init process.
 *
 * The recover workflow starts with the `recover()` function, which will
 * first init the class - setup messenger and monclient -, and then proceed
 * to finding all the broken maps and recover any broken ranges that may be
 * found.
 *
 * Finding broken maps, may they be full or incremental, relies on reading
 * map epochs from disk and decoding them. Maps with zero length are
 * considered somehow corrupt, as well as any maps which throw exceptions
 * during decode.
 *
 * Ranges will be asked from the monitors, and replies will be handled by
 * `ms_dispatch()`. The process is blocking, with the recovery function
 * `recover_range()` blocking while waiting for an adequate reply to be
 * handled by `ms_dispatch()`. If `osd_map_recover_broken_timeout` is
 * greater than zero, a timeout will be triggered if we don't get an
 * adequate reply in the mean time.
 *
 * Given a reply with the wanted range, those maps will then be written
 * off to disk. Once all the broken maps have been written to disk, the
 * recovery will finish successfuly.
 *
 */
struct OSDMapRecovery : public Dispatcher {

  OSD *osd;

  MonClient monc;
  Messenger *ms;

  Mutex lock;
  Cond wanted_cond;
  bool wants_inc_maps;
  pair<epoch_t, epoch_t> wanted;
  pair<epoch_t, epoch_t> wanted_available;
  pair<epoch_t,epoch_t> remote_available;
  // received from the monitors
  map<epoch_t, bufferlist> maps;
  map<epoch_t, bufferlist> incremental_maps;

  enum state_t {
    STATE_NONE,
    STATE_INIT,
    STATE_RECOVERING,
    STATE_WAIT_MON,
    STATE_WORKING,
    STATE_DONE,
    STATE_ERROR
  } state;

  const char *get_state() {
    switch (state) {
      case STATE_NONE:
        return "none";
      case STATE_INIT:
        return "init";
      case STATE_RECOVERING:
        return "recovering";
      case STATE_WAIT_MON:
        return "wait_mon";
      case STATE_WORKING:
        return "working";
      case STATE_DONE:
        return "done";
      case STATE_ERROR:
        return "error";
    }
    return "unknown";
  }


  explicit OSDMapRecovery(CephContext *_cct, OSD *_osd) :
    Dispatcher(_cct),
    osd(_osd),
    monc(_cct),
    ms(nullptr),
    lock("OSDMR::lock"),
    wants_inc_maps(false),
    state(STATE_NONE)
  { }

  virtual ~OSDMapRecovery() {
    Mutex::Locker l(lock);

    state = STATE_NONE;
    monc.shutdown();
    if (ms) {
      ms->shutdown();
      delete ms;
    }
  }

  int init();
  void clear();
  bool ms_dispatch(Message *m);

  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) { }

  bool _contained_in(const pair<epoch_t,epoch_t>& a,
                     const pair<epoch_t,epoch_t>& b) const {
    return (b.first >= a.first && b.second <= a.second);
  }

  int _recover(list<pair<epoch_t,epoch_t> >& ranges,
               bool inc);
  int recover_range(const pair<epoch_t,epoch_t> &r,
                    list<pair<epoch_t,epoch_t> >& missing,
                    bool inc);
  int recover(const epoch_t first, const epoch_t last);

  bool find_broken_ranges(epoch_t first,
                          epoch_t last,
                          list<pair<epoch_t,epoch_t> >& ranges,
                          bool inc);
private:
  bool _is_broken_map(epoch_t e);
  bool _is_broken_inc(epoch_t e);
};

