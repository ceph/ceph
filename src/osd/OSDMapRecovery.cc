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
#include "osd/OSDMapRecovery.h"

#include "os/ObjectStore.h"

#include "msg/Messenger.h"
#include "msg/Message.h"
#include "mon/MonClient.h"

#include "messages/MOSDMap.h"
#include "messages/MMonGetOSDMap.h"

#include "common/ceph_context.h"
#include "common/errno.h"
#include "include/assert.h"
#include "common/config.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

static ostream& _prefix(std::ostream* _dout, OSDMapRecovery *osdmr) {
  return *_dout << "osd." << osdmr->osd->get_nodeid()
                << " osdmr(" << osdmr->get_state() << ") ";
}

/* OSDMapRecovery attempts to recover broken osd maps by asking them from
 * the monitors. Recovering osd maps may mask other corruptions. This should
 * be considered solely as a last resort.
 *
 * This is how it goes:
 *
 * 'init' gets basic requirements set up
 *
 *  - a dedicated messenger, so we don't mess with the OSDs own client ms
 *  - a mon client, dully authenticated
 *
 */
int OSDMapRecovery::init()
{
  int auth_attempts = 0;
  int max_auth_attempts = 10;

  int ret = monc.build_initial_monmap();
  if (ret < 0)
    return ret;

  ms = Messenger::create_client_messenger(cct, "recover_mon_client");
  if (!ms) {
    return -EINVAL;
  }
  monc.set_messenger(ms);
  ret = monc.init();
  if (ret < 0) {
    derr << __func__ << " error initiating monclient" << dendl;
    goto err_out;
  }
  ret = monc.authenticate();
  if (ret < 0) {
    derr << __func__ << " unable to authenticate with monitors" << dendl;
    goto err_out;
  }

  while (monc.wait_auth_rotating(30.0) < 0) {
    if (++auth_attempts > max_auth_attempts) {
      derr << __func__ << " unable to get rotating auth keys" << dendl;
      ret = -EINVAL;
      goto err_monout;
    }
  }

  ms->add_dispatcher_head(this);
  state = STATE_INIT;

  return 0;

 err_monout:
  monc.shutdown();
 err_out:
  ms->shutdown();
  delete ms;
  return ret;
}

void OSDMapRecovery::clear() {
  dout(0) << __func__ << dendl;

  Mutex::Locker l(lock);
  wanted.first = wanted.second = 0;
  wanted_available.first = wanted_available.second = 0;
  remote_available.first = remote_available.second = 0;

  maps.clear();
  incremental_maps.clear();
}

bool OSDMapRecovery::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);

  dout(20) << __func__ << " " << m << " " << *m << dendl;

  if (m->get_type() != CEPH_MSG_OSD_MAP) {
    dout(10) << __func__ << " unhandled message" << dendl;
    return false;
  }

  if (state != STATE_WAIT_MON) {
    dout(0) << __func__ << " unexpected message -- drop!" << dendl;
    m->put();
    return true;
  }

  MOSDMap *mm = static_cast<MOSDMap*>(m);

  dout(0) << __func__ << " got maps ["
          << mm->get_first() << " .. " << mm->get_last()
          << "], wanted [" << wanted.first
          << " .. " << wanted.second << "]"
          << ", remote has [ " << mm->oldest_map
          << " .. " << mm->newest_map << "]"
          << dendl;

  remote_available = make_pair(mm->oldest_map, mm->newest_map);

  if (mm->get_first() == 0 &&
      mm->get_last() == 0 &&
      wanted.first != 0 &&
      wanted.second != 0) {
    m->put();
    return true;
  }

  wanted_available.first = wanted_available.second = 0;
  pair<epoch_t,epoch_t> &war = wanted_available; // wanted available range

  // check if received values are within wanted range
  if (wanted.first >= mm->get_first()) {
    if (wanted.second <= mm->get_last()) {
      war = wanted;
    } else {
      war.first = wanted.first;
      war.second = mm->get_last();
    }
  } else if (wanted.second >= mm->get_first()) {
    war.first = mm->get_first();
    if (wanted.second <= mm->get_last()) {
      war.second = wanted.second;
    } else {
      war.second = mm->get_last();
    }
  }

  dout(0) << __func__ << " wanted available range ["
          << war.first << " .. " << war.second << "]" << dendl;
  if (war.first == 0 && war.second == 0) {
    // there's nothing we want. drop it.
    dout(0) << __func__ << " nothing we want -- drop!" << dendl;
    m->put();
    return true;
  }

  if (wants_inc_maps && mm->incremental_maps.empty() &&
      !mm->maps.empty()) {
    dout(0) << __func__ << " wanted incremental maps, got full maps -- drop!"
            << dendl;
    m->put();
    return true;
  }

  for (auto e : mm->maps) {
    if (e.first < war.first || e.first > war.second)
      continue;
    maps[e.first] = e.second;
  }

  for (auto e : mm->incremental_maps) {
    if (e.first < war.first || e.first > war.second)
      continue;
    incremental_maps[e.first] = e.second;
  }

  dout(0) << __func__ << " have " << maps.size()
          << " maps and " << incremental_maps.size()
          << " incremental maps" << dendl;

  wanted_cond.SignalAll();

  return true;
}

int OSDMapRecovery::recover_range(
    const pair<epoch_t,epoch_t> &r,
    list<pair<epoch_t,epoch_t> >& missing,
    bool inc)
{
  dout(10) << __func__ << " " << (inc ? "inc" : "full")
           << " [" << r.first << " .. "
           << r.second << "]" << dendl;

  clear();

  lock.Lock();
  wanted = r;
  MMonGetOSDMap *req = new MMonGetOSDMap;
  if (inc) {
    req->request_inc(r.first, r.second);
    wants_inc_maps = true;
  } else {
    req->request_full(r.first, r.second);
  }
  monc.send_mon_message(req);

  state = STATE_WAIT_MON;
  if (cct->_conf->osd_map_recover_broken_timeout > 0.0) {
    utime_t until = ceph_clock_now(cct);
    until += cct->_conf->osd_map_recover_broken_timeout;
    int r = wanted_cond.WaitUntil(lock, until);
    if (r == ETIMEDOUT) {
      derr << __func__ << " timeout waiting for reply from monitors" << dendl;
      lock.Unlock();
      return -r;
    }
  } else {
    wanted_cond.Wait(lock);
  }

  state = STATE_WORKING;
  dout(0) << __func__ << " got available range ["
          << wanted_available.first << " .. "
          << wanted_available.second << "]" << dendl;

  int err = 0;
  if (!_contained_in(wanted, wanted_available)) {
    dout(0) << __func__ << " didn't get a valid range" << dendl;
    if (!_contained_in(wanted, remote_available)) {
      dout(0) << __func__
              << " monitor does not have available the requested maps"
              << " (available [" << remote_available.first
              << " .. " << remote_available.second << "]"
              << dendl;
      err = -ENOENT;
    } else {
      err = -ERANGE;
    }
    lock.Unlock();
    return err;
  }

  if (wanted.first < wanted_available.first) {
    pair<epoch_t,epoch_t> p;
    p.first = wanted.first;
    p.second = wanted_available.first - 1;
    missing.push_back(p);
  }

  if (wanted.second > wanted_available.second) {
    pair<epoch_t,epoch_t> p;
    p.first = wanted_available.second + 1;
    p.second = wanted.second;
    missing.push_back(p);
  }

  if (!missing.empty()) {
    dout(0) << __func__ << " missing ranges " << missing << dendl;
  }

  ObjectStore::Transaction t;
  for (epoch_t e = wanted_available.first;
       e <= wanted_available.second;
       ++e) {

    if (inc) {
      auto i = incremental_maps.find(e);
      if (i != incremental_maps.end()) {
        dout(10) << __func__ << " inc map epoch " << e << dendl;
        bufferlist& bl = i->second;
        ghobject_t incoid = osd->get_inc_osdmap_pobject_name(e);
        t.write(coll_t::meta(), incoid, 0, bl.length(), bl);
      }
    } else {
      auto i = maps.find(e);
      if (i != maps.end()) {
        dout(10) << __func__ << " full map epoch " << e << dendl;
        OSDMap *o = new OSDMap;
        bufferlist& bl = i->second;
        o->decode(bl);
        dout(0) << *o << dendl;
        assert(bl.length() > 0);
        ghobject_t fulloid = osd->get_osdmap_pobject_name(e);
        t.write(coll_t::meta(), fulloid, 0, bl.length(), bl);
      }
    }
  }
  err = osd->service.store->apply_transaction(
      osd->service.meta_osr.get(),
      std::move(t));
  if (err < 0) {
    derr << __func__ << " unable to apply transaction" << dendl;
  }

  lock.Unlock();
  return err;
}

int OSDMapRecovery::_recover(list<pair<epoch_t,epoch_t> >& ranges, bool inc)
{
  dout(10) << __func__ << " " << ranges.size() << " "
           << (inc ? "broken inc maps" : "broken ranges") << dendl;

  while (!ranges.empty()) {
    pair<epoch_t,epoch_t> r = ranges.front();
    ranges.pop_front();

    list<pair<epoch_t,epoch_t> > missing;
    int ret = recover_range(r, missing, inc);
    if (ret < 0) {
      derr << __func__ << " error recovering range ["
        << r.first << " .. " << r.second << "]" << dendl;
      state = STATE_ERROR;
      return ret;
    }
    if (!missing.empty()) {
      ranges.splice(ranges.begin(), missing);
    }
  }
  return 0;
}

int OSDMapRecovery::recover(const epoch_t first, const epoch_t last)
{
  assert(state == STATE_INIT);

  state = STATE_RECOVERING;
  dout(10) << __func__ << " epochs from " << first
           << " to " << last << dendl;

  list<pair<epoch_t,epoch_t> > broken_maps;
  if (!find_broken_ranges(first, last, broken_maps, false)) {
    dout(0) << __func__ << " no broken maps found" << dendl;
  }

  list<pair<epoch_t,epoch_t> > broken_inc_maps;
  if (!find_broken_ranges(first, last, broken_inc_maps, true)) {
    dout(0) << __func__ << " no broken inc maps found" << dendl;
  }

  int ret = _recover(broken_maps, false);
  if (ret < 0) {
    derr << __func__ << " error recovering broken maps" << dendl;
    state = STATE_ERROR;
    return ret;
  }

  ret = _recover(broken_inc_maps, true);
  if (ret < 0) {
    derr << __func__ << " error recovering broken inc maps" << dendl;
    state = STATE_ERROR;
    return ret;
  }

  state = STATE_DONE;
  dout(0) << __func__ << " finished recovery" << dendl;
  return 0;
}

bool OSDMapRecovery::_is_broken_map(epoch_t e)
{
  try {
    bufferlist bl;

    if (!osd->service.get_map_bl(e, bl, false) || bl.length() == 0) {
      throw ceph::buffer::end_of_buffer();
    }

    OSDMap m;
    m.decode(bl);

  } catch (ceph::buffer::end_of_buffer e) {
    return true;
  } catch (ceph::buffer::malformed_input e) {
    return true;
  }
  return false;
}

bool OSDMapRecovery::_is_broken_inc(epoch_t e)
{
  try {
    bufferlist bl;

    if (!osd->service.get_inc_map_bl(e, bl, false) || bl.length() == 0) {
      throw ceph::buffer::end_of_buffer();
    }

    OSDMap::Incremental inc;
    auto p = bl.begin();
    inc.decode(p);

  } catch (ceph::buffer::end_of_buffer e) {
    return true;
  } catch (ceph::buffer::malformed_input e) {
    return true;
  }

  return false;
}

bool OSDMapRecovery::find_broken_ranges(
    epoch_t first,
    epoch_t last,
    list<pair<epoch_t,epoch_t> >& ranges,
    bool inc)
{

  dout(10) << __func__
           << " " << (inc ? "inc" : "full" )
           << " [" << first << " .. " << last << "]" << dendl;

  for (auto e = first; e <= last; e ++) {

    bool broken = (inc ? _is_broken_inc(e) : _is_broken_map(e));

    if (broken) {
      dout(0) << __func__ << " " << (inc ? "inc" : "full")
              << " epoch " << e << " broken" << dendl;

      auto &l = ranges.back();
      if (l.second == (e - 1)) {
        l.second = e;
      } else {
        ranges.push_back(std::make_pair(e, e));
      }
    }
  }

  return !ranges.empty();
}

