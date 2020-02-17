// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "MDSCacheObject.h"
#include "MDSContext.h"
#include "common/Formatter.h"

std::string_view MDSCacheObject::generic_pin_name(int p) const {
  switch (p) {
    case PIN_REPLICATED: return "replicated";
    case PIN_DIRTY: return "dirty";
    case PIN_LOCK: return "lock";
    case PIN_REQUEST: return "request";
    case PIN_WAITER: return "waiter";
    case PIN_DIRTYSCATTERED: return "dirtyscattered";
    case PIN_AUTHPIN: return "authpin";
    case PIN_PTRWAITER: return "ptrwaiter";
    case PIN_TEMPEXPORTING: return "tempexporting";
    case PIN_CLIENTLEASE: return "clientlease";
    case PIN_DISCOVERBASE: return "discoverbase";
    default: ceph_abort(); return std::string_view();
  }
}

void MDSCacheObject::finish_waiting(uint64_t mask, int result) {
  MDSContext::vec finished;
  take_waiting(mask, finished);
  finish_contexts(g_ceph_context, finished, result);
}

void MDSCacheObject::dump(Formatter *f) const
{
  f->dump_bool("is_auth", is_auth());

  // Fields only meaningful for auth
  f->open_object_section("auth_state");
  {
    f->open_object_section("replicas");
    for (const auto &it : get_replicas()) {
      std::ostringstream rank_str;
      rank_str << it.first;
      f->dump_int(rank_str.str().c_str(), it.second);
    }
    f->close_section();
  }
  f->close_section(); // auth_state

  // Fields only meaningful for replica
  f->open_object_section("replica_state");
  {
    f->open_array_section("authority");
    f->dump_int("first", authority().first);
    f->dump_int("second", authority().second);
    f->close_section();
    f->dump_unsigned("replica_nonce", get_replica_nonce());
  }
  f->close_section();  // replica_state

  f->dump_int("auth_pins", auth_pins);
  f->dump_bool("is_frozen", is_frozen());
  f->dump_bool("is_freezing", is_freezing());

#ifdef MDS_REF_SET
    f->open_object_section("pins");
    for(const auto& p : ref_map) {
      f->dump_int(pin_name(p.first).data(), p.second);
    }
    f->close_section();
#endif
    f->dump_int("nref", ref);
}

/*
 * Use this in subclasses when printing their specialized
 * states too.
 */
void MDSCacheObject::dump_states(Formatter *f) const
{
  if (state_test(STATE_AUTH)) f->dump_string("state", "auth");
  if (state_test(STATE_DIRTY)) f->dump_string("state", "dirty");
  if (state_test(STATE_NOTIFYREF)) f->dump_string("state", "notifyref");
  if (state_test(STATE_REJOINING)) f->dump_string("state", "rejoining");
  if (state_test(STATE_REJOINUNDEF))
    f->dump_string("state", "rejoinundef");
}

bool MDSCacheObject::is_waiter_for(uint64_t mask, uint64_t min) {
  if (!min) {
    min = mask;
    while (min & (min-1))  // if more than one bit is set
      min &= min-1;        //  clear LSB
  }
  for (auto p = waiting.lower_bound(min); p != waiting.end(); ++p) {
    if (p->first & mask) return true;
    if (p->first > mask) return false;
  }
  return false;
}

void MDSCacheObject::take_waiting(uint64_t mask, MDSContext::vec& ls) {
  if (waiting.empty()) return;

  // process ordered waiters in the same order that they were added.
  std::map<uint64_t, MDSContext*> ordered_waiters;

  for (auto it = waiting.begin(); it != waiting.end(); ) {
    if (it->first & mask) {
        if (it->second.first > 0) {
          ordered_waiters.insert(it->second);
        } else {
          ls.push_back(it->second.second);
        }
//      pdout(10,g_conf()->debug_mds) << (mdsco_db_line_prefix(this))
//                                 << "take_waiting mask " << hex << mask << dec << " took " << it->second
//                                 << " tag " << hex << it->first << dec
//                                 << " on " << *this
//                                 << dendl;
        waiting.erase(it++);
    } else {
//      pdout(10,g_conf()->debug_mds) << "take_waiting mask " << hex << mask << dec << " SKIPPING " << it->second
//                                 << " tag " << hex << it->first << dec
//                                 << " on " << *this 
//                                 << dendl;
        ++it;
    }
  }
  for (auto it = ordered_waiters.begin(); it != ordered_waiters.end(); ++it) {
    ls.push_back(it->second);
  }
  if (waiting.empty()) {
    put(PIN_WAITER);
    waiting.clear();
  }
}

uint64_t MDSCacheObject::last_wait_seq = 0;
