// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include "common/config.h"
#include "common/debug.h"
#include "common/Formatter.h"
#include "MonCaps.h"
#include "mon_types.h"
#include "include/ceph_features.h"

#define dout_subsys ceph_subsys_auth

bool MonCaps::get_next_token(string s, size_t& pos, string& token)
{
  int start = s.find_first_not_of(" \t", pos);
  int end;

  if (start < 0) {
    return false; 
  }
  
  token.clear();

  while (true) {
    if (s[start] == '=' || s[start] == ',' || s[start] == ';') {
      end = start + 1;
    } else {
      end = s.find_first_of(";,= \t", start+1);
      if (end < 0) {
	end = s.size();
      }
      else if (end >= start + 2 && s[end] == ' ' && s[end-1] == '\\') {
	token += s.substr(start, end - start - 1);
	token += ' ';
	start = end + 1;
	continue;
      }
    }
    break;
  }

  token += s.substr(start, end - start);

  pos = end;
  return true;
}

bool MonCaps::is_rwx(CephContext *cct, string& token, rwx_t& cap_val)
{
  const char *t = token.c_str();
  int val = 0;

  if (cct)
    ldout(cct, 10) << "got token=" << token << dendl;
  
  while (*t) {
    switch (*t) {
    case 'r':
      val |= MON_CAP_R;
      break;
    case 'w':
      val |= MON_CAP_W;
      break;
    case 'x':
      val |= MON_CAP_X;
      break;
    default:
      return false;
    }
    t++;
  }
  if (cct)
    ldout(cct, 10) << "return val=" << val << dendl;

  if (val)
    cap_val = val;

  return true;
}

int MonCaps::get_service_id(string& token)
{
  if (token.compare("pgmap") == 0) {
    return PAXOS_PGMAP;
  } else if (token.compare("mdsmap") == 0) {
    return PAXOS_MDSMAP;
  } else if (token.compare("monmap") == 0) {
    return PAXOS_MONMAP;
  } else if (token.compare("osdmap") == 0) {
    return PAXOS_OSDMAP;
  } else if (token.compare("log") == 0) {
    return PAXOS_LOG;
  } else if (token.compare("auth") == 0) {
    return PAXOS_AUTH;
  }

  return -EINVAL;
}

bool MonCaps::parse(bufferlist::iterator& iter, CephContext *cct)
{
  string s;
  try {
    ::decode(s, iter);
  } catch (const buffer::error &err) {
    return false;
  }
  return parse(s, cct);
}

bool MonCaps::parse(string s, CephContext *cct)
{
  text = s;
  try {
    if (cct)
      ldout(cct, 10) << "decoded caps: " << s << dendl;

    size_t pos = 0;
    string token;
    bool init = true;

    bool op_allow = false;
    bool op_deny = false;
    bool any_cmd = false;
    bool got_eq = false;
    list<int> services_list;
    list<int> uid_list;
    bool last_is_comma = false;
    rwx_t cap_val = 0;

    while (pos < s.size()) {
      if (init) {
        op_allow = false;
        op_deny = false;
        any_cmd = false;
        got_eq = false;
        last_is_comma = false;
        cap_val = 0;
        init = false;
        services_list.clear();
	uid_list.clear();
      }

#define ASSERT_STATE(x) \
do { \
  if (!(x)) { \
    if (cct) ldout(cct, 0) << "error parsing caps at pos=" << pos << " (" #x ")" << dendl; \
  } \
} while (0)

      if (get_next_token(s, pos, token)) {
	if (token.compare("*") == 0) { //allow all operations
	  ASSERT_STATE(op_allow);
	  allow_all = true;
	} else if (token.compare("command") == 0) {
	  ASSERT_STATE(op_allow);
	  list<string> command;
	  while (get_next_token(s, pos, token)) {
	    if (token.compare(";") == 0)
	      break;
	    command.push_back(token);
	  }
	  cmd_allow.push_back(command);
	} else if (token.compare("=") == 0) {
          ASSERT_STATE(any_cmd);
          got_eq = true;
        } else if (token.compare("allow") == 0) {
          ASSERT_STATE((!op_allow) && (!op_deny));
          op_allow = true;
        } else if (token.compare("deny") == 0) {
          ASSERT_STATE((!op_allow) && (!op_deny));
          op_deny = true;
        } else if ((token.compare("services") == 0) ||
                   (token.compare("service") == 0)) {
          ASSERT_STATE(op_allow || op_deny);
          any_cmd = true;
	} else if (token.compare("uid") == 0) {
	  ASSERT_STATE(op_allow || op_deny);
	  any_cmd = true;
	} else if (is_rwx(cct, token, cap_val)) {
          ASSERT_STATE(op_allow || op_deny);
        } else if (token.compare(";") != 0) {
	  ASSERT_STATE(got_eq);
	  if (token.compare(",") == 0) {
	    ASSERT_STATE(!last_is_comma);
	  } else {
	    last_is_comma = false;
	    int service = get_service_id(token);
	    if (service != -EINVAL) {
	      if (service >= 0) {
		services_list.push_back(service);
	      } else {
		if (cct)
		  ldout(cct, 0) << "error parsing caps at pos=" << pos << ", unknown service_name: " << token << dendl;
	      }
	    } else { //must be a uid
	      uid_list.push_back(strtoul(token.c_str(), NULL, 10));
	    }
          }
        }

        if (token.compare(";") == 0 || pos >= s.size()) {
	  if (got_eq) {
            ASSERT_STATE(!services_list.empty() || !uid_list.empty());
            
            for (list<int>::iterator i = services_list.begin(); i != services_list.end(); ++i) {
              MonCap& cap = services_map[*i];
              if (op_allow) {
                cap.allow |= cap_val;
              } else {
                cap.deny |= cap_val;
              }
            }
	    for (list<int>::iterator i = uid_list.begin(); i != uid_list.end(); ++i) {
	      MonCap& cap = pool_auid_map[*i];
	      if (op_allow) {
		cap.allow |= cap_val;
	      } else {
		cap.deny |= cap_val;
	      }
	    }
          } else {
            if (op_allow) {
              default_action |= cap_val;
            } else {
              default_action &= ~cap_val;
            }
          }
          init = true;
        }
        
      }
    }
  } catch (const buffer::error &err) {
    return false;
  }

  if (cct) {
    ldout(cct, 10) << "default=" << (int)default_action << dendl;
    map<int, MonCap>::iterator it;
    for (it = services_map.begin(); it != services_map.end(); ++it) {
      ldout(cct, 10) << it->first << " -> (" << (int)it->second.allow << "." << (int)it->second.deny << ")" << dendl;
    }
  }

  return true;
}

rwx_t MonCaps::get_caps(int service) const
{
  if (allow_all)
    return MON_CAP_ALL;

  int caps = default_action;
  map<int, MonCap>::const_iterator it = services_map.find(service);
  if (it != services_map.end()) {
    const MonCap& sc(it->second);
    caps |= sc.allow;
    caps &= ~sc.deny;
    
  }
  return caps;
}

/* general strategy:
 * if they specify an auid, make sure they are allowed to behave
 * as that user (for r/w/x as needed by req_perms).
 * Then, make sure they have the correct cap on the requested service.
 * If any test fails, return false. If they all pass, success!
 *
 * Note that this means auid permissions are NOT very su-like. It gives
 * you access to their data with the rwx that they specify, but you
 * only get as much access as they allow you AND you have on your own data.
 *
 */
bool MonCaps::check_privileges(int service, int req_perms, uint64_t req_auid)
{
  if (allow_all)
    return true; // you're an admin, do anything!
  if (req_auid != CEPH_AUTH_UID_DEFAULT && req_auid != auid) {
    if (!pool_auid_map.count(req_auid))
      return false;
    MonCap& auid_cap = pool_auid_map[req_auid];
    if ((auid_cap.allow & req_perms) != req_perms)
      return false;
  }
  int service_caps = get_caps(service);
  if ((service_caps & req_perms) != req_perms)
    return false;
  return true;
}

// ----

string rwx_to_string(rwx_t r)
{
  string s;
  if (r & MON_CAP_R)
    s += "r";
  else
    s += "-";
  if (r & MON_CAP_W)
    s += "w";
  else
    s += "-";
  if (r & MON_CAP_X)
    s += "x";
  else
    s += "-";
  return s;
}

// ----

void MonCap::dump(Formatter *f) const
{
  f->dump_string("allow", rwx_to_string(allow));
  f->dump_string("deny", rwx_to_string(deny));
}

void MonCap::generate_test_instances(list<MonCap*>& o)
{
  o.push_back(new MonCap);
  o.push_back(new MonCap(MON_CAP_R, MON_CAP_W));
  o.push_back(new MonCap(MON_CAP_RW, MON_CAP_ALL));
}

// ----

void MonCaps::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_MONENC) == 0) {
    __u8 v = 2;
    ::encode(v, bl);
    ::encode(text, bl);
    ::encode(default_action, bl);
    ::encode(services_map, bl);
    ::encode(pool_auid_map, bl);
    ::encode(allow_all, bl);
    ::encode(auid, bl);
    ::encode(cmd_allow, bl);
    return;
  }

  ENCODE_START(3, 3, bl);
  ::encode(text, bl);
  ::encode(default_action, bl);
  ::encode(services_map, bl);
  ::encode(pool_auid_map, bl);
  ::encode(allow_all, bl);
  ::encode(auid, bl);
  ::encode(cmd_allow, bl);
  ENCODE_FINISH(bl);
}

void MonCaps::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, bl);
  ::decode(text, bl);
  ::decode(default_action, bl);
  ::decode(services_map, bl);
  ::decode(pool_auid_map, bl);
  ::decode(allow_all, bl);
  ::decode(auid, bl);
  ::decode(cmd_allow, bl);
  DECODE_FINISH(bl);
}

void MonCaps::dump(Formatter *f) const
{
  f->dump_string("text", text);
  f->dump_string("default_action", rwx_to_string(default_action));
  f->dump_int("allow_all", allow_all);
  f->dump_unsigned("auid", auid);
  f->open_object_section("services_map");
  for (map<int,MonCap>::const_iterator p = services_map.begin(); p != services_map.end(); ++p) {
    f->open_object_section("moncap");
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->open_object_section("pool_auid_map");
  for (map<int,MonCap>::const_iterator p = pool_auid_map.begin(); p != pool_auid_map.end(); ++p) {
    f->open_object_section("moncap");
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();   
}

void MonCaps::generate_test_instances(list<MonCaps*>& o)
{
  o.push_back(new MonCaps);

  o.push_back(new MonCaps);
  o.back()->parse("allow *", NULL);

  o.push_back(new MonCaps);
  o.back()->parse("allow rwx", NULL);

  o.push_back(new MonCaps);
  o.back()->parse("allow rx", NULL);

  o.push_back(new MonCaps);
  o.back()->parse("allow wx", NULL);

  o.push_back(new MonCaps);
  o.back()->parse("allow command foo; allow command bar *; allow command baz ...; allow command foo add * mon allow\\ rwx osd allow\\ *", NULL);
}
