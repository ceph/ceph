#include <errno.h>
#include "config.h"
#include "MonCaps.h"
#include "mon_types.h"

bool MonCaps::get_next_token(string s, size_t& pos, string& token)
{
  int start = s.find_first_not_of(" \t", pos);
  int end;

  if (start < 0) {
    return false; 
  }

  if (s[start] == '=' || s[start] == ',' || s[start] == ';') {
    end = start + 1;
  } else {
    end = s.find_first_of(";,= \t", start+1);
  }

  if (end < 0) {
    end=s.size();
  }

  token = s.substr(start, end - start);

  pos = end;

  return true;
}

bool MonCaps::is_rwx(string& token, rwx_t& cap_val)
{
  const char *t = token.c_str();
  int val = 0;

  generic_dout(10) << "got token=" << token << dendl;

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
  generic_dout(10) << "return val=" << val << dendl;

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
  } else if (token.compare("class") == 0) {
    return PAXOS_CLASS;
  } else if (token.compare("auth") == 0) {
    return PAXOS_AUTH;
  }

  return -EINVAL;
}

bool MonCaps::parse(bufferlist::iterator& iter)
{
  string s;

  try {
    ::decode(s, iter);
    text = s;

    generic_dout(10) << "decoded caps: " << s << dendl;

    size_t pos = 0;
    string token;
    bool init = true;

    bool op_allow = false;
    bool op_deny = false;
    bool cmd_service = false;
    bool any_cmd = false;
    bool cmd_uid = false;
    bool got_eq = false;
    list<int> services_list;
    list<int> uid_list;
    bool last_is_comma = false;
    rwx_t cap_val = 0;

    while (pos < s.size()) {
      if (init) {
        op_allow = false;
        op_deny = false;
        cmd_service = false;
	cmd_uid = false;
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
       generic_dout(0) << "error parsing caps at pos=" << pos << " (" #x ")" << dendl; \
  } \
} while (0)

      if (get_next_token(s, pos, token)) {
	if (token.compare("*") == 0) { //allow all operations
	  ASSERT_STATE(op_allow);
	  allow_all = true;
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
          cmd_service = true;
          any_cmd = true;
	} else if (token.compare("uid") == 0) {
	  ASSERT_STATE(op_allow || op_deny);
	  any_cmd = true;
	  cmd_uid = true;
	} else if (is_rwx(token, cap_val)) {
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
		generic_dout(0) << "error parsing caps at pos=" << pos << ", unknown service_name: " << token << dendl;
	      }
	    } else { //must be a uid
	      uid_list.push_back(strtoul(token.c_str(), NULL, 10));
	    }
          }
        }

        if (token.compare(";") == 0 || pos >= s.size()) {
          if (got_eq) {
            ASSERT_STATE((services_list.size() > 0) ||
			 (uid_list.size() > 0));
            list<int>::iterator iter;
            for (iter = services_list.begin(); iter != services_list.end(); ++iter) {
              MonCap& cap = services_map[*iter];
              if (op_allow) {
                cap.allow |= cap_val;
              } else {
                cap.deny |= cap_val;
              }
            }
	    for (iter = uid_list.begin(); iter != uid_list.end(); ++iter) {
	      MonCap& cap = pool_auid_map[*iter];
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

  generic_dout(10) << "default=" << (int)default_action << dendl;
  map<int, MonCap>::iterator it;
  for (it = services_map.begin(); it != services_map.end(); ++it) {
    generic_dout(10) << it->first << " -> (" << (int)it->second.allow << "." << (int)it->second.deny << ")" << dendl;
  }

  return true;
}

rwx_t MonCaps::get_caps(int service)
{
  if (allow_all)
    return MON_CAP_ALL;

  int caps = default_action;
  map<int, MonCap>::iterator it = services_map.find(service);
  if (it != services_map.end()) {
    MonCap& sc = it->second;
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
  if (allow_all) return true; //you're an admin, do anything!
  if (req_auid != CEPH_AUTH_UID_DEFAULT && req_auid != auid) {
    if (!pool_auid_map.count(req_auid)) return false;
    MonCap& auid_cap = pool_auid_map[req_auid];
    if ((auid_cap.allow & req_perms) != req_perms) return false;
  }
  int service_caps = get_caps(service);
  if ((service_caps & req_perms) != req_perms) return false;
  return true;
}
