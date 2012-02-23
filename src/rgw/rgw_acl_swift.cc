
#include <string.h>

#include <vector>

#include "rgw_common.h"
#include "rgw_user.h"
#include "rgw_acl_swift.h"

using namespace std;

static int parse_list(string& uid_list, vector<string>& uids)
{
  char *s = strdup(uid_list.c_str());
  if (!s)
    return -ENOMEM;

  const char *p = strtok(s, " ,");
  while (p) {
    if (*p) {
      string acl = p;
      uids.push_back(acl);
    }
    p = strtok(NULL, " ,");
  }
  free(s);
  return 0;
}

void RGWAccessControlPolicy_SWIFT::add_grants(vector<string>& uids, int perm)
{
  vector<string>::iterator iter;
  for (iter = uids.begin(); iter != uids.end(); ++iter ) {
    ACLGrant grant;
    RGWUserInfo grant_user;
    string& uid = *iter;
    if (rgw_get_user_info_by_uid(uid, grant_user) < 0) {
      dout(10) << "grant user does not exist:" << uid << dendl;
      /* skipping silently */
    } else {
      grant.set_canon(uid, grant_user.display_name, perm);
      acl.add_grant(&grant);
    }
  }
}

bool RGWAccessControlPolicy_SWIFT::create(string& id, string& name, string& read_list, string& write_list)
{
  acl.create_default(id, name);
  owner.set_id(id);
  owner.set_name(name);

  if (read_list.size()) {
    vector<string> uids;
    int r = parse_list(read_list, uids);
    if (r < 0) {
      dout(0) << "ERROR: parse_list returned r=" << r << dendl;
      return false;
    }

    add_grants(uids, RGW_PERM_READ);
  }
  if (write_list.size()) {
    vector<string> uids;
    int r = parse_list(write_list, uids);
    if (r < 0) {
      dout(0) << "ERROR: parse_list returned r=" << r << dendl;
      return false;
    }

    add_grants(uids, RGW_PERM_WRITE);
  }
  return true;
}

