#pragma once

#include "rgw_common.h"
#include "rgw_user.h"
#include "common/errno.h"
#include <sys/fsuid.h>

namespace rgw::sal::posix {
  class FSEnt;
}

class UidGuard {
  uid_t prev_uid;
  gid_t prev_gid;
public:
  UidGuard(uid_t uid, gid_t gid)
    : prev_uid(setfsuid(uid)), prev_gid(setfsgid(gid)) {}
  ~UidGuard() { setfsuid(prev_uid); setfsgid(prev_gid); }
  UidGuard(const UidGuard&) = delete;
  UidGuard& operator=(const UidGuard&) = delete;
};

#define RUN_AS(uid, gid, expr) \
  ([&]() -> decltype(expr) { UidGuard _g(uid, gid); return (expr); }())

class POSIXSystemUser {
  private:
    int uid{0};
    int gid{0};

  public:
    POSIXSystemUser() {}

    void set_uid(int uid) { this->uid = uid; }
    int get_uid() { return uid; }
    void set_gid(int gid) { this->gid = gid; }
    int get_gid() { return gid; }
};

class POSIXSystemManager {
  private:
    std::string path;
    std::multimap<std::string, std::pair<int, int>> user_data;

  public:
    POSIXSystemManager() {}

    std::string get_user_mapping_path() {
      return path;
    }

    int init(const DoutPrefixProvider *dpp);
    int populate_user_data(const DoutPrefixProvider *dpp);
    int find_posix_user(const DoutPrefixProvider *dpp, const rgw_user& ruser, POSIXSystemUser& posix_user);
    int update_posix_user(const DoutPrefixProvider *dpp, const rgw_user& ruser, POSIXSystemUser posix_user);
    int remove_posix_user(const DoutPrefixProvider *dpp, const rgw_user& ruser);
};
