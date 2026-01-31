#pragma once

#include "rgw_common.h"
#include "rgw_user.h"
#include "common/errno.h"

#define RUN_AS(dpp, mapped_uid, mapped_gid, orig_uid, orig_gid, call, ret, call_ret)	\
  do {                                                              			\
    ret = setresuid(-1, mapped_uid, -1);						\
    if (ret < 0) {									\
      ldpp_dout(dpp, 0) << "ERROR: could not set mapped uid" << dendl;			\
      goto out;										\
    }     										\
    ret = setresgid(-1, mapped_gid, -1);						\
    if (ret < 0) {									\
      ldpp_dout(dpp, 0) << "ERROR: could not set mapped gid" << dendl;			\
      goto out;										\
    }     										\
    call_ret = call;									\
    if (call_ret < 0) {									\
      ldpp_dout(dpp, 0) << "ERROR: Op failed" << dendl;					\
    }   										\
    ret = setresuid(-1, orig_uid, -1);							\
    if (ret < 0) {									\
      ldpp_dout(dpp, 0) << "ERROR: could not set original uid" << dendl;		\
      goto out;										\
    }   										\
    ret = setresgid(-1, orig_gid, -1);  						\
    if (ret < 0) {									\
      ldpp_dout(dpp, 0) << "ERROR: could not set original gid" << dendl;		\
      goto out;										\
    }      										\
  } while(0);	

namespace rgw::sal {
  class FSEnt;
}

class POSIXSystemUser {
  private:
    int uid;
    int gid;

  public:
    POSIXSystemUser() {}

    void set_uid(int uid) { this->uid = uid; }
    int get_uid() { return uid; } 
    void set_gid(int uid) { this->gid = uid; }
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
