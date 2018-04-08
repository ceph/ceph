#ifndef CEPH_RGW_ADMIN_OPT_USER_H
#define CEPH_RGW_ADMIN_OPT_USER_H

#include "rgw_rados.h"
#include "rgw_user.h"
#include "rgw_admin_common.h"

// This header and the corresponding source file contain handling of the following commads / groups of commands:
// User

int handle_opt_user_create(const std::string& subuser, RGWUserAdminOpState& user_op, RGWUser& user);

int handle_opt_user_stats(bool sync_stats, const std::string& bucket_name, const std::string& tenant,
                          rgw_user& user_id, RGWRados *store, Formatter *formatter);


// TODO: how to handle commands with different command prefix but same following words?
class RgwAdminUserCommandsHandler : public RgwAdminCommandGroupHandler {
public:
  RgwAdminUserCommandsHandler(std::vector<const char*>& args, const std::vector<std::string>& prefix,
                              RGWRados* store, Formatter* formatter)
      : RgwAdminCommandGroupHandler(args, prefix, {}, store, formatter) {
    if (parse_command_and_parameters() == 0) {
      std::cout << "Parsed command: " << command << std::endl;
    }
  }

  ~RgwAdminUserCommandsHandler() override = default;

  RgwAdminCommandGroup get_type() const override { return USER; }

  int execute_command() override {
    std::string err_msg;
    int ret = 0;
    RGWUser user;
    if (!user_id.empty() || !subuser.empty()) {
      ret = user.init(store, user_op);
      if (ret < 0) {
        cerr << "user.init failed: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
    switch (command) {
      case OPT_USER_INFO:
        break;
      case OPT_USER_CREATE:
        ret = handle_opt_user_create(subuser, user_op, user);
        if (ret > 0) {
          return ret;
        }
        break;
      case OPT_USER_RM:
        ret = user.remove(user_op, &err_msg);
        if (ret < 0) {
          cerr << "could not remove user: " << err_msg << std::endl;
        }
        return -ret;
      case OPT_USER_ENABLE:
        user_op.set_suspension(false);
        // falling through on purpose
      case OPT_USER_SUSPEND:
        user_op.set_suspension(true);
        // falling through on purpose
      case OPT_USER_MODIFY:
        ret = user.modify(user_op, &err_msg);
        if (ret < 0) {
          cerr << "could not modify user: " << err_msg << std::endl;
          return -ret;
        }

        break;
      case OPT_SUBUSER_CREATE:
        ret = user.subusers.add(user_op, &err_msg);
        if (ret < 0) {
          cerr << "could not create subuser: " << err_msg << std::endl;
          return -ret;
        }

        break;
      case OPT_SUBUSER_MODIFY:
        ret = user.subusers.modify(user_op, &err_msg);
        if (ret < 0) {
          cerr << "could not modify subuser: " << err_msg << std::endl;
          return -ret;
        }

        break;
      case OPT_SUBUSER_RM:
        ret = user.subusers.remove(user_op, &err_msg);
        if (ret < 0) {
          cerr << "could not remove subuser: " << err_msg << std::endl;
          return -ret;
        }

        break;
      case OPT_CAPS_ADD:
        ret = user.caps.add(user_op, &err_msg);
        if (ret < 0) {
          cerr << "could not add caps: " << err_msg << std::endl;
          return -ret;
        }

        break;
      case OPT_CAPS_RM:
        ret = user.caps.remove(user_op, &err_msg);
        if (ret < 0) {
          cerr << "could not remove caps: " << err_msg << std::endl;
          return -ret;
        }

        break;
      case OPT_KEY_CREATE:
        ret = user.keys.add(user_op, &err_msg);
        if (ret < 0) {
          cerr << "could not create key: " << err_msg << std::endl;
          return -ret;
        }

        break;
      case OPT_KEY_RM:
        ret = user.keys.remove(user_op, &err_msg);
        if (ret < 0) {
          cerr << "could not remove key: " << err_msg << std::endl;
          return -ret;
        }
        break;
        // TODO: handle OPT_USER_CHECK, OPT_USER_LIST and OPT_USER_STATS -- they don't seem to fit in this class
      default:
        return EINVAL;
    }

    ret = user.info(info, &err_msg);
    if (ret < 0) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return -ret;
    }
    show_user_info();
    return 0;
  }

private:
  int parse_command_and_parameters() override;

  void show_user_info();

  std::string bucket_name;
  RGWUserInfo info;
  std::string subuser;
  bool sync_stats = false;
  std::string tenant;
  rgw_user user_id;
  RGWUserAdminOpState user_op;
};

#endif //CEPH_RGW_ADMIN_OPT_USER_H
