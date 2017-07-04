// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_group.h"
#include "rgw_rest_group.h"

#define dout_subsys ceph_subsys_rgw

void RGWRestGroup::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWRestGroup::verify_permission()
{
  return check_caps(s->user->caps);
}

int RGWGroupRead::check_caps(RGWUserCaps& caps)
{
  return caps.check_cap("groups", RGW_CAP_READ);
}

int RGWGroupWrite::check_caps(RGWUserCaps& caps)
{
  return caps.check_cap("groups", RGW_CAP_WRITE);
}

int RGWCreateGroup::get_params()
{
  group_name = s->info.args.get("GroupName");
  group_path = s->info.args.get("Path");

  if (group_name.empty()) {
    ldout(s->cct, 20) << "ERROR: Group name is empty" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWCreateGroup::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  RGWGroup group(s->cct, store, group_name, group_path, s->user->user_id.tenant);
  op_ret = group.create(true);

  if (op_ret == -EEXIST) {
    op_ret = -ERR_GROUP_EXISTS;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("group");
    group.dump(s->formatter);
    s->formatter->close_section();
  }
}

int RGWAddUserToGroup::get_params()
{
  group_name = s->info.args.get("GroupName");
  user = s->info.args.get("UserName");

  if (group_name.empty() || user.empty()) {
    ldout(s->cct, 20) << "ERROR: One of Group name or User name is empty" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWAddUserToGroup::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  RGWGroup group(s->cct, store, group_name, s->user->user_id.tenant);
  op_ret = group.get();
  //group is found
  if (op_ret == 0) {
    RGWUserInfo info, updated_info;
    rgw_user user_id;
    user_id.from_str(user);
    op_ret = rgw_get_user_info_by_uid(store, user_id, info);
    //user is found
    if (op_ret == 0) {
      //add user to group
      if (group.add_user(user)) {
        op_ret = group.update();
      }
      if (op_ret == 0) {
        updated_info = info;
        group_name = s->user->user_id.tenant + "$" + group.get_name();
        if (rgw_add_group_to_user(updated_info, group_name)) {
          op_ret = rgw_store_user_info(store, updated_info, &info, NULL, real_time(), false);
          if (op_ret < 0) {
            //rollback, remove user
            if (group.remove_user(user)) {
              op_ret = group.update();
              if (op_ret < 0)
                op_ret = -ERR_INTERNAL_ERROR;
            }
          }
        }
      } else {
        op_ret = -ERR_INTERNAL_ERROR;
      }
    } else {
      op_ret = -ERR_NO_USER_FOUND;
    }
  } else {
    op_ret = -ERR_NO_GROUP_FOUND;
  }
}

int RGWGetGroup::get_params()
{
  group_name = s->info.args.get("GroupName");

  if (group_name.empty()) {
    ldout(s->cct, 20) << "ERROR: Group name is empty" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWGetGroup::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  ldout(s->cct, 20) << "INFO: Tenant is" <<  s->user->user_id.tenant << dendl;
  RGWGroup group(s->cct, store, group_name, s->user->user_id.tenant);
  op_ret = group.get();
  if (op_ret < 0) {
    op_ret = -ERR_NO_GROUP_FOUND;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("group");
    group.dump(s->formatter);
    s->formatter->close_section();
  }
}

int RGWRemoveUserFromGroup::get_params()
{
  group_name = s->info.args.get("GroupName");
  user = s->info.args.get("UserName");

  if (group_name.empty() || user.empty()) {
    ldout(s->cct, 20) << "ERROR: One of Group name or User name is empty" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWRemoveUserFromGroup::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  RGWGroup group(s->cct, store, group_name, s->user->user_id.tenant);
  op_ret = group.get();
  //group is found
  if (op_ret == 0) {
    RGWUserInfo info, updated_info;
    rgw_user user_id;
    user_id.from_str(user);
    op_ret = rgw_get_user_info_by_uid(store, user_id, info);
    //user is found
    if (op_ret == 0) {
      //remove user from group
      if (group.remove_user(user)) {
        op_ret = group.update();
      }
      if (op_ret == 0) {
        updated_info = info;
        group_name = s->user->user_id.tenant + "$" + group.get_name();
        if (rgw_remove_group_from_user(updated_info, group_name)) {
          op_ret = rgw_store_user_info(store, updated_info, &info, NULL, real_time(), false);
          if (op_ret < 0) {
            //rollback, add user
            if (group.add_user(user)) {
              op_ret = group.update();
              if (op_ret < 0)
                op_ret = -ERR_INTERNAL_ERROR;
            }
          }
        }
      } else {
        op_ret = -ERR_INTERNAL_ERROR;
      }
    } else {
      op_ret = -ERR_NO_USER_FOUND;
    }
  } else {
    op_ret = -ERR_NO_GROUP_FOUND;
  }
}

int RGWUpdateGroup::get_params()
{
  group_name = s->info.args.get("GroupName");
  new_group_name = s->info.args.get("NewGroupName");
  new_group_path = s->info.args.get("NewPath");

  if (group_name.empty()) {
    ldout(s->cct, 20) << "ERROR: Group name is empty" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWUpdateGroup::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  RGWGroup group(s->cct, store, group_name, s->user->user_id.tenant);
  op_ret = group.get();
  if (op_ret == 0) {
    string name, new_name;
    if (! new_group_name.empty()) {
      name = group.get_tenant() + "$" + group.get_name();
      size_t pos = new_group_name.find('$');
      if (pos == std::string::npos) {
        new_name = s->user->user_id.tenant + "$" + new_group_name;
      } else {
        new_name = new_group_name;
      }
    }
    if (! new_name.empty() && (name != new_name)) {
      op_ret = group.update_name(new_name);
      if (op_ret < 0 ) {
        if (op_ret == -EEXIST) {
          op_ret = -ERR_GROUP_EXISTS;
        } else {
          op_ret = -ERR_INTERNAL_ERROR;
        }
        return;
      }
      vector<string> users;
      group.get_users(users);
      for (auto& user : users) {
        RGWUserInfo info, updated_info;
        op_ret = rgw_get_user_info_by_uid(store, user, info);
        if (op_ret == 0) {
          updated_info = info;
          if (rgw_update_group_in_user(updated_info, name, new_name)) {
            op_ret = rgw_store_user_info(store, updated_info, &info, NULL,
                                      real_time(), false);
            if (op_ret < 0) {
              ldout(s->cct, 0) << "ERROR: unable to store updated user info: " << user << dendl;
              op_ret = -ERR_INTERNAL_ERROR;
            }
          } else {
            ldout(s->cct, 0) << "ERROR: group not found for user: " << user << dendl;
            op_ret = -ERR_NO_GROUP_FOUND;
          }
        } else {
          ldout(s->cct, 0) << "ERROR: unable to fetch user info: " << user << dendl;
          op_ret = -ERR_INTERNAL_ERROR;
        }
        if (op_ret < 0) {
          ldout(s->cct, 0) << "ERROR: Reverting to old name: " << name << dendl;
          //revert back to the old name
          op_ret = group.update_name(name);
          if (op_ret < 0) {
            op_ret = -ERR_INTERNAL_ERROR;
            return;
          }
          return;
        }
      }
    }
  } else {
    ldout(s->cct, 0) << "ERROR: unable to find group: " << user << dendl;
    op_ret = -ERR_NO_GROUP_FOUND;
    return;
  }

  if (! new_group_path.empty() && (group.get_path() != new_group_path)) {
    op_ret = group.update_path(new_group_path);
    if (op_ret < 0) {
      op_ret = -ERR_INTERNAL_ERROR;
      return;
    }
  }

    if (! (new_group_name.empty() && new_group_path.empty())) {
    op_ret = group.update();
    if (op_ret < 0) {
      ldout(s->cct, 0) << "ERROR: unable to update group: " << user << dendl;
      op_ret = -ERR_INTERNAL_ERROR;
      return;
    }
  }
}

int RGWListGroups::get_params()
{
  path_prefix = s->info.args.get("PathPrefix");

  return 0;
}

void RGWListGroups::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  vector<RGWGroup> groups;
  op_ret = RGWGroup::get_groups_by_path_prefix(store, g_ceph_context,
                                               path_prefix,
                                               s->user->user_id.tenant, groups);
  if (op_ret < 0) {
    op_ret = -ERR_INTERNAL_ERROR;
    return;
  }

  if (op_ret == 0) {
    s->formatter->open_array_section("Groups");
    for (const auto& group : groups) {
      ldout(s->cct, 0) << "ERROR: group name: " << group.get_name() << dendl;
      s->formatter->open_object_section("group");
      group.dump(s->formatter);
      s->formatter->close_section();
    }
    s->formatter->close_section();
  }
}

int RGWListGroupsForUser::get_params()
{
  user = s->info.args.get("UserName");
  if (user.empty()) {
    ldout(s->cct, 20) << "ERROR: User name is empty" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWListGroupsForUser::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  RGWUserInfo info;
  rgw_user user_id;
  user_id.from_str(user);
  op_ret = rgw_get_user_info_by_uid(store, user_id, info);
  if (op_ret < 0) {
    ldout(s->cct, 20) << "ERROR: User not found: " << user << dendl;
    op_ret = -ERR_NO_USER_FOUND;
    return;
  }

  for (const auto& group_name : info.groups) {
    RGWGroup group(g_ceph_context, store, group_name, "");
    op_ret = group.get();
    if (op_ret < 0) {
      if (op_ret == -ENOENT) {
        ldout(s->cct, 20) << "ERROR: Group not found: " << group_name << dendl;
        op_ret = -ERR_NO_GROUP_FOUND;
        return;
      } else {
        op_ret = -ERR_INTERNAL_ERROR;
        return;
      }
    }
    s->formatter->open_object_section("group");
    group.dump(s->formatter);
    s->formatter->close_section();
  }
}

int RGWDeleteGroup::get_params()
{
  group_name = s->info.args.get("GroupName");
  if (group_name.empty()) {
    ldout(s->cct, 20) << "ERROR: Group name is empty" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWDeleteGroup::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  RGWGroup group(g_ceph_context, store, group_name, s->user->user_id.tenant);
  op_ret = group.get();
  if (op_ret < 0) {
    op_ret = -ERR_NO_GROUP_FOUND;
    return;
  }

  vector<string> users;
  group.get_users(users);
  if (users.empty()) {
    op_ret = group.delete_obj();
    if (op_ret < 0) {
      op_ret = -ERR_INTERNAL_ERROR;
      return;
    }
  } else {
    op_ret = -ERR_GROUP_NOT_EMPTY;
    return;
  }
}
