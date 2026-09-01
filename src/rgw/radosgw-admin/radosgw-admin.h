// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Copyright (C) 2024 IBM 
*/

#pragma once

#include <any>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "include/str_list.h"

class SimpleCmd {
public:
  struct Def {
    std::string cmd;
    std::any opt;
  };

  using Aliases = std::vector<std::set<std::string> >;
  using Commands = std::vector<Def>;

private:
  struct Node {
    std::map<std::string, Node> next;
    std::set<std::string> expected; /* separate un-normalized list */
    std::any opt;
  };

  Node cmd_root;
  std::map<std::string, std::string> alias_map;

  std::string normalize_alias(const std::string& s) const {
    auto iter = alias_map.find(s);
    if (iter == alias_map.end()) {
      return s;
    }

    return iter->second;
  }
  void init_alias_map(Aliases& aliases) {
    for (auto& alias_set : aliases) {
      std::optional<std::string> first;

      for (auto& alias : alias_set) {
        if (!first) {
          first = alias;
        } else {
          alias_map[alias] = *first;
        }
      }
    }
  }

  bool gen_next_expected(Node *node, std::vector<std::string> *expected, bool ret) {
    for (auto& next_cmd : node->expected) {
      expected->push_back(next_cmd);
    }
    return ret;
  }

public:
  SimpleCmd() {}

  SimpleCmd(std::optional<Commands> cmds,
            std::optional<Aliases> aliases) {
    if (aliases) {
      add_aliases(*aliases);
    }

    if (cmds) {
      add_commands(*cmds);
    }
  }

  void add_aliases(Aliases& aliases) {
    init_alias_map(aliases);
  }

  void add_commands(std::vector<Def>& cmds) {
    for (auto& cmd : cmds) {
      std::vector<std::string> words;
      get_str_vec(cmd.cmd, " ", words);

      auto node = &cmd_root;
      for (auto& word : words) {
        auto norm = normalize_alias(word);
        auto parent = node;

        node->expected.insert(word);

        node = &node->next[norm];

        if (norm == "[*]") { /* optional param at the end */
          parent->next["*"] = *node; /* can be also looked up by '*' */
          parent->opt = cmd.opt;
        }
      }

      node->opt = cmd.opt;
    }
  }

  template <class Container>
  bool find_command(Container& args,
                    std::any *opt_cmd,
                    std::vector<std::string> *extra_args,
                    std::string *error,
                    std::vector<std::string> *expected) {
    auto node = &cmd_root;

    std::optional<std::any> found_opt;

    for (auto& arg : args) {
      std::string norm = normalize_alias(arg);
      auto iter = node->next.find(norm);
      if (iter == node->next.end()) {
        iter = node->next.find("*");
        if (iter == node->next.end()) {
          *error = std::string("ERROR: Unrecognized argument: '") + arg + "'";
          return gen_next_expected(node, expected, false);
        }
        extra_args->push_back(arg);
        if (!found_opt) {
          found_opt = node->opt;
        }
      }
      node = &(iter->second);
    }

    *opt_cmd = found_opt.value_or(node->opt);

    if (!opt_cmd->has_value()) {
      *error ="ERROR: Unknown command";
      return gen_next_expected(node, expected, false);
    }

    return true;
  }
};

// Command identifiers for radosgw-admin subcommands. Kept here so
// additional translation units under radosgw-admin/ can share the
// same enum without depending on radosgw-admin.cc.
namespace rgw_admin {

enum class OPT {
  NO_CMD,
#include "radosgw-admin/opt_user.inc"
  USER_POLICY_ATTACH,
  USER_POLICY_DETACH,
  USER_POLICY_LIST_ATTACHED,
#include "radosgw-admin/opt_bucket.inc"
#ifdef WITH_RADOSGW_RADOS
  BUCKET_SYNC_CHECKPOINT,
  BUCKET_SYNC_INFO,
  BUCKET_SYNC_STATUS,
  BUCKET_SYNC_MARKERS,
  BUCKET_SYNC_INIT,
  BUCKET_SYNC_RUN,
  BUCKET_SYNC_DISABLE,
  BUCKET_SYNC_ENABLE,
  BUCKET_RESYNC_ENCRYPTED_MULTIPART,
#endif
  BUCKET_LOGGING_FLUSH,
  BUCKET_LOGGING_INFO,
  BUCKET_LOGGING_LIST,
  POLICY,
#ifdef WITH_RADOSGW_RADOS
#include "radosgw-admin/opt_log.inc"
#endif
#include "radosgw-admin/opt_usage.inc"
  OBJECT_PUT,
  OBJECT_RM,
  OBJECT_UNLINK,
  OBJECT_STAT,
#ifdef WITH_RADOSGW_RADOS
  OBJECT_MANIFEST,
  OBJECT_REWRITE,
  OBJECT_REINDEX,
#endif
  OBJECTS_EXPIRE,
  OBJECTS_EXPIRE_STALE_LIST,
  OBJECTS_EXPIRE_STALE_RM,
#ifdef WITH_RADOSGW_RADOS
  BI_GET,
  BI_PUT,
  BI_LIST,
  BI_PURGE,
  OLH_GET,
  OLH_READLOG,
#endif
#ifdef WITH_RADOSGW_RADOS
  DEDUP_STATS,
  DEDUP_ESTIMATE,
  DEDUP_ABORT,
  DEDUP_EXEC,
  DEDUP_PAUSE,
  DEDUP_RESUME,
  DEDUP_THROTTLE,
  GC_LIST,
  GC_PROCESS,
#endif
  LC_LIST,
  LC_GET,
#ifdef WITH_RADOSGW_RADOS
  LC_PROCESS,
#endif
  LC_RESHARD_FIX,
#ifdef WITH_RADOSGW_RADOS
  ORPHANS_FIND,
  ORPHANS_FINISH,
  ORPHANS_LIST_JOBS,
#endif
#include "radosgw-admin/opt_quota_ratelimit.inc"
  ZONEGROUP_ADD,
  ZONEGROUP_CREATE,
  ZONEGROUP_DEFAULT,
  ZONEGROUP_DELETE,
  ZONEGROUP_GET,
  ZONEGROUP_MODIFY,
  ZONEGROUP_SET,
  ZONEGROUP_LIST,
  ZONEGROUP_REMOVE,
  ZONEGROUP_RENAME,
  ZONEGROUP_PLACEMENT_ADD,
  ZONEGROUP_PLACEMENT_MODIFY,
  ZONEGROUP_PLACEMENT_RM,
  ZONEGROUP_PLACEMENT_LIST,
  ZONEGROUP_PLACEMENT_GET,
  ZONEGROUP_PLACEMENT_DEFAULT,
  ZONE_CREATE,
  ZONE_DELETE,
  ZONE_GET,
  ZONE_MODIFY,
  ZONE_SET,
  ZONE_LIST,
  ZONE_RENAME,
  ZONE_DEFAULT,
#ifdef WITH_RADOSGW_RADOS
  ZONE_PLACEMENT_ADD,
#endif
  ZONE_PLACEMENT_MODIFY,
  ZONE_PLACEMENT_RM,
  ZONE_PLACEMENT_LIST,
  ZONE_PLACEMENT_GET,
  CAPS_ADD,
  CAPS_RM,
#ifdef WITH_RADOSGW_RADOS
  METADATA_GET,
  METADATA_PUT,
  METADATA_RM,
  METADATA_LIST,
  METADATA_SYNC_STATUS,
  METADATA_SYNC_INIT,
  METADATA_SYNC_RUN,
  MDLOG_LIST,
  MDLOG_AUTOTRIM,
  MDLOG_TRIM,
  MDLOG_STATUS,
  SYNC_ERROR_LIST,
  SYNC_ERROR_TRIM,
#endif
  SYNC_GROUP_CREATE,
  SYNC_GROUP_MODIFY,
  SYNC_GROUP_GET,
  SYNC_GROUP_REMOVE,
  SYNC_GROUP_FLOW_CREATE,
  SYNC_GROUP_FLOW_REMOVE,
  SYNC_GROUP_PIPE_CREATE,
  SYNC_GROUP_PIPE_MODIFY,
  SYNC_GROUP_PIPE_REMOVE,
  SYNC_POLICY_GET,
  BILOG_LIST,
#ifdef WITH_RADOSGW_RADOS
  BILOG_TRIM,
  BILOG_STATUS,
  BILOG_AUTOTRIM,
  DATA_SYNC_STATUS,
  DATA_SYNC_INIT,
  DATA_SYNC_RUN,
#endif
  DATALOG_LIST,
  DATALOG_STATUS,
  DATALOG_AUTOTRIM,
  DATALOG_TRIM,
  DATALOG_TYPE,
  DATALOG_PRUNE,
  DATALOG_SEMAPHORE_LIST,
  DATALOG_SEMAPHORE_RESET,
  REALM_CREATE,
  REALM_DELETE,
  REALM_GET,
  REALM_GET_DEFAULT,
  REALM_LIST,
  REALM_LIST_PERIODS,
  REALM_RENAME,
  REALM_SET,
  REALM_DEFAULT,
  REALM_DEFAULT_RM,
  REALM_PULL,
  PERIOD_DELETE,
  PERIOD_GET,
  PERIOD_GET_CURRENT,
  PERIOD_PULL,
  PERIOD_PUSH,
  PERIOD_LIST,
  PERIOD_UPDATE,
  PERIOD_COMMIT,
  GLOBAL_QUOTA_GET,
  GLOBAL_QUOTA_SET,
  GLOBAL_QUOTA_ENABLE,
  GLOBAL_QUOTA_DISABLE,
  GLOBAL_RATELIMIT_GET,
  GLOBAL_RATELIMIT_SET,
  GLOBAL_RATELIMIT_ENABLE,
  GLOBAL_RATELIMIT_DISABLE,
  SYNC_INFO,
#ifdef WITH_RADOSGW_RADOS
  SYNC_STATUS,
#endif
  ROLE_CREATE,
  ROLE_DELETE,
  ROLE_GET,
  ROLE_TRUST_POLICY_MODIFY,
  ROLE_LIST,
  ROLE_POLICY_PUT,
  ROLE_POLICY_LIST,
  ROLE_POLICY_GET,
  ROLE_POLICY_DELETE,
  ROLE_POLICY_ATTACH,
  ROLE_POLICY_DETACH,
  ROLE_POLICY_LIST_ATTACHED,
  ROLE_UPDATE,
#ifdef WITH_RADOSGW_RADOS
  RESHARD_ADD,
  RESHARD_LIST,
  RESHARD_STATUS,
  RESHARD_PROCESS,
  RESHARD_CANCEL,
  MFA_CREATE,
  MFA_REMOVE,
  MFA_GET,
  MFA_LIST,
  MFA_CHECK,
  MFA_RESYNC,
  RESHARD_STALE_INSTANCES_LIST,
  RESHARD_STALE_INSTANCES_DELETE,
  RESHARDLOG_LIST,
  RESHARDLOG_PURGE,
#endif
#include "radosgw-admin/opt_pubsub.inc"
#include "radosgw-admin/opt_script.inc"
#include "radosgw-admin/opt_account.inc"
  RESTORE_STATUS,
  RESTORE_LIST,
  GLOBAL_CORS_GET,
};

} // namespace rgw_admin
