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
#include "radosgw-admin/opt_bucket.inc"
#ifdef WITH_RADOSGW_RADOS
#include "radosgw-admin/opt_bucket_resync_encrypted_multipart.inc"
#endif
#include "radosgw-admin/opt_bucket_sync.inc"
#include "radosgw-admin/opt_bucket_logging.inc"
#ifdef WITH_RADOSGW_RADOS
#include "radosgw-admin/opt_log.inc"
#endif
#include "radosgw-admin/opt_usage.inc"
#include "radosgw-admin/opt_account.inc"
#include "radosgw-admin/opt_object.inc"
#include "radosgw-admin/opt_bi.inc"
#include "radosgw-admin/opt_olh.inc"
#include "radosgw-admin/opt_dedup.inc"
#include "radosgw-admin/opt_gc.inc"
#include "radosgw-admin/opt_lc.inc"
#ifdef WITH_RADOSGW_RADOS
  ORPHANS_FIND,
  ORPHANS_FINISH,
  ORPHANS_LIST_JOBS,
#endif
#include "radosgw-admin/opt_quota_ratelimit.inc"
#include "radosgw-admin/opt_realm.inc"
#include "radosgw-admin/opt_zonegroup.inc"
#include "radosgw-admin/opt_zone.inc"
#include "radosgw-admin/opt_period.inc"
#ifdef WITH_RADOSGW_RADOS
#include "radosgw-admin/opt_metadata.inc"
#include "radosgw-admin/opt_sync.inc"
#endif
#include "radosgw-admin/opt_bilog.inc"
#ifdef WITH_RADOSGW_RADOS
#include "radosgw-admin/opt_datalog.inc"
#endif
#include "radosgw-admin/opt_role.inc"
#ifdef WITH_RADOSGW_RADOS
#include "radosgw-admin/opt_reshard.inc"
#include "radosgw-admin/opt_mfa.inc"
#endif
#include "radosgw-admin/opt_pubsub.inc"
#include "radosgw-admin/opt_script.inc"
#include "radosgw-admin/opt_restore.inc"
#include "radosgw-admin/opt_cors.inc"
};

} // namespace rgw_admin
