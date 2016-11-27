
#include "RoleSelector.h"

int MDSRoleSelector::parse_rank(
    const FSMap &fsmap,
    std::string const &str)
{
  if (str == "all" || str == "*") {
    std::set<mds_rank_t> in;
    const MDSMap &mds_map = fsmap.get_filesystem(fscid)->mds_map;
    mds_map.get_mds_set(in);

    for (auto rank : in) {
      roles.push_back(mds_role_t(fscid, rank));
    }

    return 0;
  } else {
    std::string rank_err;
    mds_rank_t rank = strict_strtol(str.c_str(), 10, &rank_err);
    if (!rank_err.empty()) {
      return -EINVAL;
    }
    if (fsmap.get_filesystem(fscid)->mds_map.is_dne(rank)) {
      return -ENOENT;
    }
    roles.push_back(mds_role_t(fscid, rank));
    return 0;
  }
}

int MDSRoleSelector::parse(const FSMap &fsmap, std::string const &str)
{
  auto colon_pos = str.find(":");
  if (colon_pos == std::string::npos) {
    // An unqualified rank.  Only valid if there is only one
    // namespace.
    if (fsmap.filesystem_count() == 1) {
      fscid = fsmap.get_filesystem()->fscid;
      return parse_rank(fsmap, str);
    } else {
      return -EINVAL;
    }
  } else if (colon_pos == 0 || colon_pos == str.size() - 1) {
    return -EINVAL;
  } else {
    const std::string ns_str = str.substr(0, colon_pos);
    const std::string rank_str = str.substr(colon_pos + 1);
    std::shared_ptr<const Filesystem> fs_ptr;
    int r = fsmap.parse_filesystem(ns_str, &fs_ptr);
    if (r != 0) {
      return r;
    }
    fscid = fs_ptr->fscid;
    return parse_rank(fsmap, rank_str);
  }
}

