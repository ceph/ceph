
#ifndef ROLE_SELECTOR_H_
#define ROLE_SELECTOR_H_

#include <string>
#include <vector>
#include "mds/mdstypes.h"
#include "mds/FSMap.h"

/**
 * When you want to let the user act on a single rank in a namespace,
 * or all of them.
 */
class MDSRoleSelector
{
  public:
    const std::vector<mds_role_t> &get_roles() const {return roles;}
    int parse(const FSMap &fsmap, std::string const &str);
    MDSRoleSelector()
      : fscid(FS_CLUSTER_ID_NONE)
    {}
    fs_cluster_id_t get_ns() const
    {
      return fscid;
    }
  protected:
    int parse_rank(
        const FSMap &fsmap,
        std::string const &str);
    std::vector<mds_role_t> roles;
    fs_cluster_id_t fscid;
};

#endif // ROLE_SELECTOR_H_

