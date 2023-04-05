#include "rgw_d4n_policy.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

bool RGWD4NPolicy::should_cache(int objSize, int minSize) {
  if (objSize < minSize)
    return true;
  else
    return false;
}

bool RGWD4NPolicy::should_cache(std::string uploadType) {
  if (uploadType == "PUT")
    return true;
  else if (uploadType == "MULTIPART")
    return false;
  else /* Should not reach here */
    return false;
}
