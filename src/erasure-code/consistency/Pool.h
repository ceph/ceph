#pragma once

#include "global/global_init.h"
#include "global/global_context.h"
#include "erasure-code/ErasureCodePlugin.h"

namespace ceph {
  namespace consistency {
    class Pool {
      protected:
        std::string pool_name;
        ceph::ErasureCodeProfile profile;

      public:
        Pool(const std::string& pool_name, const ceph::ErasureCodeProfile& profile);
        ceph::ErasureCodeProfile get_ec_profile(void);
        std::string get_pool_name(void); 
    };
  }
}