#pragma once

#include "include/buffer.h"
#include "erasure-code/ErasureCode.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "osd/ECUtil.h"
#include "osd/ECUtilL.h"

using stripe_info_l_t = ECLegacy::ECUtilL::stripe_info_t;

namespace ceph {
  namespace consistency {
    class ECEncoder {
      protected:
        ceph::ErasureCodeProfile profile;
        int chunk_size;
        int ec_init_plugin(ceph::ErasureCodeInterfaceRef *ec_impl);
        int ec_init_sinfo_optimized(ceph::ErasureCodeInterfaceRef *ec_impl,
                                    std::unique_ptr<ECUtil::stripe_info_t> *sinfo);
        int ec_init_sinfo_legacy(ceph::ErasureCodeInterfaceRef *ec_impl,
                                 std::unique_ptr<stripe_info_l_t> *sinfo);
        int do_encode_optimized(ceph::bufferlist inbl,
                                ceph::bufferlist &outbl);
        int do_encode_legacy(ceph::bufferlist inbl,
                             ceph::bufferlist &outbl);

      public:
        ECEncoder(ceph::ErasureCodeProfile profile, int chunk_size);
        int do_encode(ceph::bufferlist inbl,
                      ceph::bufferlist &outbl,
                      bool is_optimized);
    };
  }
}