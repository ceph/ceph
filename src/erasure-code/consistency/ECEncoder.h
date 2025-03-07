#pragma once

#include "include/buffer.h"
#include "erasure-code/ErasureCode.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "osd/ECUtil.h"

namespace ceph {
  namespace consistency {
    class ECEncoder {
      protected:
        ceph::ErasureCodeProfile profile;
        int stripe_unit;
      public:
        ECEncoder(ceph::ErasureCodeProfile profile, int stripe_unit);
        int ec_init(int stripe_unit,
                     ceph::ErasureCodeInterfaceRef *ec_impl,
                     std::unique_ptr<ECUtil::stripe_info_t> *sinfo);
        int do_encode(ceph::bufferlist inbl,
                      ceph::bufferlist &outbl);

    };
  }
}