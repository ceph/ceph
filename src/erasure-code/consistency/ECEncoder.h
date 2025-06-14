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
    template <typename SInfo>
    class ECEncoder {
      protected:
        ceph::ErasureCodeProfile profile;
        ceph::ErasureCodeInterfaceRef ec_impl;
        std::unique_ptr<SInfo> stripe_info;
        int chunk_size;
        int ec_init_plugin(ceph::ErasureCodeInterfaceRef *ec_impl);
        int ec_init_sinfo(ceph::ErasureCodeInterfaceRef *ec_impl,
                          std::unique_ptr<ECUtil::stripe_info_t> *s);
        int ec_init_sinfo(ceph::ErasureCodeInterfaceRef *ec_impl,
                          std::unique_ptr<stripe_info_l_t> *s);
        int do_encode(ceph::bufferlist inbl,
                      ceph::bufferlist &outbl,
                      std::unique_ptr<ECUtil::stripe_info_t> *s);
        int do_encode(ceph::bufferlist inbl,
                      ceph::bufferlist &outbl,
                      std::unique_ptr<stripe_info_l_t> *s);

      public:
        ECEncoder(ceph::ErasureCodeProfile profile, int chunk_size);
        int do_encode(ceph::bufferlist inbl, ceph::bufferlist &outbl);
        int get_k(void);
        int get_m(void);
    };
  }
}