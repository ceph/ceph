// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_LUKS_MAGIC_H
#define CEPH_LIBRBD_CRYPTO_LUKS_MAGIC_H

#include "common/ceph_context.h"
#include "include/buffer.h"

namespace librbd {
namespace crypto {
namespace luks {

class Magic {
public:
  static int is_luks(ceph::bufferlist& bl);
  static int is_rbd_clone(ceph::bufferlist& bl);

  static int replace_magic(CephContext* cct, ceph::bufferlist& bl);
private:
  static int read(ceph::bufferlist& bl, uint32_t bl_off,
                  uint32_t read_size, char* result);
  static int cmp(ceph::bufferlist& bl, uint32_t bl_off,
                 const std::string& cmp_str);
  static void transform_secondary_header_magic(char* magic);
};

} // namespace luks
} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_LUKS_MAGIC_H
