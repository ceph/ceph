// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Magic.h"

#include "common/dout.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::luks::Magic: " << __func__ \
                           << ": "

namespace librbd {
namespace crypto {
namespace luks {

namespace {

constexpr uint64_t MAGIC_LENGTH = 6;
const std::string LUKS_MAGIC = "LUKS\xba\xbe";
const std::string RBD_CLONE_MAGIC = "RBDL\xba\xbe";

} // anonymous namespace

int Magic::read(ceph::bufferlist &bl, uint32_t bl_off,
                uint32_t read_size, char* result) {
  if (bl_off + read_size > bl.length()) {
    return -EINVAL;
  }

  memcpy(result, bl.c_str() + bl_off, read_size);
  return 0;
}

int Magic::cmp(ceph::bufferlist &bl, uint32_t bl_off,
               const std::string &cmp_str) {
  auto cmp_length = cmp_str.length();

  if (bl_off + cmp_length > bl.length()) {
    return -EINVAL;
  }

  if (memcmp(bl.c_str() + bl_off, cmp_str.c_str(), cmp_length)) {
    return 0;
  }

  return 1;
}

int Magic::is_luks(ceph::bufferlist& bl) {
  return cmp(bl, 0, LUKS_MAGIC);
}

int Magic::is_rbd_clone(ceph::bufferlist& bl) {
  return cmp(bl, 0, RBD_CLONE_MAGIC);
}

void Magic::transform_secondary_header_magic(char* magic) {
  std::swap(magic[0], magic[3]);
  std::swap(magic[1], magic[2]);
}

int Magic::replace_magic(CephContext* cct, ceph::bufferlist& bl) {
  const std::string *old_magic, *new_magic;
  if (is_luks(bl) > 0) {
    old_magic = &LUKS_MAGIC;
    new_magic = &RBD_CLONE_MAGIC;
  } else if (is_rbd_clone(bl) > 0) {
    old_magic = &RBD_CLONE_MAGIC;
    new_magic = &LUKS_MAGIC;
  } else {
    lderr(cct) << "invalid magic: " << dendl;
    return -EILSEQ;
  }

  // read luks version
  uint16_t version;
  auto r = read(bl, MAGIC_LENGTH, sizeof(version), (char*)&version);
  if (r < 0) {
    lderr(cct) << "cannot read header version: " << cpp_strerror(r) << dendl;
    return r;
  }
  boost::endian::big_to_native_inplace(version);

  switch (version) {
    case 1: {
      // LUKS1, no secondary header
      break;
    }
    case 2: {
      // LUKS2, secondary header follows primary header
      // read header size
      uint64_t hdr_size;
      r = read(bl, MAGIC_LENGTH + sizeof(version), sizeof(hdr_size),
                    (char*)&hdr_size);
      if (r < 0) {
        lderr(cct) << "cannot read header size: " << cpp_strerror(r) << dendl;
        return r;
      }
      boost::endian::big_to_native_inplace(hdr_size);

      if ((uint32_t)hdr_size + MAGIC_LENGTH > bl.length()) {
        ldout(cct, 20) << "cannot replace secondary header magic" << dendl;
        return -EINVAL;
      }

      // check secondary header magic
      auto secondary_header_magic = bl.c_str() + hdr_size;
      transform_secondary_header_magic(secondary_header_magic);
      auto is_secondary_header_magic_valid =
              !memcmp(secondary_header_magic, old_magic->c_str(), MAGIC_LENGTH);
      if (!is_secondary_header_magic_valid) {
        transform_secondary_header_magic(secondary_header_magic);
        lderr(cct) << "invalid secondary header magic" << dendl;
        return -EILSEQ;
      }

      // replace secondary header magic
      memcpy(secondary_header_magic, new_magic->c_str(), MAGIC_LENGTH);
      transform_secondary_header_magic(secondary_header_magic);

      break;
    }
    default: {
      lderr(cct) << "bad header version: " << version << dendl;
      return -EINVAL;
    }
  }

  // switch primary header magic
  memcpy(bl.c_str(), new_magic->c_str(), MAGIC_LENGTH);

  return 0;
}

} // namespace luks
} // namespace crypto
} // namespace librbd
