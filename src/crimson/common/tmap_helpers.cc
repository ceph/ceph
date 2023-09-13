// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/tmap_helpers.h"

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/rados.h"

namespace detail {

#define decode_or_return(v, bp) \
  try {				\
    ::decode(v, bp);		\
  } catch (...)	{		\
    return -EINVAL;		\
  }

class TMapContents {
  std::map<std::string, bufferlist> keys;
  bufferlist header;
public:
  TMapContents() = default;

  int decode(bufferlist::const_iterator &bliter) {
    keys.clear();
    header.clear();
    if (bliter.end()) {
      return 0;
    }
    decode_or_return(header, bliter);
    __u32 num_keys;
    decode_or_return(num_keys, bliter);
    for (; num_keys > 0; --num_keys) {
      std::string key;
      decode_or_return(key, bliter);
      decode_or_return(keys[key], bliter);
    }
    return 0;
  }

  bufferlist encode() {
    bufferlist bl;
    ::encode(header, bl);
    ::encode(static_cast<__u32>(keys.size()), bl);
    for (auto &[k, v]: keys) {
      ::encode(k, bl);
      ::encode(v, bl);
    }
    return bl;
  }

  int update(bufferlist::const_iterator in) {
    while (!in.end()) {
      __u8 op;
      decode_or_return(op, in);

      if (op == CEPH_OSD_TMAP_HDR) {
	decode_or_return(header, in);
	continue;
      }

      std::string key;
      decode_or_return(key, in);

      switch (op) {
      case CEPH_OSD_TMAP_SET: {
	decode_or_return(keys[key], in);
	break;
      }
      case CEPH_OSD_TMAP_CREATE: {
	if (keys.contains(key)) {
	  return -EEXIST;
	}
	decode_or_return(keys[key], in);
	break;
      }
      case CEPH_OSD_TMAP_RM: {
	auto kiter = keys.find(key);
	if (kiter == keys.end()) {
	  return -ENOENT;
	}
	keys.erase(kiter);
	break;
      }
      case CEPH_OSD_TMAP_RMSLOPPY: {
	keys.erase(key);
	break;
      }
      }
    }
    return 0;
  }

  int put(bufferlist::const_iterator in) {
    return 0;
  }
};

}

namespace crimson::common {

using do_tmap_up_ret = tl::expected<bufferlist, int>;
do_tmap_up_ret do_tmap_up(bufferlist::const_iterator in, bufferlist contents)
{
  detail::TMapContents tmap;
  auto bliter = contents.cbegin();
  int r = tmap.decode(bliter);
  if (r < 0) {
    return tl::unexpected(r);
  }
  r = tmap.update(in);
  if (r < 0) {
    return tl::unexpected(r);
  }
  return tmap.encode();
}

using do_tmap_up_ret = tl::expected<bufferlist, int>;
do_tmap_up_ret do_tmap_put(bufferlist::const_iterator in)
{
  detail::TMapContents tmap;
  int r = tmap.decode(in);
  if (r < 0) {
    return tl::unexpected(r);
  }
  return tmap.encode();
}

}
