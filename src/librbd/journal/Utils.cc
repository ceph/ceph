// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/Utils.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/journal/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::"

namespace librbd {
namespace journal {
namespace util {

int C_DecodeTag::decode(bufferlist::iterator *it, TagData *tag_data) {
  try {
    using ceph::decode;
    decode(*tag_data, *it);
  } catch (const buffer::error &err) {
    return -EBADMSG;
  }
  return 0;
}

int C_DecodeTag::process(int r) {
  if (r < 0) {
    lderr(cct) << "C_DecodeTag: " << this << " " << __func__ << ": "
               << "failed to allocate tag: " << cpp_strerror(r)
      	 << dendl;
    return r;
  }

  Mutex::Locker locker(*lock);
  *tag_tid = tag.tid;

  bufferlist::iterator data_it = tag.data.begin();
  r = decode(&data_it, tag_data);
  if (r < 0) {
    lderr(cct) << "C_DecodeTag: " << this << " " << __func__ << ": "
               << "failed to decode allocated tag" << dendl;
    return r;
  }

  ldout(cct, 20) << "C_DecodeTag: " << this << " " << __func__ << ": "
                 << "allocated journal tag: "
                 << "tid=" << tag.tid << ", "
                 << "data=" << *tag_data << dendl;
  return 0;
}

int C_DecodeTags::process(int r) {
  if (r < 0) {
    lderr(cct) << "C_DecodeTags: " << this << " " << __func__ << ": "
               << "failed to retrieve journal tags: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  if (tags.empty()) {
    lderr(cct) << "C_DecodeTags: " << this << " " << __func__ << ": "
               << "no journal tags retrieved" << dendl;
    return -ENOENT;
  }

  Mutex::Locker locker(*lock);
  *tag_tid = tags.back().tid;
  bufferlist::iterator data_it = tags.back().data.begin();
  r = C_DecodeTag::decode(&data_it, tag_data);
  if (r < 0) {
    lderr(cct) << "C_DecodeTags: " << this << " " << __func__ << ": "
               << "failed to decode journal tag" << dendl;
    return r;
  }

  ldout(cct, 20) << "C_DecodeTags: " << this << " " << __func__ << ": "
                 << "most recent journal tag: "
                 << "tid=" << *tag_tid << ", "
                 << "data=" << *tag_data << dendl;
  return 0;
}

} // namespace util
} // namespace journal
} // namespace librbd
