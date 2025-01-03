// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <string>
#include <errno.h>

#include "objclass/objclass.h"
#include "osd/osd_types.h"

#include "cls_cephfs.h"

CLS_VER(1,0)
CLS_NAME(cephfs)

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

std::ostream &operator<<(std::ostream &out, const ObjCeiling &in)
{
  out << "id: " << in.id << " size: " << in.size;
  return out;
}


/**
 * Set a named xattr to a given value, if and only if the xattr
 * is not already set to a greater value.
 *
 * If the xattr is missing, then it is set to the input integer.
 *
 * @param xattr_name: name of xattr to compare against and set
 * @param input_val: candidate new value, of encode()'able type
 * @returns 0 on success (irrespective of whether our new value
 *          was used) else an error code
 */
template <typename A>
static int set_if_greater(cls_method_context_t hctx,
    const std::string &xattr_name, const A input_val)
{
  bufferlist existing_val_bl;

  bool set_val = false;
  int r = cls_cxx_getxattr(hctx, xattr_name.c_str(), &existing_val_bl);
  if (r == -ENOENT || existing_val_bl.length() == 0) {
    set_val = true;
  } else if (r >= 0) {
    auto existing_p = existing_val_bl.cbegin();
    try {
      A existing_val;
      decode(existing_val, existing_p);
      if (!existing_p.end()) {
        // Trailing junk?  Consider it invalid and overwrite
        set_val = true;
      } else {
        // Valid existing value, do comparison
        set_val = input_val > existing_val;
      }
    } catch (const ceph::buffer::error &err) {
      // Corrupt or empty existing value, overwrite it
      set_val = true;
    }
  } else {
    return r;
  }

  // Conditionally set the new xattr
  if (set_val) {
    bufferlist set_bl;
    encode(input_val, set_bl);
    return cls_cxx_setxattr(hctx, xattr_name.c_str(), &set_bl);
  } else {
    return 0;
  }
}

static int accumulate_inode_metadata(cls_method_context_t hctx,
    bufferlist *in, bufferlist *out)
{
  ceph_assert(in != NULL);
  ceph_assert(out != NULL);

  int r = 0;

  // Decode `in`
  auto q = in->cbegin();
  AccumulateArgs args;
  try {
    args.decode(q);
  } catch (const ceph::buffer::error &err) {
    return -EINVAL;
  }

  ObjCeiling ceiling(args.obj_index, args.obj_size);
  r = set_if_greater(hctx, args.obj_xattr_name, ceiling);
  if (r < 0) {
    return r;
  }

  r = set_if_greater(hctx, args.mtime_xattr_name, args.mtime);
  if (r < 0) {
    return r;
  }

  r = set_if_greater(hctx, args.obj_size_xattr_name, args.obj_size);
  if (r < 0) {
    return r;
  }

  return 0;
}

// I want to select objects that have a name ending 00000000
// and an xattr (scrub_tag) not equal to a specific value.
// This is so special case that we can't really pretend it's
// generic, so just fess up and call this the cephfs filter.
class PGLSCephFSFilter : public PGLSFilter {
protected:
  std::string scrub_tag;
public:
  int init(bufferlist::const_iterator& params) override {
    try {
      InodeTagFilterArgs args;
      args.decode(params);
      scrub_tag = args.scrub_tag;
    } catch (ceph::buffer::error &e) {
      return -EINVAL;
    }

    if (scrub_tag.empty()) {
      xattr = "";
    } else {
      xattr = "_scrub_tag";
    }

    return 0;
  }

  ~PGLSCephFSFilter() override {}
  bool reject_empty_xattr() const override { return false; }
  bool filter(const hobject_t& obj,
              const bufferlist& xattr_data) const override;
};

bool PGLSCephFSFilter::filter(const hobject_t &obj,
                              const bufferlist& xattr_data) const
{
  const std::string need_ending = ".00000000";
  const std::string &obj_name = obj.oid.name;

  if (obj_name.length() < need_ending.length()) {
    return false;
  }

  const bool match = obj_name.compare (obj_name.length() - need_ending.length(), need_ending.length(), need_ending) == 0;
  if (!match) {
    return false;
  }

  if (!scrub_tag.empty() && xattr_data.length() > 0) {
    std::string tag_ondisk;
    auto q = xattr_data.cbegin();
    try {
      decode(tag_ondisk, q);
      if (tag_ondisk == scrub_tag)
	return false;
    } catch (const ceph::buffer::error &err) {
    }
  }

  return true;
}

PGLSFilter *inode_tag_filter()
{
  return new PGLSCephFSFilter();
}

/**
 * initialize class
 *
 * We do two things here: we register the new class, and then register
 * all of the class's methods.
 */
CLS_INIT(cephfs)
{
  // this log message, at level 0, will always appear in the ceph-osd
  // log file.
  CLS_LOG(0, "loading cephfs");

  cls_handle_t h_class;
  cls_method_handle_t h_accumulate_inode_metadata;

  cls_register("cephfs", &h_class);
  cls_register_cxx_method(h_class, "accumulate_inode_metadata",
			  CLS_METHOD_WR | CLS_METHOD_RD,
			  accumulate_inode_metadata, &h_accumulate_inode_metadata);

  // A PGLS filter
  cls_register_cxx_filter(h_class, "inode_tag", inode_tag_filter);
}

