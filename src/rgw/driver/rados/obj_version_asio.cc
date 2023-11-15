// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "obj_version_asio.h"
#include "rgw_common.h"

namespace rgw::neorados {
namespace version = neorados::cls::version;
void VersionTracker::prepare_read(neorados::ReadOp& op) {
  auto check_objv = version_for_check();

  if (check_objv) {
    op.exec(version::check(*check_objv, VER_COND_EQ));
  }

  op.exec(version::read(&read_version));
}

void VersionTracker::prepare_write(neorados::WriteOp& op) {
  auto check_objv = version_for_check();
  auto modify_version = version_for_write();

  if (check_objv) {
    op.exec(version::check(*check_objv, VER_COND_EQ));
  }

  if (modify_version) {
    op.exec(version::set(*modify_version));
  } else {
    op.exec(version::inc());
  }
}

void VersionTracker::apply_write()
{
  const bool checked = (read_version.ver != 0);
  const bool incremented = (write_version.ver == 0);

  if (checked && incremented) {
    // apply cls_version_inc() so our next operation can recheck it
    ++read_version.ver;
  } else {
    read_version = write_version;
  }
  write_version = obj_version();
}

void VersionTracker::new_write_version(CephContext *cct)
{
  static constexpr auto TAG_LEN = 24;
  write_version.ver = 1;

  write_version.tag.clear();
  append_rand_alpha(cct, write_version.tag, write_version.tag, TAG_LEN);
}

}
