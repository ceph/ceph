// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "ImageReplayer.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd-mirror: "

using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

namespace rbd {
namespace mirror {

ImageReplayer::ImageReplayer(RadosRef local, RadosRef remote,
			     int64_t remote_pool_id,
			     const string &remote_image_id) :
  m_lock(stringify("rbd::mirror::ImageReplayer ") + stringify(remote_pool_id) +
	 string(" ") + remote_image_id),
  m_remote_pool_id(remote_pool_id),
  m_image_id(remote_image_id),
  m_local(local),
  m_remote(remote)
{
}

ImageReplayer::~ImageReplayer()
{
}

int ImageReplayer::start()
{
  int r = m_remote->ioctx_create2(m_remote_pool_id, m_remote_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for remote pool " << m_remote_pool_id
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }
  m_pool_name = m_remote_ioctx.get_pool_name();
  r = m_local->ioctx_create(m_pool_name.c_str(), m_local_ioctx);
  if (r < 0) {
    derr << "error opening ioctx for local pool " << m_pool_name
	 << " : " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void ImageReplayer::stop()
{
  m_remote_ioctx.close();
  m_local_ioctx.close();
}

} // namespace mirror
} // namespace rbd
