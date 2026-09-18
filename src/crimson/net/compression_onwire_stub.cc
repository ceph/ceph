// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/ceph_assert.h"
#include "msg/async/compression_onwire.h"

namespace ceph::compression::onwire {

std::optional<ceph::bufferlist> TxHandler::compress(const ceph::bufferlist&)
{
  ceph_abort();
}

void TxHandler::done()
{
  ceph_abort();
}

std::optional<ceph::bufferlist> RxHandler::decompress(const ceph::bufferlist&)
{
  ceph_abort();
}

} // namespace ceph::compression::onwire
