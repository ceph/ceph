// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/ceph_assert.h"
#include "msg/async/compression_onwire.h"

namespace ceph::compression::onwire {

std::optional<ceph::bufferlist> TxHandler::compress(const ceph::bufferlist&)
{
  ceph_abort_msg("on-wire compression is not supported by crimson");
}

void TxHandler::done()
{
  ceph_abort_msg("on-wire compression is not supported by crimson");
}

std::optional<ceph::bufferlist> RxHandler::decompress(const ceph::bufferlist&)
{
  ceph_abort_msg("on-wire decompression is not supported by crimson");
}

} // namespace ceph::compression::onwire
