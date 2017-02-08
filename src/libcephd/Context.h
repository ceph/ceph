// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBOSD_CONTEXT_H
#define CEPH_LIBOSD_CONTEXT_H

class CephContext;

namespace ceph
{
namespace osd
{

int context_create(int id, const char *config, const char *cluster,
                   int argc, const char **argv, CephContext** cct_out);

} // namespace osd
} // namespace ceph

#endif // CEPH_LIBOSD_CONTEXT_H
