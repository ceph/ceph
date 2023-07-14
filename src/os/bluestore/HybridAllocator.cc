// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "HybridAllocator.h"

#define dout_context (T::get_context())
#define dout_subsys ceph_subsys_bluestore
#undef  dout_prefix
#define dout_prefix *_dout << (std::string(this->get_type()) + "::").c_str()

/*
 * class HybridAvlAllocator
 *
 *
 */
const char* HybridAvlAllocator::get_type() const
{
  return "hybrid";
}

#include "HybridAllocator_impl.h"
