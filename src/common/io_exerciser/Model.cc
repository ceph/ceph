// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "Model.h"

ceph::io_exerciser::Model::Model(const std::string oid, uint64_t block_size) : 
num_io(0),
oid(oid),
block_size(block_size)
{

}

const uint64_t ceph::io_exerciser::Model::get_block_size() const
{
  return block_size;
}

const std::string& ceph::io_exerciser::Model::get_oid() const
{
  return oid;
}

int ceph::io_exerciser::Model::get_num_io() const
{
  return num_io;
}