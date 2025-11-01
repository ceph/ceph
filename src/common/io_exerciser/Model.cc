// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "Model.h"

using Model = ceph::io_exerciser::Model;

Model::Model(const std::string& primary_oid, const std::string& secondary_oid, uint64_t block_size)
    : num_io(0), primary_oid(primary_oid), secondary_oid(secondary_oid), block_size(block_size) {}

const std::string Model::get_primary_oid() const { return primary_oid; }

const std::string Model::get_secondary_oid() const { return secondary_oid; }

void Model::set_primary_oid(const std::string& new_oid) {
    primary_oid = new_oid;
}

void Model::set_secondary_oid(const std::string& new_oid) {
    secondary_oid = new_oid;
}

void Model::swap_primary_secondary_oid() {
    std::string old_primary;
    old_primary = get_primary_oid();
    set_primary_oid(get_secondary_oid());
    set_secondary_oid(old_primary);
}

const uint64_t Model::get_block_size() const { return block_size; }

int Model::get_num_io() const { return num_io; }

void Model::set_test_step(ceph::io_exerciser::Sequence new_seq, int new_step) {
  curseq = new_seq;
  step = new_step;
}
