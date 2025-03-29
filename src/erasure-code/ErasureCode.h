// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_ERASURE_CODE_H
#define CEPH_ERASURE_CODE_H

/*! @file ErasureCode.h
    @brief Base class for erasure code plugins implementors

 */

#include "ErasureCodeInterface.h"
 #include "include/ceph_assert.h"

namespace ceph {

class ErasureCode : public ErasureCodeInterface {
 public:
  static const unsigned SIMD_ALIGN;

  std::vector<shard_id_t> chunk_mapping;
  ErasureCodeProfile _profile;

  // for CRUSH rule
  std::string rule_root;
  std::string rule_failure_domain;
  std::string rule_device_class;
  int rule_osds_per_failure_domain = -1;
  int rule_num_failure_domains = -1;

  ~ErasureCode() override {
  }

  int init(ceph::ErasureCodeProfile &profile, std::ostream *ss) override;

  const ErasureCodeProfile &get_profile() const override {
    return _profile;
  }

  int create_rule(const std::string &name,
                  CrushWrapper &crush,
                  std::ostream *ss) const override;

  int sanity_check_k_m(int k, int m, std::ostream *ss);

  unsigned int get_coding_chunk_count() const override {
    return get_chunk_count() - get_data_chunk_count();
  }

  virtual int get_sub_chunk_count() override {
    return 1;
  }

  [[deprecated]]
  virtual int _minimum_to_decode(const std::set<int> &want_to_read,
                                 const std::set<int> &available_chunks,
                                 std::set<int> *minimum);

  virtual int _minimum_to_decode(const shard_id_set &want_to_read,
                                 const shard_id_set &available_chunks,
                                 shard_id_set *minimum);

  [[deprecated]]
  int minimum_to_decode(const std::set<int> &want_to_read,
                        const std::set<int> &available,
                        std::map<int,
                                 std::vector<std::pair<int,
                                                       int>>> *minimum) override;

  int minimum_to_decode(const shard_id_set &want_to_read,
                        const shard_id_set &available,
                        shard_id_set &minimum_set,
                        mini_flat_map<shard_id_t, std::vector<std::pair<int, int>>> *minimum_sub_chunks) override;

  [[deprecated]]
  int minimum_to_decode_with_cost(const std::set<int> &want_to_read,
                                  const std::map<int, int> &available,
                                  std::set<int> *minimum) override;

  int minimum_to_decode_with_cost(const shard_id_set &want_to_read,
                                  const mini_flat_map<shard_id_t, int> &available,
                                  shard_id_set *minimum) override;

  int encode_prepare(const bufferlist &raw,
                     mini_flat_map<shard_id_t, bufferlist> &encoded) const;

  [[deprecated]]
  int encode_prepare(const bufferlist &raw,
                     std::map<int, bufferlist> &encoded) const;

  int encode(const shard_id_set &want_to_encode,
             const bufferlist &in,
             mini_flat_map<shard_id_t, bufferlist> *encoded) override;

  [[deprecated]]
  int encode(const std::set<int> &want_to_encode,
             const bufferlist &in,
             std::map<int, bufferlist> *encoded) override;

  [[deprecated]]
  int decode(const std::set<int> &want_to_read,
             const std::map<int, bufferlist> &chunks,
             std::map<int, bufferlist> *decoded,
             int chunk_size) override;

  int decode(const shard_id_set &want_to_read,
             const mini_flat_map<shard_id_t, bufferlist> &chunks,
             mini_flat_map<shard_id_t, bufferlist> *decoded, int chunk_size) override;

  [[deprecated]]
  virtual int _decode(const std::set<int> &want_to_read,
                      const std::map<int, bufferlist> &chunks,
                      std::map<int, bufferlist> *decoded);

  virtual int _decode(const shard_id_set &want_to_read,
                      const mini_flat_map<shard_id_t, bufferlist> &chunks,
                      mini_flat_map<shard_id_t, bufferlist> *decoded);

  const std::vector<shard_id_t> &get_chunk_mapping() const override;

  int to_mapping(const ErasureCodeProfile &profile, std::ostream *ss);

  static int to_int(const std::string &name,
                    ErasureCodeProfile &profile,
                    int *value,
                    const std::string &default_value,
                    std::ostream *ss);

  static int to_bool(const std::string &name,
                     ErasureCodeProfile &profile,
                     bool *value,
                     const std::string &default_value,
                     std::ostream *ss);

  static int to_string(const std::string &name,
                       ErasureCodeProfile &profile,
                       std::string *value,
                       const std::string &default_value,
                       std::ostream *ss);

  [[deprecated]]
  int decode_concat(const std::set<int> &want_to_read,
                    const std::map<int, bufferlist> &chunks,
                    bufferlist *decoded) override;
  [[deprecated]]
  int decode_concat(const std::map<int, bufferlist> &chunks,
                    bufferlist *decoded) override;

  void encode_delta(const bufferptr &old_data,
                    const bufferptr &new_data,
                    bufferptr *delta_maybe_in_place) override {
    ceph_abort("Not yet supported by this plugin");
  }

  void apply_delta(const shard_id_map< bufferptr> &in,
                   shard_id_map< bufferptr> &out) override {
    ceph_abort("Not yet supported by this plugin");
  }

 protected:
  int parse(const ErasureCodeProfile &profile, std::ostream *ss);

 private:
  [[deprecated]]
  unsigned int chunk_index(unsigned int i) const;
  shard_id_t chunk_index(raw_shard_id_t i) const;
};
}

#endif
