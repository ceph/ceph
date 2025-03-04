// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_ERASURE_CODE_LRC_H
#define CEPH_ERASURE_CODE_LRC_H

#include "include/err.h"
#include "json_spirit/json_spirit.h"
#include "erasure-code/ErasureCode.h"

#define ERROR_LRC_ARRAY			-(MAX_ERRNO + 1)
#define ERROR_LRC_OBJECT		-(MAX_ERRNO + 2)
#define ERROR_LRC_INT			-(MAX_ERRNO + 3)
#define ERROR_LRC_STR			-(MAX_ERRNO + 4)
#define ERROR_LRC_PLUGIN		-(MAX_ERRNO + 5)
#define ERROR_LRC_DESCRIPTION		-(MAX_ERRNO + 6)
#define ERROR_LRC_PARSE_JSON		-(MAX_ERRNO + 7)
#define ERROR_LRC_MAPPING		-(MAX_ERRNO + 8)
#define ERROR_LRC_MAPPING_SIZE		-(MAX_ERRNO + 9)
#define ERROR_LRC_FIRST_MAPPING		-(MAX_ERRNO + 10)
#define ERROR_LRC_COUNT_CONSTRAINT	-(MAX_ERRNO + 11)
#define ERROR_LRC_CONFIG_OPTIONS	-(MAX_ERRNO + 12)
#define ERROR_LRC_LAYERS_COUNT		-(MAX_ERRNO + 13)
#define ERROR_LRC_RULE_OP		-(MAX_ERRNO + 14)
#define ERROR_LRC_RULE_TYPE		-(MAX_ERRNO + 15)
#define ERROR_LRC_RULE_N		-(MAX_ERRNO + 16)
#define ERROR_LRC_ALL_OR_NOTHING	-(MAX_ERRNO + 17)
#define ERROR_LRC_GENERATED		-(MAX_ERRNO + 18)
#define ERROR_LRC_K_M_MODULO		-(MAX_ERRNO + 19)
#define ERROR_LRC_K_MODULO		-(MAX_ERRNO + 20)
#define ERROR_LRC_M_MODULO		-(MAX_ERRNO + 21)

class ErasureCodeLrc final : public ceph::ErasureCode {
public:
  static const std::string DEFAULT_KML;

  struct Layer {
    explicit Layer(const std::string &_chunks_map) : chunks_map(_chunks_map) { }
    ceph::ErasureCodeInterfaceRef erasure_code;
    std::vector<int> data;
    std::vector<int> coding;
    std::vector<int> chunks;
    std::set<int> chunks_as_set;
    std::string chunks_map;
    ceph::ErasureCodeProfile profile;
  };
  std::vector<Layer> layers;
  std::string directory;
  unsigned int chunk_count;
  unsigned int data_chunk_count;
  std::string rule_root;
  std::string rule_device_class;
  struct Step {
    Step(const std::string &_op, const std::string &_type, int _n) :
      op(_op),
      type(_type),
      n(_n) {}
    std::string op;
    std::string type;
    int n;
  };
  std::vector<Step> rule_steps;

  explicit ErasureCodeLrc(const std::string &dir)
    : directory(dir),
      chunk_count(0), data_chunk_count(0), rule_root("default")
  {
    rule_steps.push_back(Step("chooseleaf", "host", 0));
  }

  ~ErasureCodeLrc() override {}

  std::set<int> get_erasures(const std::set<int> &need,
			const std::set<int> &available) const;

  int _minimum_to_decode(const std::set<int> &want_to_read,
			 const std::set<int> &available,
			 std::set<int> *minimum) override;

  int create_rule(const std::string &name,
			     CrushWrapper &crush,
			     std::ostream *ss) const override;

  unsigned int get_chunk_count() const override {
    return chunk_count;
  }

  unsigned int get_data_chunk_count() const override {
    return data_chunk_count;
  }

  unsigned int get_chunk_size(unsigned int stripe_width) const override;

  int encode_chunks(const std::set<int> &want_to_encode,
		    std::map<int, ceph::buffer::list> *encoded) override;

  int decode_chunks(const std::set<int> &want_to_read,
		    const std::map<int, ceph::buffer::list> &chunks,
		    std::map<int, ceph::buffer::list> *decoded) override;

  int init(ceph::ErasureCodeProfile &profile, std::ostream *ss) override;

  virtual int parse(ceph::ErasureCodeProfile &profile, std::ostream *ss);

  int parse_kml(ceph::ErasureCodeProfile &profile, std::ostream *ss);

  int parse_rule(ceph::ErasureCodeProfile &profile, std::ostream *ss);

  int parse_rule_step(const std::string &description_string,
		      json_spirit::mArray description,
		      std::ostream *ss);

  int layers_description(const ceph::ErasureCodeProfile &profile,
			 json_spirit::mArray *description,
			 std::ostream *ss) const;
  int layers_parse(const std::string &description_string,
		   json_spirit::mArray description,
		   std::ostream *ss);
  int layers_init(std::ostream *ss);
  int layers_sanity_checks(const std::string &description_string,
			   std::ostream *ss) const;
};

#endif
