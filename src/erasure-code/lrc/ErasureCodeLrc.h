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
#define ERROR_LRC_RULESET_OP		-(MAX_ERRNO + 14)
#define ERROR_LRC_RULESET_TYPE		-(MAX_ERRNO + 15)
#define ERROR_LRC_RULESET_N		-(MAX_ERRNO + 16)
#define ERROR_LRC_ALL_OR_NOTHING	-(MAX_ERRNO + 17)
#define ERROR_LRC_GENERATED		-(MAX_ERRNO + 18)
#define ERROR_LRC_K_M_MODULO		-(MAX_ERRNO + 19)
#define ERROR_LRC_K_MODULO		-(MAX_ERRNO + 20)
#define ERROR_LRC_M_MODULO		-(MAX_ERRNO + 21)

class ErasureCodeLrc : public ErasureCode {
public:
  static const string DEFAULT_KML;

  struct Layer {
    explicit Layer(string _chunks_map) : chunks_map(_chunks_map) { }
    ErasureCodeInterfaceRef erasure_code;
    vector<int> data;
    vector<int> coding;
    vector<int> chunks;
    set<int> chunks_as_set;
    string chunks_map;
    ErasureCodeProfile profile;
  };
  vector<Layer> layers;
  string directory;
  unsigned int chunk_count;
  unsigned int data_chunk_count;
  string ruleset_root;
  struct Step {
    Step(string _op, string _type, int _n) :
      op(_op),
      type(_type),
      n(_n) {}
    string op;
    string type;
    int n;
  };
  vector<Step> ruleset_steps;

  explicit ErasureCodeLrc(const std::string &dir)
    : directory(dir),
      chunk_count(0), data_chunk_count(0), ruleset_root("default")
  {
    ruleset_steps.push_back(Step("chooseleaf", "host", 0));
  }

  virtual ~ErasureCodeLrc() {}

  set<int> get_erasures(const set<int> &need,
			const set<int> &available) const;

  virtual int minimum_to_decode(const set<int> &want_to_read,
				const set<int> &available,
				set<int> *minimum);

  virtual int create_ruleset(const string &name,
			     CrushWrapper &crush,
			     ostream *ss) const;

  virtual unsigned int get_chunk_count() const {
    return chunk_count;
  }

  virtual unsigned int get_data_chunk_count() const {
    return data_chunk_count;
  }

  virtual unsigned int get_chunk_size(unsigned int object_size) const;

  virtual int encode_chunks(const set<int> &want_to_encode,
			    map<int, bufferlist> *encoded);

  virtual int decode_chunks(const set<int> &want_to_read,
			    const map<int, bufferlist> &chunks,
			    map<int, bufferlist> *decoded);

  virtual int init(ErasureCodeProfile &profile, ostream *ss);

  virtual int parse(ErasureCodeProfile &profile, ostream *ss);

  int parse_kml(ErasureCodeProfile &profile, ostream *ss);

  int parse_ruleset(ErasureCodeProfile &profile, ostream *ss);

  int parse_ruleset_step(string description_string,
			 json_spirit::mArray description,
			 ostream *ss);

  int layers_description(const ErasureCodeProfile &profile,
			 json_spirit::mArray *description,
			 ostream *ss) const;
  int layers_parse(string description_string,
		   json_spirit::mArray description,
		   ostream *ss);
  int layers_init(ostream *ss);
  int layers_sanity_checks(string description_string,
			   ostream *ss) const;
};

#endif
