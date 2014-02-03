// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
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

#ifndef CEPH_ERASURE_CODE_PYRAMID_H
#define CEPH_ERASURE_CODE_PYRAMID_H

#include "include/err.h"
#include "json_spirit/json_spirit.h"
#include "osd/ErasureCode.h"

#define ERROR_PYRAMID_ARRAY		-(MAX_ERRNO + 1)
#define ERROR_PYRAMID_OBJECT		-(MAX_ERRNO + 2)
#define ERROR_PYRAMID_INT		-(MAX_ERRNO + 3)
#define ERROR_PYRAMID_STR		-(MAX_ERRNO + 4)
#define ERROR_PYRAMID_PLUGIN		-(MAX_ERRNO + 5)
#define ERROR_PYRAMID_DESCRIPTION	-(MAX_ERRNO + 6)
#define ERROR_PYRAMID_PARSE_JSON	-(MAX_ERRNO + 7)
#define ERROR_PYRAMID_MAPPING		-(MAX_ERRNO + 8)
#define ERROR_PYRAMID_MAPPING_SIZE	-(MAX_ERRNO + 9)
#define ERROR_PYRAMID_FIRST_MAPPING	-(MAX_ERRNO + 10)
#define ERROR_PYRAMID_COUNT_CONSTRAINT	-(MAX_ERRNO + 11)

class ErasureCodePyramid : public ErasureCode {
public:
  struct Layer {
    ErasureCodeInterfaceRef erasure_code;
    string mapping;
    vector<Layer> layers;
  };
  Layer layer;
  struct Description {
    map<string,string> parameters;
    unsigned int size;
  };
  vector<Description> descriptions;
  string directory;
  unsigned int chunk_count;
  unsigned int data_chunk_count;

  virtual ~ErasureCodePyramid() {}

  virtual int minimum_to_decode(const set<int> &want_to_read,
				const set<int> &available,
				set<int> *minimum);

  int layer_minimum_to_decode(const Layer &layer,
			      const set<int> &want,
			      const set<int> &available,
			      set<int> *minimum) const;

  virtual unsigned int get_chunk_count() const {
    return chunk_count;
  }

  virtual unsigned int get_data_chunk_count() const {
    return data_chunk_count;
  }

  virtual unsigned int get_chunk_size(unsigned int object_size) const;

  int layer_encode(const Layer &layer, vector<bufferlist> &chunks);

  virtual int encode_chunks(vector<bufferlist> &chunks);

  virtual int encode(const set<int> &want_to_encode,
		     const bufferlist &in,
		     map<int, bufferlist> *encoded);

  int layer_decode(const Layer &layer,
		   vector<bool> *erasures,
		   vector<bufferlist> &chunks);

  virtual int decode_chunks(vector<bool> erasures,
			    vector<bufferlist> &chunks);

  virtual int decode(const set<int> &want_to_read,
		     const map<int, bufferlist> &chunks,
		     map<int, bufferlist> *decoded);

  int init(const map<string,string> &parameters, ostream *ss);

  int layers_description(const map<string,string> &parameters,
			 json_spirit::mArray *description,
			 ostream *ss) const;
  int layers_parse(string description_string,
		   json_spirit::mArray description,
		   ostream *ss);
  int layers_init(unsigned int description_index,
		  const string &mapping,
		  unsigned int divisor,
		  Layer *layer);
  int layers_sanity_checks(string description_string,
			   ostream *ss) const;
  int layer_sanity_checks(string description_string,
			  const Layer &layer,
			  int level,
			  ostream *ss) const;
};

#endif
