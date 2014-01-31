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

#include <errno.h>
#include <algorithm>

#include "common/debug.h"
#include "osd/ErasureCodePlugin.h"
#include "json_spirit/json_spirit_writer.h"

#include "ErasureCodePyramid.h"

// re-include our assert to clobber boost's
#include "include/assert.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePyramid: ";
}

int ErasureCodePyramid::layers_description(const map<string,string> &parameters,
					   json_spirit::mArray *description,
					   ostream *ss) const
{
  if (parameters.count("erasure-code-pyramid") == 0) {
    *ss << "could not find 'erasure-code-pyramid' in " << parameters;
    return ERROR_PYRAMID_DESCRIPTION;
  }
  string str = parameters.find("erasure-code-pyramid")->second;
  try {
    json_spirit::mValue json;
    json_spirit::read_or_throw(str, json);

    if (json.type() != json_spirit::array_type) {
      *ss << "erasure-code-pyramid='" << str
	 << "' must be a JSON array but is of type "
	 << json.type() << " instead";
      return ERROR_PYRAMID_ARRAY;
    }
    *description = json.get_array();
  } catch (json_spirit::Error_position &e) {
    *ss << "failed to parse erasure-code-pyramid='" << str << "'"
       << " at line " << e.line_ << ", column " << e.column_
       << " : " << e.reason_;
    return ERROR_PYRAMID_PARSE_JSON;
  }
  return 0;
}

int ErasureCodePyramid::layers_parse(string description_string,
				     json_spirit::mArray description,
				     ostream *ss)
{
  int position = 0;
  for (vector<json_spirit::mValue>::iterator i = description.begin();
       i != description.end();
       i++, position++) {
    if (i->type() != json_spirit::obj_type) {
      stringstream json_string;
      json_spirit::write(*i, json_string);
      *ss << "each element of the array "
	  << description_string << " must be a JSON object but "
	  << json_string.str() << " at position " << position
	  << " is of type " << i->type() << " instead";
      return ERROR_PYRAMID_OBJECT;
    }
    json_spirit::mObject layer_json = i->get_obj();
    map<string, string> parameters;
    int size = 1;
    for (map<string, json_spirit::mValue>::iterator j = layer_json.begin();
	 j != layer_json.end();
	 ++j) {
      if (j->first == "size") {
	if (j->second.type() != json_spirit::int_type) {
	  stringstream json_string;
	  json_spirit::write(*i, json_string);
	  *ss << "element " << j->first << " from object "
	      << json_string.str() << " at position " << position
	      << " is of type " << j->second.type() << " instead of int";
	  return ERROR_PYRAMID_INT;
	}
	size = j->second.get_int();
      } else if (j->second.type() != json_spirit::str_type) {
	stringstream json_string;
	json_spirit::write(*i, json_string);
	*ss << "element " << j->first << " from object "
	    << json_string.str() << " at position " << position
	    << " is of type " << j->second.type() << " instead of string";
	return ERROR_PYRAMID_STR;
      } else {
	parameters[j->first] = j->second.get_str();
      }
    }
    Description description;
    description.size = size;
    description.parameters = parameters;
    descriptions.push_back(description);
  }

  if (descriptions[0].parameters.count("mapping") == 0) {
    *ss << "mapping is missing from the top first layer " 
	<< descriptions[0].parameters << " of the pyramid "
	<< description_string;
    return ERROR_PYRAMID_FIRST_MAPPING;
  }
  const string &first_mapping = descriptions[0].parameters.find("mapping")->second;

  for (unsigned int i = 0; i < descriptions.size(); i++) {
    const Description description = descriptions[i];
    if (description.parameters.count("mapping") == 0) {
      *ss << "mapping is missing from the layer " << i << " of the pyramid "
	  << description_string;
      return ERROR_PYRAMID_MAPPING;
    }
    const string &mapping = description.parameters.find("mapping")->second;
    if (mapping.size() != first_mapping.size()) {
      *ss << "layer " << i << " has mapping='" << mapping << "'"
	  << " which must be of the same size as the mapping of the "
	  << " first layer ('" << first_mapping << "')";
      return ERROR_PYRAMID_MAPPING_SIZE;
    }
    if (description.parameters.count("erasure-code-plugin") == 0) {
      *ss << "layer " << i << " is missing \"erasure-code-plugin\" in "
	  << description_string;
      return ERROR_PYRAMID_PLUGIN;
    }
  }

  return 0;
}

int ErasureCodePyramid::layers_init(unsigned int description_index,
				    const string &mapping,
				    unsigned int divisor,
				    Layer *layer)
{
  Description &description = descriptions[description_index];
  layer->mapping = mapping;

  unsigned int next_index = description_index + 1;
  if (next_index < descriptions.size()) {
    Description &next = descriptions[next_index];
    const string &next_mapping = next.parameters["mapping"];
    divisor *= next.size;
    int width = next_mapping.size() / divisor;
    for (unsigned int i = 0; i < next.size; i++) {
      Layer next_layer;
      string sub_mapping = next_mapping.substr(i * width, width);
      int r = layers_init(next_index, sub_mapping, divisor, &next_layer);
      if (r)
	return r;
      layer->layers.push_back(next_layer);
    }
  }

  ErasureCodePluginRegistry &registry = ErasureCodePluginRegistry::instance();
  description.parameters["erasure-code-directory"] = directory;
  return registry.factory(description.parameters["erasure-code-plugin"],
			  description.parameters,
			  &layer->erasure_code);
}

int ErasureCodePyramid::layers_sanity_checks(string description_string,
					     ostream *ss) const
{
  return layer_sanity_checks(description_string, layer, 0, ss);
}

int ErasureCodePyramid::layer_sanity_checks(string description_string,
					    const Layer &layer,
					    int level,
					    ostream *ss) const
{
  if (layer.layers.size() <= 0)
    return 0;

  int expected = layer.layers[0].erasure_code->get_chunk_count();
  bool mismatch = false;
  for (unsigned int i = 0; i < layer.layers.size(); i++) {
    int count = 0;
    for (unsigned int j = 0; j < layer.layers[i].mapping.size(); j++)
      if (layer.layers[i].mapping[j] != '-')
	count++;
    if (count != expected)
      mismatch = true;
  }
  if (mismatch) {
    *ss << "at layer " << level << ", mappings are expected to have exactly "
	<< expected << " chunks, but the count is different: "
	<< "The dash (-) char means no chunk, any other char is a chunk.\n";
    for (unsigned int i = 0; i < layer.layers.size(); i++) {
      int count = 0;
      for (unsigned int j = 0; j < layer.layers[i].mapping.size(); j++)
	if (layer.layers[i].mapping[j] != '-')
	  count++;
      *ss << layer.layers[i].mapping << " has " << count << " chunks\n";
    }
    return ERROR_PYRAMID_COUNT_CONSTRAINT;
  }
  for (unsigned int i = 0; i < layer.layers.size(); i++) {
    int r = layer_sanity_checks(description_string, layer.layers[i],
				level + 1, ss);
    if (r)
      return r;
  }

  return 0;
}

int ErasureCodePyramid::init(const map<string,string> &parameters,
			     ostream *ss)
{
  if (parameters.count("erasure-code-directory") != 0)
    directory = parameters.find("erasure-code-directory")->second;

  int r;

  json_spirit::mArray description;
  r = layers_description(parameters, &description, ss);
  if (r)
    return r;

  string description_string = parameters.find("erasure-code-pyramid")->second;

  dout(10) << "init(" << description_string << ")" << dendl;

  list<Layer> layers;
  r = layers_parse(description_string, description, ss);
  if (r)
    return r;

  const string &mapping = descriptions[0].parameters["mapping"];
  r = layers_init(0, mapping, 1, &layer);
  if (r)
    return r;
  data_chunk_count = layer.erasure_code->get_data_chunk_count();
  chunk_count = mapping.size();

  return layers_sanity_checks(description_string, ss);
}

int ErasureCodePyramid::minimum_to_decode(const set<int> &want_to_read,
					  const set<int> &available_chunks,
					  set<int> *minimum) 
{
  return layer_minimum_to_decode(layer, want_to_read, available_chunks, minimum);
}

int ErasureCodePyramid::layer_minimum_to_decode(const Layer &layer,
						const set<int> &want,
						const set<int> &available,
						set<int> *minimum) const
{
  bool has_erasures = !includes(available.begin(), available.end(),
				want.begin(), want.end());

  if (!has_erasures) {
    *minimum = want;
    return 0;
  }

  if (layer.layers.size() > 0) {
    has_erasures = false;

    for (unsigned int i = 0; i < layer.layers.size(); i++) {
      unsigned int mapping_length = layer.layers[i].mapping.size();
      set<int> lower_want;
      set<int> lower_available;
      for (unsigned int j = 0; j < mapping_length; j++) {
	if (want.find(i * mapping_length + j) != want.end())
	  lower_want.insert(j);
	if (available.find(i * mapping_length + j) != available.end())
	  lower_available.insert(j);
      }
      set<int> lower_minimum;
      int r = layer_minimum_to_decode(layer.layers[i],
				      lower_want,
				      lower_available,
				      &lower_minimum);
      if (r && r != -EIO)
	return r;
      if (r == -EIO) {
	has_erasures = true;
      } else {
	for (unsigned int j = 0; j < mapping_length; j++) {
	  if (lower_minimum.find(j) != lower_minimum.end())
	    minimum->insert(i * mapping_length + j);
	}
      }
    }
  }

  if (!has_erasures)
    return 0;

  unsigned int mapping_length = layer.mapping.size();
  const char *mapping = layer.mapping.c_str();
  set<int> layer_want;
  set<int> layer_available;
  for (unsigned int i = 0; i < mapping_length; i++) {
    if (mapping[i] != '-') {
      if (want.find(i) != want.end())
	layer_want.insert(i);
      if (available.find(i) != available.end())
	layer_available.insert(i);
    }
  }

  set<int> layer_minimum;
  int r = layer.erasure_code->minimum_to_decode(layer_want,
						layer_available,
						&layer_minimum);
  if (r)
    return r;

  minimum->insert(layer_minimum.begin(), layer_minimum.end());
  return 0;
}

unsigned int ErasureCodePyramid::get_chunk_size(unsigned int object_size) const
{
  unsigned int chunk_size = layer.erasure_code->get_chunk_size(object_size);
  unsigned int padded_size = data_chunk_count * chunk_size;

  for (const Layer *lower = &layer;
       lower->layers.size() > 0;
       lower = &(lower->layers[0])) {
    assert(padded_size % lower->layers.size() == 0);
    padded_size /= lower->layers.size();
    assert(chunk_size == lower->erasure_code->get_chunk_size(padded_size));
  }
  return chunk_size;
}

int ErasureCodePyramid::layer_encode(const Layer &layer,
				     vector<bufferlist> &chunks)
{
  const char *mapping = layer.mapping.c_str();
  unsigned int mapping_length = layer.mapping.size();
  vector<bufferlist> data;
  vector<bufferlist> coding;
  for (unsigned int i = 0; i < mapping_length; i++) {
    switch (mapping[i]) {
    case '^':
      coding.push_back(chunks[i]);
      break;
    case '-':
      break;
    default:
      data.push_back(chunks[i]);
      break;
    }
  }
  vector<bufferlist> layer_chunks = data;
  layer_chunks.insert(layer_chunks.end(), coding.begin(), coding.end());
  int r = layer.erasure_code->encode_chunks(layer_chunks);
  if (r)
    return r;

  for (unsigned int i = 0; i < layer.layers.size(); i++) {
    const Layer &lower = layer.layers[i];
    unsigned int width = lower.mapping.size();
    vector<bufferlist> lower_chunks;
    for (unsigned int j = i * width; j < (i + 1) * width; j++)
      lower_chunks.push_back(chunks[j]);
    r = layer_encode(lower, lower_chunks);
    if (r)
      return r;
  }

  return 0;
}

int ErasureCodePyramid::encode_chunks(vector<bufferlist> &chunks)
{
  return layer_encode(layer, chunks);
}

int ErasureCodePyramid::encode(const set<int> &want_to_encode,
			       const bufferlist &in,
			       map<int, bufferlist> *encoded)
{
  unsigned int blocksize = get_chunk_size(in.length());
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  unsigned int padded_length = blocksize * k;
  bufferlist out(in);
  if (padded_length - in.length() > 0) {
    dout(10) << "encode adjusted buffer length from " << in.length()
	     << " to " << padded_length << dendl;
    bufferptr pad(padded_length - in.length());
    pad.zero();
    out.push_back(pad);
    out.rebuild_page_aligned();
  }
  unsigned coding_length = blocksize * m;
  bufferptr coding(buffer::create_page_aligned(coding_length));
  out.push_back(coding);
  vector<bufferlist> chunks;
  unsigned int k_index = 0;
  unsigned int m_index = 0;
  const string &mapping = layer.mapping;
  assert(k + m == mapping.size());
  for (unsigned int i = 0; i < k + m; i++) {
    bufferlist &chunk = (*encoded)[i];
    switch (mapping[i]) {
    case '^':
    case '-':
      chunk.substr_of(out, (k + m_index++) * blocksize, blocksize);
      break;
    default:
      chunk.substr_of(out, k_index++ * blocksize, blocksize);
      break;
    }
    chunks.push_back(chunk);
  }
  assert(k_index + m_index == k + m);
  encode_chunks(chunks);
  for (unsigned int i = 0; i < k + m; i++)
    if (want_to_encode.count(i) == 0)
      encoded->erase(i);
  return 0;
}

int ErasureCodePyramid::layer_decode(const Layer &layer,
				     vector<bool> *erasures,
				     vector<bufferlist> &chunks)
{
  bool has_erasures = false;
  unsigned int mapping_length = layer.mapping.size();
  for (unsigned int i = 0; i < mapping_length; i++) {
    if ((*erasures)[i]) {
      has_erasures = true;
      break;
    }
  }

  if (!has_erasures)
    return 0;

  for (unsigned int i = 0; i < layer.layers.size(); i++) {
    vector<bufferlist> lower_chunks;
    vector<bool> lower_erasures;
    unsigned int mapping_length = layer.layers[i].mapping.size();
    for (unsigned int j = 0; j < mapping_length; j++) {
      lower_chunks.push_back(chunks[i * mapping_length + j]);
      lower_erasures.push_back((*erasures)[i * mapping_length + j]);
    }
    int r = layer_decode(layer.layers[i], &lower_erasures, lower_chunks);
    if (r && r != -EIO)
      return r;
    if (r == -EIO)
      has_erasures = true;
    else
      for (unsigned int j = 0; j < mapping_length; j++)
	(*erasures)[i * mapping_length + j] = lower_erasures[j];
  }

  if (!has_erasures)
    return 0;

  const char *mapping = layer.mapping.c_str();
  vector<bool> layer_erasures;
  vector<bufferlist> data;
  vector<bufferlist> coding;
  set<int> available;
  set<int> want_to_read;
  for (unsigned int i = 0; i < mapping_length; i++) {
    want_to_read.insert(i);
    switch (mapping[i]) {
    case '^':
      coding.push_back(chunks[i]);
      layer_erasures.push_back((*erasures)[i]);
      if (!layer_erasures.back())
	available.insert(i);
      break;
    case '-':
      break;
    default:
      data.push_back(chunks[i]);
      layer_erasures.push_back((*erasures)[i]);
      if (!layer_erasures.back())
	available.insert(i);
      break;
    }
  }

  set<int> minimum;
  int r = layer.erasure_code->minimum_to_decode(want_to_read,
						available,
						&minimum);
  if (r && r != -EIO)
    return r;

  if (r != -EIO) {
    vector<bufferlist> layer_chunks = data;
    layer_chunks.insert(layer_chunks.end(), coding.begin(), coding.end());
    int r = layer.erasure_code->decode_chunks(layer_erasures, layer_chunks);
    if (r)
      return r;
    for (unsigned int i = 0; i < mapping_length; i++)
      if (mapping[i] != '-')
	(*erasures)[i] = false;
  }

  return r;
}

int ErasureCodePyramid::decode_chunks(vector<bool> erasures,
				      vector<bufferlist> &chunks)
{
  return layer_decode(layer, &erasures, chunks);
}

int ErasureCodePyramid::decode(const set<int> &want_to_read,
			       const map<int, bufferlist> &chunks,
			       map<int, bufferlist> *decoded)
{
  unsigned int k = get_data_chunk_count();
  unsigned int m = get_chunk_count() - k;
  unsigned int blocksize = chunks.begin()->second.length();
  vector<bufferlist> buffers;
  vector<bool> erasures;
  for (unsigned int i =  0; i < k + m; i++) {
    if (chunks.find(i) == chunks.end()) {
      erasures.push_back(true);
      if (decoded->find(i) == decoded->end() ||
	  decoded->find(i)->second.length() != blocksize) {
	bufferptr ptr(buffer::create_page_aligned(blocksize));
	(*decoded)[i].push_front(ptr);
      }
    } else {
      erasures.push_back(false);
      (*decoded)[i] = chunks.find(i)->second;
    }
    buffers.push_back((*decoded)[i]);
  }

  int r = decode_chunks(erasures, buffers);
  if (r)
    return r;

  for (unsigned int i = 0; i < k + m; i++)
    if (want_to_read.count(i) == 0)
      decoded->erase(i);
  return 0;
}
