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

#include <errno.h>
#include <algorithm>

#include "include/str_map.h"
#include "common/debug.h"
#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
#include "include/stringify.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "json_spirit/json_spirit_writer.h"

#include "ErasureCodeLrc.h"

// re-include our assert to clobber boost's
#include "include/assert.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeLrc: ";
}

int ErasureCodeLrc::create_ruleset(const string &name,
				   CrushWrapper &crush,
				   ostream *ss) const
{
  if (crush.rule_exists(name)) {
    *ss << "rule " << name << " exists";
    return -EEXIST;
  }
  if (!crush.name_exists(ruleset_root)) {
    *ss << "root item " << ruleset_root << " does not exist";
    return -ENOENT;
  }
  int root = crush.get_item_id(ruleset_root);

  int ruleset = 0;
  int rno = 0;
  for (rno = 0; rno < crush.get_max_rules(); rno++) {
    if (!crush.rule_exists(rno) && !crush.ruleset_exists(rno))
       break;
  }
  ruleset = rno;

  int steps = 4 + ruleset_steps.size();
  int min_rep = 3;
  int max_rep = get_chunk_count();
  int ret;
  ret = crush.add_rule(steps, ruleset, pg_pool_t::TYPE_ERASURE,
		  min_rep, max_rep, rno);
  assert(ret == rno);
  int step = 0;

  ret = crush.set_rule_step(rno, step++, CRUSH_RULE_SET_CHOOSELEAF_TRIES, 5, 0);
  assert(ret == 0);
  ret = crush.set_rule_step(rno, step++, CRUSH_RULE_SET_CHOOSE_TRIES, 100, 0);
  assert(ret == 0);
  ret = crush.set_rule_step(rno, step++, CRUSH_RULE_TAKE, root, 0);
  assert(ret == 0);
  // [ [ "choose", "rack", 2 ],
  //   [ "chooseleaf", "host", 5 ] ]
  for (vector<Step>::const_iterator i = ruleset_steps.begin();
       i != ruleset_steps.end();
       ++i) {
    int op = i->op == "chooseleaf" ?
      CRUSH_RULE_CHOOSELEAF_INDEP : CRUSH_RULE_CHOOSE_INDEP;
    int type = crush.get_type_id(i->type);
    if (type < 0) {
      *ss << "unknown crush type " << i->type;
      return -EINVAL;
    }
    ret = crush.set_rule_step(rno, step++, op, i->n, type);
    assert(ret == 0);
  }
  ret = crush.set_rule_step(rno, step++, CRUSH_RULE_EMIT, 0, 0);
  assert(ret == 0);
  crush.set_rule_name(rno, name);
  return ruleset;
}

int ErasureCodeLrc::layers_description(const ErasureCodeProfile &profile,
				       json_spirit::mArray *description,
				       ostream *ss) const
{
  if (profile.count("layers") == 0) {
    *ss << "could not find 'layers' in " << profile << std::endl;
    return ERROR_LRC_DESCRIPTION;
  }
  string str = profile.find("layers")->second;
  try {
    json_spirit::mValue json;
    json_spirit::read_or_throw(str, json);

    if (json.type() != json_spirit::array_type) {
      *ss << "layers='" << str
	  << "' must be a JSON array but is of type "
	  << json.type() << " instead" << std::endl;
      return ERROR_LRC_ARRAY;
    }
    *description = json.get_array();
  } catch (json_spirit::Error_position &e) {
    *ss << "failed to parse layers='" << str << "'"
	<< " at line " << e.line_ << ", column " << e.column_
	<< " : " << e.reason_ << std::endl;
    return ERROR_LRC_PARSE_JSON;
  }
  return 0;
}

int ErasureCodeLrc::layers_parse(string description_string,
				 json_spirit::mArray description,
				 ostream *ss)
{
  int position = 0;
  for (vector<json_spirit::mValue>::iterator i = description.begin();
       i != description.end();
       ++i, position++) {
    if (i->type() != json_spirit::array_type) {
      stringstream json_string;
      json_spirit::write(*i, json_string);
      *ss << "each element of the array "
	  << description_string << " must be a JSON array but "
	  << json_string.str() << " at position " << position
	  << " is of type " << i->type() << " instead" << std::endl;
      return ERROR_LRC_ARRAY;
    }
    json_spirit::mArray layer_json = i->get_array();
    ErasureCodeProfile profile;
    int index = 0;
    for (vector<json_spirit::mValue>::iterator j = layer_json.begin();
	 j != layer_json.end();
	 ++j, ++index) {
      if (index == 0) {
	if (j->type() != json_spirit::str_type) {
	  stringstream element;
	  json_spirit::write(*j, element);
	  *ss << "the first element of the entry "
	      << element.str() << " (first is zero) "
	      << position << " in " << description_string
	      << " is of type " << (*j).type() << " instead of string" << std::endl;
	  return ERROR_LRC_STR;
	}
	layers.push_back(Layer(j->get_str()));
	Layer &layer = layers.back();
	layer.chunks_map = j->get_str();
      } else if(index == 1) {
	Layer &layer = layers.back();
	if (j->type() != json_spirit::str_type &&
	    j->type() != json_spirit::obj_type) {
	  stringstream element;
	  json_spirit::write(*j, element);
	  *ss << "the second element of the entry "
	      << element.str() << " (first is zero) "
	      << position << " in " << description_string
	      << " is of type " << (*j).type() << " instead of string or object"
	      << std::endl;
	  return ERROR_LRC_CONFIG_OPTIONS;
	}
	if (j->type() == json_spirit::str_type) {
	  int err = get_json_str_map(j->get_str(), *ss, &layer.profile);
	  if (err)
	    return err;
	} else if (j->type() == json_spirit::obj_type) {
	  json_spirit::mObject o = j->get_obj();

	  for (map<string, json_spirit::mValue>::iterator i = o.begin();
	       i != o.end();
	       ++i) {
	    layer.profile[i->first] = i->second.get_str();
	  }
	}
      } else {
	  // ignore trailing elements
      }
    }
  }
  return 0;
}

int ErasureCodeLrc::layers_init(ostream *ss)
{
  ErasureCodePluginRegistry &registry = ErasureCodePluginRegistry::instance();
  for (unsigned int i = 0; i < layers.size(); i++) {
    Layer &layer = layers[i];
    int position = 0;
    for(std::string::iterator it = layer.chunks_map.begin();
	it != layer.chunks_map.end();
	++it) {
      if (*it == 'D')
	layer.data.push_back(position);
      if (*it == 'c')
	layer.coding.push_back(position);
      if (*it == 'c' || *it == 'D')
	layer.chunks_as_set.insert(position);
      position++;
    }
    layer.chunks = layer.data;
    layer.chunks.insert(layer.chunks.end(),
			layer.coding.begin(), layer.coding.end());
    if (layer.profile.find("k") == layer.profile.end())
      layer.profile["k"] = stringify(layer.data.size());
    if (layer.profile.find("m") == layer.profile.end())
      layer.profile["m"] = stringify(layer.coding.size());
    if (layer.profile.find("plugin") == layer.profile.end())
      layer.profile["plugin"] = "jerasure";
    if (layer.profile.find("technique") == layer.profile.end())
      layer.profile["technique"] = "reed_sol_van";
    int err = registry.factory(layer.profile["plugin"],
			       directory,
			       layer.profile,
			       &layer.erasure_code,
			       ss);
    if (err)
      return err;
  }
  return 0;
}

int ErasureCodeLrc::layers_sanity_checks(string description_string,
					 ostream *ss) const
{
  int position = 0;

  if (layers.size() < 1) {
    *ss << "layers parameter has " << layers.size()
	<< " which is less than the minimum of one. "
	<< description_string << std::endl;
    return ERROR_LRC_LAYERS_COUNT;
  }
  for (vector<Layer>::const_iterator layer = layers.begin();
       layer != layers.end();
       ++layer) {
    if (chunk_count != layer->chunks_map.length()) {
      *ss << "the first element of the array at position "
	  << position << " (starting from zero) "
	  << " is the string '" << layer->chunks_map
	  << " found in the layers parameter "
	  << description_string << ". It is expected to be "
	  << chunk_count << " characters long but is "
	  << layer->chunks_map.length() << " characters long instead "
	  << std::endl;
      return ERROR_LRC_MAPPING_SIZE;
    }
  }
  return 0;
}

int ErasureCodeLrc::parse(ErasureCodeProfile &profile,
			  ostream *ss)
{
  int r = ErasureCode::parse(profile, ss);
  if (r)
    return r;

  return parse_ruleset(profile, ss);
}

const string ErasureCodeLrc::DEFAULT_KML("-1");

int ErasureCodeLrc::parse_kml(ErasureCodeProfile &profile,
			      ostream *ss)
{
  int err = ErasureCode::parse(profile, ss);
  const int DEFAULT_INT = -1;
  int k, m, l;
  err |= to_int("k", profile, &k, DEFAULT_KML, ss);
  err |= to_int("m", profile, &m, DEFAULT_KML, ss);
  err |= to_int("l", profile, &l, DEFAULT_KML, ss);

  if (k == DEFAULT_INT && m == DEFAULT_INT && l == DEFAULT_INT)
    return err;

  if ((k != DEFAULT_INT || m != DEFAULT_INT || l != DEFAULT_INT) &&
      (k == DEFAULT_INT || m == DEFAULT_INT || l == DEFAULT_INT)) {
    *ss << "All of k, m, l must be set or none of them in "
	<< profile << std::endl;
    return ERROR_LRC_ALL_OR_NOTHING;
  }

  const char *generated[] = { "mapping",
			      "layers",
			      "ruleset-steps" };

  for (int i = 0; i < 3; i++) {
    if (profile.count(generated[i])) {
      *ss << "The " << generated[i] << " parameter cannot be set "
	  << "when k, m, l are set in " << profile << std::endl;
      return ERROR_LRC_GENERATED;
    }
  }

  if ((k + m) % l) {
    *ss << "k + m must be a multiple of l in "
	<< profile << std::endl;
    return ERROR_LRC_K_M_MODULO;
  }

  int local_group_count = (k + m) / l;

  if (k % local_group_count) {
    *ss << "k must be a multiple of (k + m) / l in "
	<< profile << std::endl;
    return ERROR_LRC_K_MODULO;
  }

  if (m % local_group_count) {
    *ss << "m must be a multiple of (k + m) / l in "
	<< profile << std::endl;
    return ERROR_LRC_M_MODULO;
  }

  string mapping;
  for (int i = 0; i < local_group_count; i++) {
    mapping += string(k / local_group_count, 'D') +
      string(m / local_group_count, '_') + "_";
  }
  profile["mapping"] = mapping;

  string layers = "[ ";

  // global layer
  layers += " [ \"";
  for (int i = 0; i < local_group_count; i++) {
    layers += string(k / local_group_count, 'D') +
      string(m / local_group_count, 'c') + "_";
  }
  layers += "\", \"\" ],";

  // local layers
  for (int i = 0; i < local_group_count; i++) {
    layers += " [ \"";
    for (int j = 0; j < local_group_count; j++) {
      if (i == j)
	layers += string(l, 'D') + "c";
      else
	layers += string(l + 1, '_');
    }
    layers += "\", \"\" ],";
  }
  profile["layers"] = layers + "]";

  ErasureCodeProfile::const_iterator parameter;
  string ruleset_locality;
  parameter = profile.find("ruleset-locality");
  if (parameter != profile.end())
    ruleset_locality = parameter->second;
  string ruleset_failure_domain = "host";
  parameter = profile.find("ruleset-failure-domain");
  if (parameter != profile.end())
    ruleset_failure_domain = parameter->second;

  if (ruleset_locality != "") {
    ruleset_steps.clear();
    ruleset_steps.push_back(Step("choose", ruleset_locality,
				 local_group_count));
    ruleset_steps.push_back(Step("chooseleaf", ruleset_failure_domain,
				 l + 1));
  } else if (ruleset_failure_domain != "") {
    ruleset_steps.clear();
    ruleset_steps.push_back(Step("chooseleaf", ruleset_failure_domain, 0));
  }

  return err;
}

int ErasureCodeLrc::parse_ruleset(ErasureCodeProfile &profile,
				  ostream *ss)
{
  int err = 0;
  err |= to_string("ruleset-root", profile,
		   &ruleset_root,
		   "default", ss);

  if (profile.count("ruleset-steps") != 0) {
    ruleset_steps.clear();
    string str = profile.find("ruleset-steps")->second;
    json_spirit::mArray description;
    try {
      json_spirit::mValue json;
      json_spirit::read_or_throw(str, json);

      if (json.type() != json_spirit::array_type) {
	*ss << "ruleset-steps='" << str
	    << "' must be a JSON array but is of type "
	    << json.type() << " instead" << std::endl;
	return ERROR_LRC_ARRAY;
      }
      description = json.get_array();
    } catch (json_spirit::Error_position &e) {
      *ss << "failed to parse ruleset-steps='" << str << "'"
	  << " at line " << e.line_ << ", column " << e.column_
	  << " : " << e.reason_ << std::endl;
      return ERROR_LRC_PARSE_JSON;
    }

    int position = 0;
    for (vector<json_spirit::mValue>::iterator i = description.begin();
	 i != description.end();
	 ++i, position++) {
      if (i->type() != json_spirit::array_type) {
	stringstream json_string;
	json_spirit::write(*i, json_string);
	*ss << "element of the array "
	    << str << " must be a JSON array but "
	    << json_string.str() << " at position " << position
	    << " is of type " << i->type() << " instead" << std::endl;
	return ERROR_LRC_ARRAY;
      }
      int r = parse_ruleset_step(str, i->get_array(), ss);
      if (r)
	return r;
    }
  }
  return 0;
}

int ErasureCodeLrc::parse_ruleset_step(string description_string,
				       json_spirit::mArray description,
				       ostream *ss)
{
  stringstream json_string;
  json_spirit::write(description, json_string);
  string op;
  string type;
  int n = 0;
  int position = 0;
  for (vector<json_spirit::mValue>::iterator i = description.begin();
       i != description.end();
       ++i, position++) {
    if ((position == 0 || position == 1) &&
	i->type() != json_spirit::str_type) {
      *ss << "element " << position << " of the array "
	  << json_string.str() << " found in " << description_string
	  << " must be a JSON string but is of type "
	  << i->type() << " instead" << std::endl;
      return position == 0 ? ERROR_LRC_RULESET_OP : ERROR_LRC_RULESET_TYPE;
    }
    if (position == 2 && i->type() != json_spirit::int_type) {
      *ss << "element " << position << " of the array "
	  << json_string.str() << " found in " << description_string
	  << " must be a JSON int but is of type "
	  << i->type() << " instead" << std::endl;
      return ERROR_LRC_RULESET_N;
    }

    if (position == 0)
      op = i->get_str();
    if (position == 1)
      type = i->get_str();
    if (position == 2)
      n = i->get_int();
  }
  ruleset_steps.push_back(Step(op, type, n));
  return 0;
}

int ErasureCodeLrc::init(ErasureCodeProfile &profile,
			 ostream *ss)
{
  int r;

  r = parse_kml(profile, ss);
  if (r)
    return r;

  r = parse(profile, ss);
  if (r)
    return r;

  json_spirit::mArray description;
  r = layers_description(profile, &description, ss);
  if (r)
    return r;

  string description_string = profile.find("layers")->second;

  dout(10) << "init(" << description_string << ")" << dendl;

  r = layers_parse(description_string, description, ss);
  if (r)
    return r;

  r = layers_init(ss);
  if (r)
    return r;

  if (profile.count("mapping") == 0) {
    *ss << "the 'mapping' profile is missing from " << profile;
    return ERROR_LRC_MAPPING;
  }
  string mapping = profile.find("mapping")->second;
  data_chunk_count = 0;
  for(std::string::iterator it = mapping.begin(); it != mapping.end(); ++it) {
    if (*it == 'D')
      data_chunk_count++;
  }
  chunk_count = mapping.length();

  r = layers_sanity_checks(description_string, ss);
  if (r)
    return r;

  //
  // When initialized with kml, the profile parameters
  // that were generated should not be stored because
  // they would otherwise be exposed to the caller.
  //
  if (profile.find("l") != profile.end() &&
      profile.find("l")->second != DEFAULT_KML) {
    profile.erase("mapping");
    profile.erase("layers");
  }
  ErasureCode::init(profile, ss);
  return 0;
}

set<int> ErasureCodeLrc::get_erasures(const set<int> &want,
				      const set<int> &available) const
{
  set<int> result;
  set_difference(want.begin(), want.end(),
		 available.begin(), available.end(),
		 inserter(result, result.end()));
  return result;
}

unsigned int ErasureCodeLrc::get_chunk_size(unsigned int object_size) const
{
  return layers.front().erasure_code->get_chunk_size(object_size);
}

void p(const set<int> &s) { cerr << s; } // for gdb

int ErasureCodeLrc::minimum_to_decode(const set<int> &want_to_read,
				      const set<int> &available_chunks,
				      set<int> *minimum)
{
  dout(20) << __func__ << " want_to_read " << want_to_read
	   << " available_chunks " << available_chunks << dendl;
  {
    set<int> erasures_total;
    set<int> erasures_not_recovered;
    set<int> erasures_want;
    for (unsigned int i = 0; i < get_chunk_count(); ++i) {
      if (available_chunks.count(i) == 0) {
	erasures_total.insert(i);
	erasures_not_recovered.insert(i);
	if (want_to_read.count(i) != 0)
	  erasures_want.insert(i);
      }
    }

    //
    // Case 1:
    //
    // When no chunk is missing there is no need to read more than what
    // is wanted.
    //
    if (erasures_want.empty()) {
      *minimum = want_to_read;
      dout(20) << __func__ << " minimum == want_to_read == "
	       << want_to_read << dendl;
      return 0;
    }

    //
    // Case 2:
    //
    // Try to recover erasures with as few chunks as possible.
    //
    for (vector<Layer>::reverse_iterator i = layers.rbegin();
	 i != layers.rend();
	 ++i) {
      //
      // If this layer has no chunk that we want, skip it.
      //
      set<int> layer_want;
      set_intersection(want_to_read.begin(), want_to_read.end(),
		       i->chunks_as_set.begin(), i->chunks_as_set.end(),
		       inserter(layer_want, layer_want.end()));
      if (layer_want.empty())
	continue;
      //
      // Are some of the chunks we want missing ?
      //
      set<int> layer_erasures;
      set_intersection(layer_want.begin(), layer_want.end(),
		       erasures_want.begin(), erasures_want.end(),
		       inserter(layer_erasures, layer_erasures.end()));
      set<int> layer_minimum;
      if (layer_erasures.empty()) {
	//
	// The chunks we want are available, this is the minimum we need
	// to read.
	//
	layer_minimum = layer_want;
      } else {
	set<int> erasures;
	set_intersection(i->chunks_as_set.begin(), i->chunks_as_set.end(),
			 erasures_not_recovered.begin(), erasures_not_recovered.end(),
			 inserter(erasures, erasures.end()));

	if (erasures.size() > i->erasure_code->get_coding_chunk_count()) {
	  //
	  // There are too many erasures for this layer to recover: skip
	  // it and hope that an upper layer will be do better.
	  //
	  continue;
	} else {
	  //
	  // Get all available chunks in that layer to recover the
	  // missing one(s).
	  //
	  set_difference(i->chunks_as_set.begin(), i->chunks_as_set.end(),
			 erasures_not_recovered.begin(), erasures_not_recovered.end(),
			 inserter(layer_minimum, layer_minimum.end()));
	  //
	  // Chunks recovered by this layer are removed from the list of
	  // erasures so that upper levels do not attempt to recover
	  // them.
	  //
	  for (set<int>::const_iterator j = erasures.begin();
	       j != erasures.end();
	       ++j) {
	    erasures_not_recovered.erase(*j);
	    if (erasures_want.count(*j))
	      erasures_want.erase(*j);
	  }
	}
      }
      minimum->insert(layer_minimum.begin(), layer_minimum.end());
    }
    if (erasures_want.empty()) {
      minimum->insert(want_to_read.begin(), want_to_read.end());
      for (set<int>::const_iterator i = erasures_total.begin();
	   i != erasures_total.end();
	   ++i) {
	if (minimum->count(*i))
	  minimum->erase(*i);
      }
      dout(20) << __func__ << " minimum = " << *minimum << dendl;
      return 0;
    }
  }

  {
    //
    // Case 3:
    //
    // The previous strategy failed to recover from all erasures.
    //
    // Try to recover as many chunks as possible, even from layers
    // that do not contain chunks that we want, in the hope that it
    // will help the upper layers.
    //
    set<int> erasures_total;
    for (unsigned int i = 0; i < get_chunk_count(); ++i) {
      if (available_chunks.count(i) == 0)
	erasures_total.insert(i);
    }

    for (vector<Layer>::reverse_iterator i = layers.rbegin();
	 i != layers.rend();
	 ++i) {
      set<int> layer_erasures;
      set_intersection(i->chunks_as_set.begin(), i->chunks_as_set.end(),
		       erasures_total.begin(), erasures_total.end(),
		       inserter(layer_erasures, layer_erasures.end()));
      //
      // If this layer has no erasure, skip it
      //
      if (layer_erasures.empty())
	continue;

      if (layer_erasures.size() > 0 &&
	  layer_erasures.size() <= i->erasure_code->get_coding_chunk_count()) {
	//
	// chunks recovered by this layer are removed from the list of
	// erasures so that upper levels know they can rely on their
	// availability
	//
	for (set<int>::const_iterator j = layer_erasures.begin();
	     j != layer_erasures.end();
	     ++j) {
	  erasures_total.erase(*j);
	}
      }
    }
    if (erasures_total.empty()) {
      //
      // Do not try to be smart about what chunks are necessary to
      // recover, use all available chunks.
      //
      *minimum = available_chunks;
      dout(20) << __func__ << " minimum == available_chunks == "
	       << available_chunks << dendl;
      return 0;
    }
  }

  derr << __func__ << " not enough chunks in " << available_chunks
       << " to read " << want_to_read << dendl;
  return -EIO;
}

int ErasureCodeLrc::encode_chunks(const set<int> &want_to_encode,
				  map<int, bufferlist> *encoded)
{
  unsigned int top = layers.size();
  for (vector<Layer>::reverse_iterator i = layers.rbegin();
       i != layers.rend();
       ++i) {
    --top;
    if (includes(i->chunks_as_set.begin(), i->chunks_as_set.end(),
		 want_to_encode.begin(), want_to_encode.end()))
      break;
  }

  for (unsigned int i = top; i < layers.size(); ++i) {
    const Layer &layer = layers[i];
    set<int> layer_want_to_encode;
    map<int, bufferlist> layer_encoded;
    int j = 0;
    for (vector<int>::const_iterator c = layer.chunks.begin();
	 c != layer.chunks.end();
	 ++c) {
      layer_encoded[j] = (*encoded)[*c];
      if (want_to_encode.find(*c) != want_to_encode.end())
	layer_want_to_encode.insert(j);
      j++;
    }
    int err = layer.erasure_code->encode_chunks(layer_want_to_encode,
						&layer_encoded);
    if (err) {
      derr << __func__ << " layer " << layer.chunks_map
	   << " failed with " << err << " trying to encode "
	   << layer_want_to_encode << dendl;
      return err;
    }
  }
  return 0;
}

int ErasureCodeLrc::decode_chunks(const set<int> &want_to_read,
				  const map<int, bufferlist> &chunks,
				  map<int, bufferlist> *decoded)
{
  set<int> available_chunks;
  set<int> erasures;
  for (unsigned int i = 0; i < get_chunk_count(); ++i) {
    if (chunks.count(i) != 0)
      available_chunks.insert(i);
    else
      erasures.insert(i);
  }

  set<int> want_to_read_erasures;

  for (vector<Layer>::reverse_iterator layer = layers.rbegin();
       layer != layers.rend();
       ++layer) {
    set<int> layer_erasures;
    set_intersection(layer->chunks_as_set.begin(), layer->chunks_as_set.end(),
		     erasures.begin(), erasures.end(),
		     inserter(layer_erasures, layer_erasures.end()));

    if (layer_erasures.size() >
	layer->erasure_code->get_coding_chunk_count()) {
      // skip because there are too many erasures for this layer to recover
    } else if(layer_erasures.size() == 0) {
      // skip because all chunks are already available
    } else {
      set<int> layer_want_to_read;
      map<int, bufferlist> layer_chunks;
      map<int, bufferlist> layer_decoded;
      int j = 0;
      for (vector<int>::const_iterator c = layer->chunks.begin();
	   c != layer->chunks.end();
	   ++c) {
	//
	// Pick chunks from *decoded* instead of *chunks* to re-use
	// chunks recovered by previous layers. In other words
	// *chunks* does not change but *decoded* gradually improves
	// as more layers recover from erasures.
	//
	if (erasures.count(*c) == 0)
	  layer_chunks[j] = (*decoded)[*c];
	if (want_to_read.count(*c) != 0)
	  layer_want_to_read.insert(j);
	layer_decoded[j] = (*decoded)[*c];
	++j;
      }
      int err = layer->erasure_code->decode_chunks(layer_want_to_read,
						   layer_chunks,
						   &layer_decoded);
      if (err) {
	derr << __func__ << " layer " << layer->chunks_map
	     << " failed with " << err << " trying to decode "
	     << layer_want_to_read << " with " << available_chunks << dendl;
	return err;
      }
      j = 0;
      for (vector<int>::const_iterator c = layer->chunks.begin();
	   c != layer->chunks.end();
	   ++c) {
	(*decoded)[*c] = layer_decoded[j];
	++j;
	if (erasures.count(*c) != 0)
	  erasures.erase(*c);
      }
      want_to_read_erasures.clear();
      set_intersection(erasures.begin(), erasures.end(),
		       want_to_read.begin(), want_to_read.end(),
		       inserter(want_to_read_erasures, want_to_read_erasures.end()));
      if (want_to_read_erasures.size() == 0)
	break;
    }
  }

  if (want_to_read_erasures.size() > 0) {
    derr << __func__ << " want to read " << want_to_read
	 << " with available_chunks = " << available_chunks
	 << " end up being unable to read " << want_to_read_erasures << dendl;
    return -EIO;
  } else {
    return 0;
  }
}
